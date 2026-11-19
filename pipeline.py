"""
pipeline.py — СТАДИИ обработки одной записи (машина состояний поверх строки БД).

Единый источник правды — строка recordings (флаги стадий, тайминги, планирование).
В папке записи лежат только выходные файлы. process() идемпотентна: делает только
не сделанное; после — проставляет планирование (done / next_due_at):

  • АРХИВАЦИЯ (рано): transcribe -> improve -> summarize -> save_to_db.
    Текст доступен сразу; запись видна в модалке.
  • УТИЛИЗАЦИЯ ВИДЕО (когда видео старше года): messages -> delete.
  • ОЧИСТКА ТЕКСТОВ (через 5 лет после удаления видео): cleanup_text.

protected (админ «хранить навсегда») отменяет messages/delete. Ручные действия бота
пишутся прямо в БД + wake(), демон переоценивает запись.

ОШИБКИ. Упавшая стадия повторяется по ЛЕСТНИЦЕ (cfg.retry_delays_sec): через 5 мин, 30 мин,
3 ч и 23 ч — всё ОТ ПЕРВОЙ ОШИБКИ, а не друг от друга (см. retry_delay). Если и последний
повтор не помог — запись получает статус FAILED_<стадия> («до решения админа») и стоит,
пока админ не вернёт её командой restart_passed (продолжит с той же стадии). Исключение — рассылка: если не
достучались лишь до отдельных участников, после лестницы идём дальше и удаляем видео по
графику. Упавшая стадия прерывает проход: за раз падает не больше одной, поэтому отсчёт
повторов у записи один.

Особые исходы: нет аудио (Толк так и сказал) — запись пропускается; нет дорожек по
спикерам — распознаём сведённую дорожку и ведём запись дальше как обычную (no_channels);
Толк запрещает удалять видео — DELETE_PROTECTION («сохранено бессрочно»), запись больше
не трогаем.

САМОПРОВЕРКИ ПЕРЕД НЕОБРАТИМЫМ. Уведомлять участников и удалять видео можно, только если
расшифровка реально лежит на диске целиком (_require_texts) — флаги в БД этого не гарантируют.
Файлы записи сверяются и в начале каждого прохода: стадию, чей файл пропал или неполон (нет
проверочной фразы store.MARKER), запись проходит заново.
Непосредственно перед удалением строка записи перечитывается: пока запись была в работе,
админ мог запретить удаление или удалить видео сам.
"""

import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import config
import db
import statuses
import usersync
import store
import texts
from talk_api import NoAudio
from transcribe import build_transcript
from enrich import improve_transcript, summarize_text, transcript_without_table
from notify import send_archive_notifications
from util import err as _err, parse_dt as _parse_dt

log = logging.getLogger("pipeline")

# Через сколько вернуться к записи, если стадия осталась несделанной при открытом окне,
# но в этом проходе не падала (напр. окно открылось, пока запись была в работе), а также
# если обработка записи сорвалась целиком (см. main._run_active).
_RETRY_BACKOFF_SEC = 300


@dataclass
class Ctx:
    """Общий контекст: клиенты + конфиг + соединение с БД. Создаётся один раз в main."""
    client: object       # TalkClient
    asr: object          # MagnitASR
    llm: object          # MagnitLLM (может быть не настроен -> llm.available()==False)
    cfg: object          # Cfg
    db: object = None    # sqlite3.Connection
    mm: object = None    # MMClient или None (заглушка/лог)


def _http_status(ex):
    return getattr(getattr(ex, "response", None), "status_code", None)


def _fmt_date(iso):
    dt = _parse_dt(iso)
    return dt.astimezone().strftime("%d.%m.%Y") if dt else "—"


def _fmt_datetime(iso):
    dt = _parse_dt(iso)
    return dt.astimezone().strftime("%d.%m.%Y в %H:%M") if dt else "—"


def _minutes(sec):
    sec = int(sec or 0)
    return max(1, round(sec / 60)) if sec else 0


def _fmt_delay(sec):
    """Отметка лестницы повторов для лога: «5 мин» / «3 ч»."""
    return f"{sec / 3600:g} ч" if sec >= 3600 else f"{sec / 60:g} мин"


def retry_delay(delays, since, now=None):
    """Через сколько секунд следующий повтор. delays — отметки лестницы, отсчитанные ОТ
    ПЕРВОЙ ОШИБКИ (since): 5 мин, 30 мин, 3 ч, 23 ч — то есть второй повтор наступает через
    30 минут после первой ошибки, а не через 35. Берём ближайшую отметку, которая ещё
    впереди; пропущенные (рабочее окно было закрыто, попытка шла долго) не «догоняем»
    несколькими повторами подряд. None — все отметки позади: повторы исчерпаны."""
    elapsed = ((now or _now()) - since).total_seconds()
    for d in delays:
        if d > elapsed:
            return d - elapsed
    return None


def _compose_header(title, created_date, duration_sec, participants, no_channels=False):
    """Шапка файлов записи: Название/Дата создания/Длительность/Участники (через запятую).
    no_channels — добавить пометку, что реплики не размечены по спикерам."""
    names = ", ".join(p.get("name") for p in participants if p.get("name")) or "—"
    head = (f"Название: {title or '—'}\n"
            f"Дата создания: {_fmt_date(created_date)}\n"
            f"Длительность: {_minutes(duration_sec)} мин\n"
            f"Участники конференции: {names}")
    if no_channels:
        head += ("\nПримечание: у записи нет отдельных дорожек по спикерам — "
                 "реплики не привязаны к участникам")
    return head


def _now():
    return datetime.now(timezone.utc)


# Флаг фазовой машины: разосланы ли единоразовые old-records-уведомления всем пользователям.
# Пока False — стадии messages/delete для «старых» записей подавляются (идёт тихий бэкфилл);
# демон разошлёт уведомления и выставит флаг, тогда удаление бэклога запускается.
OLD_RECORDS_FLAG = "old_records_all_notified"


def _old_records_done(ctx):
    return db.get_meta(ctx.db, OLD_RECORDS_FLAG) == "1"


def deletion_eligible(row, cfg, now=None):
    """Пора ли утилизировать видео: видео старше порога (delete_video_age_days).
    Порог одинаков для test и prod — test отличается лишь ИМИТАЦИЕЙ удаления и
    короткими задержками (см. cfg.simulate_delete / cfg.delete_delay_sec)."""
    dt = _parse_dt(row.get("created_date"))
    if not dt:
        return False
    return (now or _now()) - dt >= timedelta(days=cfg.delete_video_age_days)


def _delete_done(row, cfg):
    """Стадия удаления видео завершена (реально или имитировано в test)."""
    return row.get("deleted") or (cfg.simulate_delete and row.get("delete_simulated"))


def _terminal(row):
    """Запись, с которой демон больше ничего не делает: нет аудио, тексты уже удалены,
    либо она ждёт решения админа (до restart_passed)."""
    return bool(row.get("no_audio") or row.get("text_cleaned") or row.get("needs_admin"))


def _require_texts(row, strict):
    """САМОПРОВЕРКА: расшифровка записи реально лежит на диске и записана целиком. Флаги
    стадий в БД этого не гарантируют (том с текстами не смонтирован после переезда, файлы
    стёрли вручную, файл обрезало при заполненном диске), а удаление видео необратимо: без
    текста запись была бы потеряна целиком."""
    path = store.existing_path(row, "asr")
    why = store.problem(path, strict) if path else "нет файла"
    if why:
        raise RuntimeError("расшифровка на диске неполная (%s: %s) — без неё видео не трогаю"
                           % (store.asr_path(row.get("rec_dir") or "", row["recording_key"]), why))


# Тревоги суперадмину из пайплайна: демон подставляет функцию (вид, текст); без неё — только лог.
alert = None


def _alert(kind, text):
    if not alert:
        log.error(text)
        return
    try:
        alert(kind, text)                # пишет в лог сама
    except Exception as ex:
        log.error(text)
        log.warning("тревога не отправлена: %s", _err(ex))


class Outage:
    """МАССОВЫЕ СБОИ СЕРВИСА. Когда внешний сервис стадии лежит (нет сети, шлюз ASR/LLM отвечает
    ошибкой, протух токен Толка, Tag недоступен), падают ВСЕ записи подряд. Без защиты каждая
    прошла бы свою лестницу повторов и через сутки встала бы «до решения админа» — после аварии
    вся очередь ждала бы ручного перезапуска. Поэтому: стадия упала у RECORDS разных записей
    подряд без единого успеха — сервис считается недоступным. Сбои записей тогда не
    засчитываются (их лестница начнётся заново), а диспетчер пускает на стадию одну пробную
    запись раз в PROBE_SEC. Первая удача — сервис снова в строю. Состояние — в памяти: после
    перезапуска демон заново убедится, что сервис лежит, по первым RECORDS сбоям."""
    RECORDS = 10
    PROBE_SEC = 300

    def __init__(self):
        self._lock = threading.Lock()
        self._failed = {}            # стадия -> ключи записей, упавших подряд с последней удачи
        self._down = {}              # стадия -> {"since", "probe"} (монотонное время)

    def failed(self, stage, key, error):
        """Стадия упала у записи key. Возвращает True, если сервис стадии лежит."""
        with self._lock:
            if stage in self._down:
                return True
            keys = self._failed.setdefault(stage, set())
            keys.add(key)
            if len(keys) < self.RECORDS:
                return False
            now = time.monotonic()
            self._down[stage] = {"since": now, "probe": now}
            n = len(keys)
        _alert(f"outage-{stage}", texts.ADMIN_STAGE_DOWN.format(
            name=statuses.STAGE_NAMES.get(stage, stage), n=n, error=error[:300],
            mins=self.PROBE_SEC // 60))
        return True

    def passed(self, stage):
        """Стадия прошла — сервис отвечает."""
        with self._lock:
            self._failed.pop(stage, None)
            down = self._down.pop(stage, None)
        if down:
            mins = round((time.monotonic() - down["since"]) / 60)
            _alert(f"outage-{stage}-up", texts.ADMIN_STAGE_UP.format(
                name=statuses.STAGE_NAMES.get(stage, stage), mins=mins))

    def down_stages(self):
        with self._lock:
            return set(self._down)

    def may_start(self, stage):
        """Можно ли пустить запись на стадию: сервис в строю — да; лежит — одну пробную раз в
        PROBE_SEC."""
        with self._lock:
            d = self._down.get(stage)
            if d is None:
                return True
            now = time.monotonic()
            if now - d["probe"] < self.PROBE_SEC:
                return False
            d["probe"] = now
            return True


OUTAGE = Outage()


# Готовые файлы записи: вид -> флаг стадии, которая его пишет, и путь.
OUTPUTS = (("asr", "transcribed", store.asr_path), ("llm", "improved", store.llm_path),
           ("short", "summarized", store.short_path))
FILE_NAMES = {"asr": "_ASR", "llm": "_LLM", "short": "_short"}
# Предохранитель самолечения: больше стольких записей за час с «неполными» файлами — скорее
# сбилась сама проверка (а не файлы), и массово переделывать записи нельзя.
REDO_LIMIT = 300
_redo_times = []
_redo_lock = threading.Lock()
_redo_tripped = False            # сработал — до перезапуска демона ничего не переделываем


def strict_files(conn):
    """Обязательна ли проверочная фраза: да — после её переноса в старые файлы."""
    return db.get_meta(conn, store.MARKER_MIGRATED) == "1"


def broken_outputs(row, strict, always_strict=()):
    """Стадии, отмеченные сделанными, чей файл пропал, пуст, обрезан или (strict) без
    проверочной фразы: {вид: что не так}. always_strict — виды, для которых фраза обязательна
    в любом случае (их файлы записаны уже с фразой)."""
    rd, key = row.get("rec_dir") or "", row["recording_key"]
    out = {}
    for kind, flag, path in OUTPUTS:
        if row.get(flag):
            why = store.problem(path(rd, key), strict or kind in always_strict)
            if why:
                out[kind] = why
    return out


def describe(broken):
    return "; ".join(f"{FILE_NAMES[k]} — {w}" for k, w in broken.items())


def set_aside(row, broken):
    """Испорченные файлы записи — в сторону: обрезанный текст в <файл>.broken (бот его не
    отдаёт, но он цел для разбора), пустой — удалить."""
    rd, key = row.get("rec_dir") or "", row["recording_key"]
    for kind, _, path in OUTPUTS:
        p = path(rd, key)
        if kind in broken and os.path.exists(p):
            if os.path.getsize(p):
                os.replace(p, p + ".broken")
            else:
                os.remove(p)


def redo_fields(row, broken):
    """Поля строки БД, возвращающие запись на стадии с испорченными файлами (и на сохранение —
    чтобы шапка легла заново). Целые файлы других стадий не трогаем: правку LLM, сделанную по
    прежней полной расшифровке, повторно не гоняем. Без расшифровки запись снова «новая»."""
    reset = [flag for kind, flag, _ in OUTPUTS if kind in broken] + ["saved_to_db"]
    status = (statuses.NEW if "transcribed" in reset
              else statuses.derive(dict(row, **{f: 0 for f in reset})))
    return {**{f: 0 for f in reset}, "status": status, "done": 0, "next_due_at": None,
            "needs_admin": 0, "hold_notified": 0, "fail_stage": None, "fail_count": 0,
            "last_error": None, "error_since": None}


def _redo_allowed(out_dir):
    """Можно ли переделывать запись с неполными файлами. Нет, если папки расшифровок нет или она
    пуста (не смонтирован том — «неполными» оказались бы ВСЕ записи) или если за час таких
    набралось REDO_LIMIT: скорее сбилась сама проверка. Тогда предохранитель срабатывает до
    перезапуска демона, а суперадмину — тревога: массово переделывать архив без человека нельзя."""
    global _redo_tripped
    with _redo_lock:
        if _redo_tripped:
            return False
        now = time.monotonic()
        _redo_times[:] = [t for t in _redo_times if now - t < 3600]
        mounted = os.path.isdir(out_dir) and bool(os.listdir(out_dir))
        if mounted and len(_redo_times) < REDO_LIMIT:
            _redo_times.append(now)
            return True
        _redo_tripped = True
    _alert("redo-tripped", texts.ADMIN_REDO_TRIPPED.format(
        why=(f"папка расшифровок {out_dir} пуста или не смонтирована" if not mounted
             else f"за час неполными оказались файлы у {REDO_LIMIT} записей")))
    return False


def _video_state(ctx, key, row, commit):
    """Что с видео в Толке ПРЯМО СЕЙЧАС (в БД — снимок на момент подхвата, мог устареть):
      "ok"      — можно удалять;
      "blocked" — Толк запрещает удаление (canBeRemoved=false), а FORCE_DELETE выключен;
      "gone"    — записи в Толке уже нет (её удалили мимо архиватора).
    Сбой связи с Толком — исключение (стадия повторится по лестнице)."""
    try:
        detail = ctx.client.recording_detail(key)
    except Exception as ex:
        if _http_status(ex) == 404:
            return "gone"
        raise
    can = detail.get("canBeRemoved")
    if can is None:                                 # поля нет в ответе — верим сохранённому
        can = row.get("can_be_removed")
    elif bool(can) != bool(row.get("can_be_removed")):
        commit(can_be_removed=int(bool(can)))
    return "ok" if (can or ctx.cfg.force_delete) else "blocked"


def stages_pending(row, cfg, llm_ok, protected=False, now=None, old_records_done=True):
    """Стадии, готовые к выполнению СЕЙЧАС. Пустой список = на сейчас делать нечего.
    old_records_done=False -> стадия messages для «старых» записей подавляется (идёт
    тихий бэкфилл до единоразовой рассылки)."""
    now = now or _now()
    if _terminal(row):
        return []                                   # терминальные состояния
    if not row.get("transcribed"):
        return ["transcribe"]
    p = []
    if llm_ok and not row.get("improved"):
        p.append("improve")
    if llm_ok and row.get("improved") and not row.get("summarized"):
        p.append("summarize")
    ready = row.get("transcribed") and ((not llm_ok) or (row.get("improved") and row.get("summarized")))
    if ready and not row.get("saved_to_db"):
        p.append("save_to_db")

    if not protected and not row.get("kept_forever"):
        if (row.get("saved_to_db") and not row.get("messages_sent")
                and not row.get("deleted") and old_records_done
                and deletion_eligible(row, cfg, now)):
            p.append("messages")
        if row.get("messages_sent") and not _delete_done(row, cfg):
            due = _parse_dt(row.get("delete_scheduled_at"))
            if due and now >= due:
                p.append("delete")

    if row.get("deleted") and not row.get("text_cleaned"):
        due = _parse_dt(row.get("text_delete_at"))
        if due and now >= due:
            p.append("cleanup_text")
    return p


def _next_due(row, cfg, protected, old_records_done=True):
    """Ближайший БУДУЩИЙ момент, когда запись снова потребует внимания (или None).
    Вызывается, когда на сейчас делать нечего (stages_pending пуст)."""
    if _terminal(row):
        return None
    if row.get("deleted"):                          # ждём 5-летней очистки текстов
        return _parse_dt(row.get("text_delete_at"))
    if protected or row.get("kept_forever"):        # хранить навсегда — авто-действий нет
        return None
    if row.get("saved_to_db") and not row.get("messages_sent"):
        dt = _parse_dt(row.get("created_date"))
        if dt is None:
            return None
        due = dt + timedelta(days=cfg.delete_video_age_days)   # когда видео «состарится»
        # Пока идёт тихий бэкфилл (флаг old-records не выставлен), «паркуем» ТОЛЬКО уже
        # старые записи — их разбудит фаза old-records (mark_backlog_for_deletion). Молодые
        # планируем как обычно (проснутся сами, когда состарятся — уже при флаге=1).
        if not old_records_done and (_now() - dt) >= timedelta(days=cfg.delete_video_age_days):
            return None
        return due
    if row.get("messages_sent") and not _delete_done(row, cfg):
        return _parse_dt(row.get("delete_scheduled_at"))   # ждём срок удаления видео
    return None


def _stage_window(cfg, stage, llm_ok):
    """Окно, в котором можно выполнять стадию: transcribe -> ASR-окно; improve/summarize
    и пост-стадии -> LLM-окно (если LLM включён; иначе пост-стадии без окна)."""
    if stage == "transcribe":
        return cfg.asr_window
    if stage in ("improve", "summarize"):
        return cfg.llm_window
    return cfg.llm_window if llm_ok else None       # save/messages/delete/cleanup


def _finalize(ctx, row, cfg, llm_ok, protected, old_records_done=True, retry_in=None):
    """Проставить планирование (done/next_due_at) с учётом рабочих окон и вернуть строку.
    retry_in — через сколько секунд повторить стадию, упавшую в этом проходе (лестница).
    Если к тому моменту её окно закрыто, запись дождётся открытия: диспетчер берёт в работу
    только записи той фазы, чьё окно открыто (см. db.due_records)."""
    key = row["recording_key"]
    now_local = datetime.now().astimezone()
    pend = stages_pending(row, cfg, llm_ok, protected, _now(), old_records_done)
    if not pend:
        nd = _next_due(row, cfg, protected, old_records_done)   # будущее по расписанию
        nd_iso, done = (nd.isoformat() if nd else None), (0 if nd else 1)
    elif retry_in is not None:                       # стадия упала -> повтор по лестнице
        nd_iso, done = (_now() + timedelta(seconds=retry_in)).isoformat(), 0
    else:
        nxt = config.next_open_local(now_local, _stage_window(cfg, pend[0], llm_ok))
        if nxt is None:                              # окно открыто (или не задано), но стадию
            nd_iso = (_now() + timedelta(seconds=_RETRY_BACKOFF_SEC)).isoformat()  # не запускали
        else:
            nd_iso = nxt.astimezone(timezone.utc).isoformat()   # ждём открытия окна стадии
        done = 0
    db.update(ctx.db, key, done=done, next_due_at=nd_iso)
    row["done"], row["next_due_at"] = done, nd_iso
    return row


def _shrink_banner(cfg, asr_clean, llm_clean, asr_name):
    """СТРАХОВКА: LLM-версия короче дословного ASR более чем на LLM_SHRINK_WARN_PCT => возможна
    потеря текста при «улучшении» — баннер сверху _LLM (в _ASR всё цело). Сравниваем БЕЗ
    таблицы замен — иначе она удлиняет _LLM и маскирует потерю. Пустая строка — потери нет."""
    if not asr_clean:
        return ""
    lost = 1 - len(transcript_without_table(llm_clean)) / len(asr_clean)
    if lost * 100 <= cfg.shrink_warn_pct:
        return ""
    log.warning("усушка LLM-версии ~%d%% (порог %d%%) — баннер в _LLM",
                round(lost * 100), cfg.shrink_warn_pct)
    return (f"⚠️ ВНИМАНИЕ! Улучшенная (LLM) расшифровка короче дословной примерно на "
            f"{round(lost * 100)}% — возможна потеря части текста. Полный дословный текст — "
            f"в файле {asr_name}.\n\n")


def redo_transcript(ctx, row, out_dir):
    """ПОВТОРНАЯ РАСШИФРОВКА по просьбе участника — личная копия для него: в архив (Transcripts,
    флаги записи) НЕ пишется. Звук заново из Толка -> базовая расшифровка, и если LLM доступен —
    улучшенная и выжимка; файлы с шапкой и проверочной фразой — в out_dir (<ID>_ASR_повтор.md…).
    Возвращает (пути файлов, llm_problem): llm_problem — почему нет улучшенной/выжимки (LLM
    выключен или не ответил), None — всё сделано. NoAudio и сбои ASR — исключение как есть."""
    cfg, key = ctx.cfg, row["recording_key"]
    tr = build_transcript(ctx.client, key, ctx.asr, cfg.language)
    header = _compose_header(row.get("title"), row.get("created_date"), row.get("duration_sec"),
                             row.get("participants") or [], no_channels=tr.no_channels)
    asr_clean = store.blocks_to_md(tr.blocks)
    texts_ = {"ASR": header + "\n\n" + asr_clean}
    llm_problem = None
    if not (ctx.llm and ctx.llm.available()):
        llm_problem = "LLM выключен"
    else:
        try:
            llm_clean = improve_transcript(asr_clean, ctx.llm)
            short = summarize_text(llm_clean.strip() or asr_clean, ctx.llm)
            texts_["LLM"] = (_shrink_banner(cfg, asr_clean, llm_clean, f"{key}_ASR_повтор.md")
                             + header + "\n\n" + llm_clean)
            texts_["short"] = header + "\n\n" + short
        except Exception as ex:
            llm_problem = _err(ex)
            log.warning("повторная расшифровка %s: LLM не ответил — будет только базовая: %s",
                        key, llm_problem)
    os.makedirs(out_dir, exist_ok=True)
    paths = []
    for kind, text in texts_.items():
        path = os.path.join(out_dir, f"{key}_{kind}_повтор.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(store.strip_marker(text) + "\n\n" + store.MARKER + "\n")
        paths.append(path)
    return paths, llm_problem


def process(ctx, key, do_asr=True, do_llm=True):
    """
    Довести запись до готовности в рамках разрешённых фаз. Возвращает строку БД.
    do_asr — можно ли транскрибировать (ASR-окно открыто); do_llm — можно ли гонять LLM
    (improve/summarize) и пост-стадии (LLM-окно открыто). Флаги фиксируются диспетчером
    ДО старта записи и не перепроверяются по ходу — начатая стадия доводится до конца.
    """
    cfg = ctx.cfg
    row = db.get(ctx.db, key)
    if row is None:
        return None
    rd = row["rec_dir"]
    llm_ok = bool(ctx.llm and ctx.llm.available())
    protected = bool(row.get("protected"))
    post = do_llm or (not llm_ok)                    # пост-стадии: LLM-окно, либо всегда если LLM выкл
    ord_done = _old_records_done(ctx)                # разосланы ли old-records-уведомления
    delays = cfg.retry_delays_sec                    # лестница повторов упавшей стадии
    retry_in = None                                  # через сколько повторить упавшую в этом проходе

    def commit(**f):
        db.update(ctx.db, key, **f)
        row.update(f)

    def ok(**f):
        """Стадия выполнена: записать результат и сбросить счётчик неудач/«залипание»."""
        commit(last_error=None, error_since=None, fail_stage=None, fail_count=0, **f)

    def finish():
        return _finalize(ctx, row, cfg, llm_ok, protected, ord_done, retry_in)

    def fail(stage, problem, service=True):
        """Стадия не удалась: назначить следующий повтор по лестнице. Отсчёт идёт от ПЕРВОЙ
        ошибки этой стадии (error_since); успех стадии его сбрасывает. Возвращает True,
        когда повторы исчерпаны (что делать дальше — решает вызывающий).
        service=True — сбой внешнего сервиса стадии: если он лежит целиком (см. Outage), попытку
        не засчитываем, а лестницу записи начнём заново после его восстановления."""
        nonlocal retry_in
        text = _err(problem) if isinstance(problem, Exception) else str(problem)[:300]
        if service and OUTAGE.failed(stage, key, text):
            commit(fail_stage=stage, fail_count=0, error_since=None,
                   last_error=f"{text} (сервис недоступен — повтор без расхода попыток)")
            retry_in = OUTAGE.PROBE_SEC
            log.warning("%s: сервис недоступен — повтор без расхода попыток: %s",
                        statuses.STAGE_NAMES.get(stage, stage), text)
            return False
        same = row.get("fail_stage") == stage            # та же стадия падает снова
        since = (_parse_dt(row.get("error_since")) if same else None) or _now()
        n = (row.get("fail_count") or 0) + 1 if same else 1
        commit(fail_stage=stage, fail_count=n, last_error=text, error_since=since.isoformat())
        retry_in = retry_delay(delays, since)
        if retry_in is None:
            return True
        log.warning("%s не удалась (неудач подряд: %d), следующий повтор — в %s: %s",
                    statuses.STAGE_NAMES.get(stage, stage), n,
                    (_now() + timedelta(seconds=retry_in)).astimezone().strftime("%d.%m %H:%M"),
                    text)
        return False

    def hold(stage):
        """Повторы исчерпаны: запись ждёт решения админа (restart_passed вернёт в работу)."""
        commit(needs_admin=1, hold_notified=0, status=statuses.failed(stage))
        log.error("%s не удалась и после всех повторов — запись ждёт решения админа (%s): %s",
                  statuses.STAGE_NAMES.get(stage, stage), row.get("status"), row.get("last_error"))
        return finish()

    if _terminal(row):                               # нет аудио / тексты удалены / ждёт админа
        return finish()

    # Видео удалили (админ вручную) раньше, чем его расшифровали, — распознавать уже нечего.
    if row.get("deleted") and not row.get("transcribed"):
        commit(no_audio=1, last_error="видео удалено до расшифровки — распознавать нечего",
               error_since=None, fail_stage=None, fail_count=0)
        log.info("видео удалено до расшифровки — запись пропущена")
        return finish()

    # САМОПРОВЕРКА ФАЙЛОВ: стадия отмечена сделанной, а её файл пропал, пуст, обрезан или без
    # проверочной фразы — возвращаем запись на эту стадию (видео ещё в Толке, переделать можно).
    strict = strict_files(ctx.db)
    if not row.get("deleted"):
        broken = broken_outputs(row, strict)
        if broken and not _redo_allowed(cfg.out_dir):
            log.error("файлы записи неполные (%s), но предохранитель самопроверки сработал — "
                      "запись не трогаю (см. тревогу суперадмину)", describe(broken))
            return finish()
        if broken:
            log.warning("файлы записи неполные (%s) — возвращаю на переделку", describe(broken))
            set_aside(row, broken)
            commit(**redo_fields(row, broken))

    # --- СТАДИЯ 1: транскрибация (ASR-окно) ---
    if not row.get("transcribed") and do_asr:
        try:
            tr = build_transcript(ctx.client, key, ctx.asr, cfg.language)
            store.write_transcript(rd, key, tr.blocks)      # тело расшифровки (шапку добавит save_to_db)
        except NoAudio as ex:              # звука нет ни в архиве, ни в видео (или он пуст) — терминально
            OUTAGE.passed("transcribe")      # Толк и шлюз ответили — это не сбой сервиса
            commit(no_audio=1, last_error=_err(ex), error_since=None, fail_stage=None, fail_count=0)
            log.info("запись без аудио — пропущена: %s", ex)
            return finish()
        except Exception as ex:
            return hold("transcribe") if fail("transcribe", ex) else finish()
        OUTAGE.passed("transcribe")
        ok(transcribed=1, no_channels=int(tr.no_channels), status=statuses.TRANSCRIBED)
        log.info("транскрибация готова: %s, реплик %d", {
            "channels": "по дорожкам спикеров", "mixed": "по общей дорожке (без дорожек спикеров)",
            "video": "по звуку из видео (аудиоархива нет)"}.get(tr.info.get("source"), "?"),
            len(tr.blocks))

    # --- СТАДИЯ 2: улучшение через LLM (LLM-окно) ---
    if row.get("transcribed") and llm_ok and not row.get("improved") and do_llm:
        try:
            body = store.read_asr_md(rd, key) or ""      # ВХОД — дословный ASR (не трогаем оригинал)
            if body.strip():
                store.write_llm_md(rd, key, improve_transcript(body, ctx.llm))   # ВЫХОД — <ID>_LLM.md
        except Exception as ex:
            return hold("improve") if fail("improve", ex) else finish()
        OUTAGE.passed("improve")
        ok(improved=1, status=statuses.IMPROVED)
        log.info("LLM-правка выполнена")

    # --- СТАДИЯ 3: короткая выжимка (LLM-окно) ---
    if row.get("transcribed") and llm_ok and row.get("improved") and not row.get("summarized") and do_llm:
        try:
            # выжимаем из LLM-версии; если её вдруг нет (или она пуста) — из дословного ASR
            src = (store.read_llm_md(rd, key) or "").strip() or (store.read_asr_md(rd, key) or "")
            if src.strip():
                store.write_short_md(rd, key, summarize_text(src, ctx.llm))
        except Exception as ex:
            return hold("summarize") if fail("summarize", ex) else finish()
        OUTAGE.passed("summarize")
        ok(summarized=1, status=statuses.SUMMARIZED)
        log.info("выжимка готова")

    # --- СТАДИЯ 4: сохранение в БД (пост-стадия: LLM-окно) ---
    # ОБЯЗАТЕЛЬНО transcribed: без расшифровки сохранять нечего (терминальные записи сюда
    # не попадают, даже когда LLM выключен и ready=not llm_ok).
    ready = row.get("transcribed") and ((not llm_ok) or (row.get("improved") and row.get("summarized")))
    if ready and not row.get("saved_to_db") and post:
        try:
            participants = ctx.client.participants_with_emails(key)
            usersync.sync_participants(ctx.db, participants, ctx.mm)   # завести users
            header = _compose_header(row.get("title"), row.get("created_date"),
                                     row.get("duration_sec"), participants,
                                     no_channels=bool(row.get("no_channels")))
            # Шапку добавляем в начало КАЖДОГО файла (strip_header делает шаг идемпотентным,
            # снимая прежнюю шапку/баннер). Каждый файл самодостаточен.
            asr_body = store.read_asr_md(rd, key)
            asr_clean = store.strip_header(asr_body) if asr_body is not None else None
            if asr_clean is not None:
                store.write_asr_md(rd, key, header + "\n\n" + asr_clean)
            llm_body = store.read_llm_md(rd, key)
            if llm_body is not None:
                llm_clean = store.strip_header(llm_body)
                banner = _shrink_banner(cfg, asr_clean, llm_clean, f"{key}_ASR.md")
                store.write_llm_md(rd, key, banner + header + "\n\n" + llm_clean)
            # шапка и в короткой выжимке
            short_body = store.read_short_md(rd, key)
            if short_body is not None:
                store.write_short_md(rd, key, header + "\n\n" + store.strip_header(short_body))
            # full_path — для обратной совместимости; кнопки бота теперь берут _ASR/_LLM явно
            delivered = store.llm_path(rd, key) if llm_body is not None else store.asr_path(rd, key)
        except Exception as ex:
            return hold("save_to_db") if fail("save_to_db", ex) else finish()
        OUTAGE.passed("save_to_db")
        ok(participants=participants, full_path=delivered,
           short_path=store.short_path(rd, key), saved_to_db=1, status=statuses.SAVED)
        with_mail = sum(1 for p in participants if p.get("email"))
        log.info("запись сохранена в архив: участников %d, из них с почтой %d",
                 len(participants), with_mail)
        if participants and not with_mail:
            log.warning("ни у одного участника нет почты — запись никому не видна в боте, "
                        "и предупредить об удалении видео будет некого")

    # Защищённую (админом или запретом Толка) запись дальше — утилизация видео — не трогаем.
    if protected or row.get("kept_forever"):
        return finish()

    # --- СТАДИЯ 5: рассылка сообщений участникам (пост-стадия; видео старше года) ---
    # Пока не разосланы единоразовые old-records-уведомления (ord_done=False) — стадию
    # подавляем: идёт тихий бэкфилл. Записи бэклога уведомит демон разом, а не по одной.
    if (post and ord_done and row.get("saved_to_db") and not row.get("messages_sent")
            and not row.get("deleted") and deletion_eligible(row, cfg)):
        res = None
        try:
            # сначала убеждаемся, что видео вообще будет удалено: иначе уведомлять не о чем
            state = _video_state(ctx, key, row, commit)
            if state == "ok":
                _require_texts(row, strict)          # обещаем людям готовый текст — он должен быть
                participants = row.get("participants") or ctx.client.participants_with_emails(key)
                when_iso = row.get("delete_scheduled_at") or (
                    _now() + timedelta(seconds=cfg.delete_delay_sec)).isoformat()

                def sent(channel, username):
                    # отмечаем каждую доставку сразу — вместе со сроком удаления, который назван
                    # в уведомлении: упади демон посреди рассылки, повтор не пришлёт дублей и не
                    # сдвинет срок для тех, кто уже получил сообщение
                    col = "notified_usernames" if channel == "tag" else "notified_mail"
                    commit(delete_scheduled_at=when_iso,
                           **{col: sorted(set(row.get(col) or []) | {username})})

                res = send_archive_notifications(
                    participants,
                    title=row.get("title") or "",
                    date_str=_fmt_date(row.get("created_date")),
                    minutes=_minutes(row.get("duration_sec")),
                    delete_dt_str=_fmt_datetime(when_iso),
                    recording_key=key, mm=ctx.mm, tag_enabled=cfg.send_to_tag,
                    send_mail=cfg.send_to_mail,
                    tag_done=row.get("notified_usernames"), mail_done=row.get("notified_mail"),
                    on_sent=sent)
                commit(delete_scheduled_at=when_iso,
                       notified_usernames=sorted(set(row.get("notified_usernames") or [])
                                                 | set(res.tag_sent)),
                       notified_mail=sorted(set(row.get("notified_mail") or [])
                                            | set(res.mail_sent)))
        except Exception as ex:
            return hold("messages") if fail("messages", ex) else finish()
        if state != "ok" or res.reached_any:
            OUTAGE.passed("messages")        # Толк/Tag ответили — сервис в строю
        if state == "gone":
            ok(**db.deleted_fields("external", cfg.text_retention_days))
            log.info("видео в Толке уже нет (удалено мимо архиватора) — уведомлять не о чем")
        elif state == "blocked":
            ok(kept_forever=1, status=statuses.DELETE_PROTECTION)
            log.info("Толк запрещает удалять это видео — «сохранено бессрочно», больше не трогаем")
            return finish()
        elif res.complete:
            ok(messages_sent=1, status=statuses.NOTIFIED, unreachable=res.unreachable)
            log.info("участники уведомлены (Tag: %d, почта: %d, без уведомления: %d) — видео "
                     "будет удалено %s", len(row.get("notified_usernames") or []),
                     len(row.get("notified_mail") or []), len(res.unreachable),
                     _fmt_datetime(row.get("delete_scheduled_at")))
        else:
            # Не доставлено НИКОМУ и дело в связи — это поломка рассылки, а не «один человек
            # недоступен»: удалять видео, никого не предупредив, нельзя -> ждём админа.
            systemic = res.has_errors and not res.reached_any
            # недоступность отдельных людей — не сбой сервиса; «никому, и дело в связи» — сбой
            if not fail("messages", "; ".join(res.pending), service=systemic):
                return finish()
            if systemic:
                return hold("messages")
            # до отдельных участников так и не достучались — идём дальше, удаление по графику
            log.warning("рассылка: не удалось уведомить %s — видео будет удалено по графику",
                        ", ".join(res.unreachable) or "часть участников")
            ok(messages_sent=1, status=statuses.NOTIFIED, unreachable=res.unreachable)

    # --- СТАДИЯ 6: удаление видео (пост-стадия; через неделю после рассылки) ---
    if post and row.get("messages_sent") and not _delete_done(row, cfg):
        due = _parse_dt(row.get("delete_scheduled_at"))
        if due and _now() >= due:
            by = None                                # кто удалил (None — удаления не было)
            changed = None                           # свежая строка, если запись изменили на ходу
            try:
                state = _video_state(ctx, key, row, commit)
                if state != "blocked":
                    # Удаление необратимо, а строку записи мы читали в начале прохода: пока
                    # она была в работе, админ мог запретить удаление или удалить видео сам.
                    fresh = db.get(ctx.db, key) or {}
                    if fresh.get("deleted") or (state == "ok" and (
                            fresh.get("protected") or fresh.get("kept_forever"))):
                        changed = fresh
                if changed is not None:
                    pass                             # уступаем решению админа (см. ниже)
                elif state == "gone":
                    by = "external"
                elif state == "ok":
                    _require_texts(row, strict)      # без полной расшифровки на диске видео не удаляем
                    if not cfg.simulate_delete:
                        try:
                            ctx.client.delete_recording(key, force=cfg.force_delete)
                            by = "auto"
                        except Exception as ex:
                            if _http_status(ex) != 404:
                                raise
                            by = "external"          # успели удалить мимо архиватора
            except Exception as ex:
                return hold("delete") if fail("delete", ex) else finish()
            OUTAGE.passed("delete")
            if changed is not None:
                row.update(changed)
                protected = bool(row.get("protected"))
                log.info("  [delete] %s: запись изменили, пока она была в работе (запрет "
                         "удаления или ручное удаление) — видео не трогаю", key)
                return finish()
            if state == "blocked":
                ok(kept_forever=1, status=statuses.DELETE_PROTECTION)
                log.info("  [delete] %s: Толк запрещает удалять это видео — «сохранено "
                         "бессрочно», больше не трогаем", key)
                return finish()
            if by:
                # TEXT_RETENTION_DAYS не задан (None) -> тексты не удаляем никогда: срок их
                # очистки не выставляется, и стадия очистки не сработает.
                ok(**db.deleted_fields(by, cfg.text_retention_days))
                log.info("  [delete] видео %s %s", key, "удалено из Толка" if by == "auto"
                         else "в Толке уже нет (удалено мимо архиватора)")
            else:
                log.info("  [delete:ИМИТАЦИЯ] %s: срок наступил (%s) — реальное удаление "
                         "пропущено (REAL_DELETE=0)", key, row.get("delete_scheduled_at"))
                ok(delete_simulated=1)

    # --- СТАДИЯ 7: очистка текстов (пост-стадия; через 5 лет после удаления видео) ---
    if post and row.get("deleted") and not row.get("text_cleaned"):
        due = _parse_dt(row.get("text_delete_at"))
        if due and _now() >= due:
            try:
                store.remove_record_dir(rd, key)
            except Exception as ex:
                return hold("cleanup_text") if fail("cleanup_text", ex, service=False) else finish()
            ok(text_cleaned=1, status=statuses.TEXT_DELETED)
            log.info("  [cleanup] %s: срок хранения истёк — тексты удалены (%s)",
                     key, row.get("text_delete_at"))

    return finish()
