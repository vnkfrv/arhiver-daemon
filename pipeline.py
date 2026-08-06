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
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import config
import db
import statuses
import store
from transcribe import build_transcript
from enrich import improve_transcript, summarize_text
from notify import send_archive_notifications

log = logging.getLogger("pipeline")

# Через сколько повторить стадию, которая была доступна по окну, но упала.
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


def _err(ex):
    return f"{type(ex).__name__}: {ex}"[:300]


def _parse_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def _fmt_date(iso):
    dt = _parse_dt(iso)
    return dt.astimezone().strftime("%d.%m.%Y") if dt else "—"


def _fmt_datetime(iso):
    dt = _parse_dt(iso)
    return dt.astimezone().strftime("%d.%m.%Y в %H:%M") if dt else "—"


def _minutes(sec):
    sec = int(sec or 0)
    return max(1, round(sec / 60)) if sec else 0


def _compose_header(title, created_date, duration_sec, participants):
    """Шапка <ID>_full.md: Название/Дата создания/Длительность/Участники (через запятую)."""
    names = ", ".join(p.get("name") for p in participants if p.get("name")) or "—"
    return (f"Название: {title or '—'}\n"
            f"Дата создания: {_fmt_date(created_date)}\n"
            f"Длительность: {_minutes(duration_sec)} мин\n"
            f"Участники конференции: {names}")


def _now():
    return datetime.now(timezone.utc)


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


def stages_pending(row, cfg, llm_ok, protected=False, now=None):
    """Стадии, готовые к выполнению СЕЙЧАС. Пустой список = на сейчас делать нечего."""
    now = now or _now()
    if row.get("no_audio") or row.get("text_cleaned"):
        return []                                   # терминальные состояния
    if not row.get("transcribed"):
        return ["transcribe"]
    p = []
    if llm_ok and not row.get("improved"):
        p.append("improve")
    if llm_ok and row.get("improved") and not row.get("summarized"):
        p.append("summarize")
    ready = (not llm_ok) or (row.get("improved") and row.get("summarized"))
    if ready and not row.get("saved_to_db"):
        p.append("save_to_db")

    if not protected:
        if (row.get("saved_to_db") and not row.get("messages_sent")
                and not row.get("deleted") and deletion_eligible(row, cfg, now)):
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


def _next_due(row, cfg, protected):
    """Ближайший БУДУЩИЙ момент, когда запись снова потребует внимания (или None).
    Вызывается, когда на сейчас делать нечего (stages_pending пуст)."""
    if row.get("no_audio") or row.get("text_cleaned"):
        return None
    if row.get("deleted"):                          # ждём 5-летней очистки текстов
        return _parse_dt(row.get("text_delete_at"))
    if protected:                                   # хранить навсегда — авто-действий нет
        return None
    if row.get("saved_to_db") and not row.get("messages_sent"):
        dt = _parse_dt(row.get("created_date"))     # ждём, пока видео станет старше года
        return dt + timedelta(days=cfg.delete_video_age_days) if dt else None
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


def _finalize(ctx, row, cfg, llm_ok, protected):
    """Проставить планирование (done/next_due_at) с учётом рабочих окон и вернуть строку."""
    key = row["recording_key"]
    now_local = datetime.now().astimezone()
    pend = stages_pending(row, cfg, llm_ok, protected, _now())
    if not pend:
        nd = _next_due(row, cfg, protected)          # будущее по расписанию (возраст/удаление/очистка)
        nd_iso, done = (nd.isoformat() if nd else None), (0 if nd else 1)
    else:
        nxt = config.next_open_local(now_local, _stage_window(cfg, pend[0], llm_ok))
        if nxt is None:                              # окно открыто (или не задано), но стадия
            nd_iso = (_now() + timedelta(seconds=_RETRY_BACKOFF_SEC)).isoformat()  # осталась -> сбой -> backoff
        else:
            nd_iso = nxt.astimezone(timezone.utc).isoformat()   # ждём открытия окна стадии
        done = 0
    db.update(ctx.db, key, done=done, next_due_at=nd_iso)
    row["done"], row["next_due_at"] = done, nd_iso
    return row


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

    def commit(**f):
        db.update(ctx.db, key, **f)
        row.update(f)

    def commit_fail(ex, **extra):
        # стадия упала: пишем last_error и ФИКСИРУЕМ момент «залипания» (если ещё не задан) —
        # по нему EMERGENCY-алерт поймёт, что что-то недоступно уже давно.
        f = {"last_error": _err(ex)}
        if not row.get("error_since"):
            f["error_since"] = _now().isoformat()
        f.update(extra)
        commit(**f)

    # --- СТАДИЯ 1: транскрибация (ASR-окно) ---
    if not row.get("transcribed") and not row.get("no_audio") and do_asr:
        try:
            _, blocks, _ = build_transcript(ctx.client, key, ctx.asr, cfg.language)
            store.write_transcript(rd, key, blocks)      # тело расшифровки (шапку добавит save_to_db)
            commit(transcribed=1, status=statuses.TRANSCRIBED, last_error=None, error_since=None)
        except Exception as ex:
            if _http_status(ex) in (400, 404):           # нет аудио — не «сбой шлюза»
                commit(no_audio=1, last_error=_err(ex), error_since=None)
                log.info("нет аудио (%s) — запись пропущена", _http_status(ex))
            else:
                log.warning("транскрибация не удалась, повтор позже: %s", _err(ex))
                commit_fail(ex)
            return _finalize(ctx, row, cfg, llm_ok, protected)

    # --- СТАДИЯ 2: улучшение через LLM (LLM-окно) ---
    if row.get("transcribed") and llm_ok and not row.get("improved") and do_llm:
        try:
            body = store.read_full_md(rd, key) or ""
            if body.strip():
                store.write_full_md(rd, key, improve_transcript(body, ctx.llm))
            commit(improved=1, status=statuses.IMPROVED, last_error=None, error_since=None)
        except Exception as ex:
            log.warning("LLM-правка не удалась, повтор позже: %s", _err(ex))
            commit_fail(ex)

    # --- СТАДИЯ 3: короткая выжимка (LLM-окно) ---
    if row.get("transcribed") and llm_ok and row.get("improved") and not row.get("summarized") and do_llm:
        try:
            full_md = store.read_full_md(rd, key) or ""
            if full_md.strip():
                store.write_short_md(rd, key, summarize_text(full_md, ctx.llm))
            commit(summarized=1, status=statuses.SUMMARIZED, last_error=None, error_since=None)
        except Exception as ex:
            log.warning("выжимка не удалась, повтор позже: %s", _err(ex))
            commit_fail(ex)

    # --- СТАДИЯ 4: сохранение в БД (пост-стадия: LLM-окно) ---
    ready = (not llm_ok) or (row.get("improved") and row.get("summarized"))
    if ready and not row.get("saved_to_db") and post:
        try:
            participants = ctx.client.participants_with_emails(key)
            # добавить шапку в начало <ID>_full.md (strip_header делает шаг идемпотентным)
            body = store.strip_header(store.read_full_md(rd, key) or "")
            header = _compose_header(row.get("title"), row.get("created_date"),
                                     row.get("duration_sec"), participants)
            store.write_full_md(rd, key, header + "\n\n" + body)
            commit(participants=participants,
                   full_path=store.full_path(rd, key),
                   short_path=store.short_path(rd, key),
                   saved_to_db=1, status=statuses.SAVED, last_error=None, error_since=None)
        except Exception as ex:
            log.warning("сохранение в БД не удалось, повтор позже: %s", _err(ex))
            commit_fail(ex)

    # Защищённую запись дальше (утилизация видео) не трогаем.
    if protected:
        return _finalize(ctx, row, cfg, llm_ok, protected)

    # --- СТАДИЯ 5: рассылка сообщений участникам (пост-стадия; видео старше года) ---
    if (post and row.get("saved_to_db") and not row.get("messages_sent")
            and not row.get("deleted") and deletion_eligible(row, cfg)):
        try:
            participants = row.get("participants") or ctx.client.participants_with_emails(key)
            when_iso = row.get("delete_scheduled_at") or (
                _now() + timedelta(seconds=cfg.delete_delay_sec)).isoformat()
            already = set(row.get("notified_usernames") or [])
            all_ok, notified = send_archive_notifications(
                participants,
                title=row.get("title") or "",
                date_str=_fmt_date(row.get("created_date")),
                minutes=_minutes(row.get("duration_sec")),
                delete_dt_str=_fmt_datetime(when_iso),
                recording_key=key, mm=ctx.mm, already=already)
            fields = {"delete_scheduled_at": when_iso,
                      "notified_usernames": sorted(already | set(notified))}
            if all_ok:
                fields.update(messages_sent=1, status=statuses.MESSAGED, error_since=None)
            commit(**fields)
        except Exception as ex:
            log.warning("рассылка сообщений не удалась, повтор позже: %s", _err(ex))
            commit_fail(ex)

    # --- СТАДИЯ 6: удаление видео (пост-стадия; через неделю после рассылки) ---
    if post and row.get("messages_sent") and not _delete_done(row, cfg):
        due = _parse_dt(row.get("delete_scheduled_at"))
        if due and _now() >= due:
            if cfg.simulate_delete:
                log.info("  [delete:ИМИТАЦИЯ] %s: срок наступил (%s) — реальное удаление "
                         "пропущено (MODE=test)", key, row.get("delete_scheduled_at"))
                commit(delete_simulated=1)
            elif row.get("can_be_removed"):
                try:
                    ctx.client.delete_recording(key, force=cfg.force_delete)
                    now = _now()
                    commit(deleted=1, deleted_at=now.isoformat(),
                           text_delete_at=(now + timedelta(days=cfg.text_retention_days)).isoformat(),
                           status=statuses.DELETED, last_error=None, error_since=None)
                    log.info("  [delete] видео %s удалено из Толка", key)
                except Exception as ex:
                    log.warning("удаление видео не удалось, повтор позже: %s", _err(ex))
                    commit_fail(ex)
            else:
                log.warning("  %s: срок удаления наступил, но canBeRemoved=false "
                            "(нужен FORCE_DELETE) — пропуск", key)

    # --- СТАДИЯ 7: очистка текстов (пост-стадия; через 5 лет после удаления видео) ---
    if post and row.get("deleted") and not row.get("text_cleaned"):
        due = _parse_dt(row.get("text_delete_at"))
        if due and _now() >= due:
            store.remove_record_dir(rd)
            commit(text_cleaned=1, status=statuses.TEXT_DELETED)
            log.info("  [cleanup] %s: 5 лет истекли — тексты удалены (%s)",
                     key, row.get("text_delete_at"))

    return _finalize(ctx, row, cfg, llm_ok, protected)
