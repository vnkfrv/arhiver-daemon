#!/usr/bin/env python3
"""
main.py — АВТОНОМНЫЙ ДЕМОН архивации записей Толка.

Каждый проход:
  1. ПОДХВАТ (когда открыто ASR-окно): заводим новые записи — все вплоть до конца
     прошлого дня (от старых к новым). Подхват создаёт только СТРОКУ в БД.
  2. ОБРАБОТКА: берём из БД записи, которым ПОРА (due_records по индексу), и обрабатываем
     их ПАРАЛЛЕЛЬНО (RECORD_WORKERS воркеров, у каждого своя запись). Стадии гейтятся
     рабочими окнами: транскрибация — в ASR-окне, LLM (improve/summarize) и пост-стадии —
     в LLM-окне. Окно проверяется ПЕРЕД стартом записи; начатую доводим до конца.
  3. СЛУЖЕБНОЕ (после прохода и раз в минуту во время него): фаза единоразового
     old-records-уведомления.

Отдельным потоком — суточная сводка админам (ADMIN_REPORT_TIME, по умолчанию 9:00) о
записях, которые за сутки встали «до решения админа»; если таких нет — молчим.
  4. Спим до ближайшей смены фазы/открытия окна.

Раздельные окна (env): ASR (TIME_START_ASR–TIME_END_ASR) обычно шире, LLM
(TIME_START_LLM–TIME_END_LLM) уже. ASR готовит расшифровки (статус TRANSCRIBED), LLM
позже обрабатывает подготовленное. Планирование целиком в БД (done/next_due_at).

    python main.py            # демон (бесконечно)
    python main.py --once     # один проход и выход
    python main.py --record <key>   # обработать одну запись по ключу и выйти
"""

import argparse
import logging
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import config
import db
import logsetup
import statuses
import store
import pipeline
import notify
import texts
from pipeline import Ctx
from config import load_cfg
from talk_api import TalkClient
from asr import get_asr
from llm import get_llm
from util import err as _err, parse_dt as _parse_dt

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

log = logging.getLogger("daemon")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _start_of_today_utc():
    """Локальная полночь текущих суток в UTC — граница «весь прошлый день»."""
    local_midnight = (
        datetime.now().astimezone().replace(hour=0, minute=0, second=0, microsecond=0)
    )
    return local_midnight.astimezone(timezone.utc)


def _local_now():
    return datetime.now().astimezone()


def _windows(cfg):
    return [w for w in (cfg.asr_window, cfg.llm_window) if w]


def _any_open(cfg, now):
    """Открыто ли хоть одно окно (или окон нет вовсе -> всегда активны)."""
    if not _windows(cfg):
        return True
    return config.in_window(now, cfg.asr_window) or config.in_window(
        now, cfg.llm_window
    )


def _next_boundary_secs(cfg):
    """Секунд до ближайшей границы любого окна (открытие/закрытие) — чтобы проснуться,
    когда фаза сменится. None, если окон нет."""
    now = _local_now()
    edges = []
    for w in _windows(cfg):
        for hh, mm in w:
            t = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if t <= now:
                t += timedelta(days=1)
            edges.append(t)
    return max(1.0, (min(edges) - now).total_seconds()) if edges else None


def _sleep_secs(cfg):
    """Сколько спать до следующей проверки. Темп всегда задаётся окнами (они обязательны)."""
    now = _local_now()
    if _any_open(cfg, now):
        b = _next_boundary_secs(cfg)  # спим до смены фазы, но не дольше 5 мин
        return min(b, 300.0) if b else 300.0
    # все окна закрыты — спим до ближайшего открытия
    opens = [config.next_open_local(now, w) for w in _windows(cfg)]
    opens = [o for o in opens if o]
    return max(1.0, (min(opens) - now).total_seconds()) if opens else 300.0


def _enroll(ctx, key, meta):
    """Завести запись в БД (папку не создаём — появится при записи файлов)."""
    cd = meta.get("createdDate")
    db.enroll(
        ctx.db,
        key,
        created_date=cd,
        rec_dir=store.rec_dir(ctx.cfg.out_dir, key, cd),
        title=meta.get("title"),
        duration_sec=meta.get("duration"),
        can_be_removed=bool(meta.get("canBeRemoved")),
    )


# Поля строки БД, сбрасывающие отсчёт повторов удаления короткой записи.
_SHORT_OK = {"last_error": None, "error_since": None, "fail_stage": None, "fail_count": 0,
             "next_due_at": None}


def _delete_short(ctx, key, blocked=False):
    """Удалить из Толка слишком короткую запись. Возвращает (поля её строки в БД, что
    произошло — для лога). blocked — Толк запрещает удалять это видео, а FORCE_DELETE
    выключен: не трогаем его вовсе. Сбой удаления — исключение (решает вызывающий)."""
    cfg = ctx.cfg
    if cfg.simulate_delete:                     # test — только лог
        return {"delete_simulated": 1}, "удаление ИМИТАЦИЯ (REAL_DELETE=0)"
    if blocked:
        return {"kept_forever": 1}, "Толк запрещает её удалять — остаётся в Толке"
    try:
        ctx.client.delete_recording(key, force=cfg.force_delete)
        by, what = "short", "удалена из Толка"
    except Exception as ex:
        if pipeline._http_status(ex) != 404:
            raise
        by, what = "external", "в Толке её уже нет"
    return {"deleted": 1, "deleted_at": _now_iso(), "deleted_by": by}, what


def _short_failed(ctx, key, ex, since, attempt):
    """Удалить короткую запись не удалось: поля строки БД с ошибкой и сроком следующей
    попытки — по той же лестнице, что у стадий пайплайна (от первой ошибки since)."""
    now = datetime.now(timezone.utc)
    retry_in = pipeline.retry_delay(ctx.cfg.retry_delays_sec, since, now)
    retry_at = now + timedelta(seconds=retry_in) if retry_in is not None else None
    log.warning("короткая запись %s: удалить из Толка не удалось (попытка %d): %s — %s",
                key, attempt, _err(ex),
                "повтор в " + retry_at.astimezone().strftime("%d.%m %H:%M") if retry_at
                else "повторы исчерпаны, видео осталось в Толке (можно удалить из админ-панели)")
    return {"last_error": _err(ex), "error_since": since.isoformat(), "fail_stage": "delete",
            "fail_count": attempt, "next_due_at": retry_at.isoformat() if retry_at else None}


def _handle_short(ctx, key, meta, dur):
    """Слишком короткая запись: не транскрибируем, УДАЛЯЕМ (prod — реально, test —
    заглушка в лог) и ставим «надгробие» в БД (done=1), чтобы не наткнуться повторно
    и чтобы водяной знак прошёл дальше. Удалить не удалось — надгробие хранит ошибку и
    срок повтора (см. _retry_short)."""
    cfg = ctx.cfg
    cd = meta.get("createdDate")
    blocked = meta.get("canBeRemoved") is False and not cfg.force_delete
    try:
        extra, what = _delete_short(ctx, key, blocked)
        log.info("короткая запись %s (%sс < %d) — %s", key, dur, cfg.min_duration, what)
    except Exception as ex:
        extra = _short_failed(ctx, key, ex, datetime.now(timezone.utc), 1)
    db.enroll(ctx.db, key, created_date=cd, rec_dir=store.rec_dir(cfg.out_dir, key, cd),
              title=meta.get("title"), duration_sec=dur, can_be_removed=bool(meta.get("canBeRemoved")))
    db.update(ctx.db, key, **{"status": statuses.TOO_SHORT, "done": 1, "next_due_at": None, **extra})


def _retry_short(ctx):
    """Повторить удаление коротких записей, не удавшееся при подхвате (Толк моргнул или
    ограничил частоту запросов на «горе» записей). Лестница пройдена — видео остаётся в
    Толке: ошибка видна в карточке записи, удалить его можно из админ-панели."""
    for row in db.short_retries(ctx.db, _now_iso()):
        key = row["recording_key"]
        try:
            fields, what = _delete_short(ctx, key)
            fields.update(_SHORT_OK)
            log.info("короткая запись %s — %s (с повторной попытки)", key, what)
        except Exception as ex:
            since = _parse_dt(row.get("error_since")) or datetime.now(timezone.utc)
            fields = _short_failed(ctx, key, ex, since, (row.get("fail_count") or 0) + 1)
            if pipeline._http_status(ex) is None:
                # Толк не ответил вовсе (сеть, таймаут) — остальные сейчас не трогаем: не
                # ждать же таймаут на каждой; их срок никуда не делся, вернёмся на след. проходе
                db.update(ctx.db, key, **fields)
                break
        db.update(ctx.db, key, **fields)


def _discover(ctx):
    """Подхватить ВСЕ записи вплоть до конца прошлого дня (от старых к новым) — одинаково
    для test и prod. Водяной знак — MAX(created_date) из БД (без state-файла).
    Короче MIN_DURATION -> сразу удаляем (не транскрибируем)."""
    cfg = ctx.cfg
    cutoff = _start_of_today_utc()
    since = db.max_created(ctx.db)
    discovered = shortdel = 0
    for meta in ctx.client.list(since=since, order="byTimeOldFirst"):
        try:
            dt = _parse_dt(meta.get("createdDate"))
            if dt is None:
                continue
            if dt >= cutoff:  # дошли до записей текущего дня — стоп
                break
            key = meta.get("key")
            if not key or db.exists(ctx.db, key):
                continue
            dur = meta.get("duration")
            if dur is not None and dur < cfg.min_duration:   # слишком короткая -> удалить
                _handle_short(ctx, key, meta, dur)
                shortdel += 1
                continue
            # В элементе списка v2 уже есть title/duration/canBeRemoved/createdDate —
            # отдельный recording_detail на КАЖДУЮ запись не нужен (критично на «горе»).
            _enroll(ctx, key, meta)
            discovered += 1
        except sqlite3.Error:
            # Сбой БД — не «битая запись»: прерываем подхват. Пропусти мы её, следующие
            # записи увели бы водяной знак (MAX(created_date)) дальше, и эта запись не была
            # бы подхвачена уже никогда. Следующий проход начнёт с неё же.
            raise
        except Exception as ex:      # один битый элемент не должен рвать весь подхват
            log.warning("подхват записи %s пропущен: %s", meta.get("key"), _err(ex))
            continue
    if shortdel:
        log.info("подхват: коротких (< %dс) удалено/помечено: %d", cfg.min_duration, shortdel)
    return discovered


# У каждого потока-воркера свой TalkClient: общий requests.Session не рассчитан на
# конкурентное использование (скачивание аудио/видео, участники, удаление).
_tls = threading.local()


def _worker_client():
    c = getattr(_tls, "client", None)
    if c is None:
        c = _tls.client = TalkClient()
    return c


def _process_one(base_ctx, key, do_asr, do_llm):
    """Обработать одну запись в отдельном потоке (свой TalkClient; лимитер и БД — общие).
    do_asr/do_llm фиксированы диспетчером; начатые стадии доводятся до конца.
    set_record — чтобы все логи этой записи (в т.ч. из asr/llm/enrich) шли с её ключом.
    """
    logsetup.set_record(key)
    try:
        ctx = replace(base_ctx, client=_worker_client())
        return pipeline.process(ctx, key, do_asr, do_llm)
    finally:
        logsetup.clear_record()


# Записи, обработка которых сорвалась целиком (а не на стадии): ключ -> монотонное время,
# раньше которого запись не берём. Отсрочку в БД (next_due_at) при этом тоже ставим, но
# сорваться обработка могла как раз из-за БД — память страхует от повторов по кругу.
_cooldown = {}


def _run_active(ctx):
    """Пока открыто хоть одно окно — обрабатывать записи НЕПРЕРЫВНО: воркеры НЕЗАВИСИМЫ,
    как только один освободился — сразу берёт следующую запись (не ждёт соседей). Держим
    до record_workers записей одновременно. Окно проверяется ПЕРЕД стартом каждой записи;
    начатую доводим до конца (do_asr/do_llm фиксируются при старте). Возвращает число
    обработанных записей."""
    cfg = ctx.cfg
    llm_ok = bool(ctx.llm and ctx.llm.available())
    inflight = set()  # ключи, что сейчас в работе (чтобы не выдать одну дважды)
    lock = threading.Lock()
    stats = {"worked": 0}

    def _done(fut, key):
        try:
            row = fut.result()
            if row is None:
                return
            with lock:
                stats["worked"] += 1
            still = pipeline.stages_pending(
                row, cfg, llm_ok, bool(row.get("protected")),
                old_records_done=pipeline._old_records_done(ctx),
            )
            log.info(
                "✓ %s | %s%s",
                key,
                row.get("status"),
                (" | осталось: " + ",".join(still)) if still else "",
            )
        except Exception as ex:
            # краткий лог (без простыни трейсбека): детали стадий уже залогированы внутри
            log.error("✗ ошибка обработки %s: %s", key, _err(ex))
            with lock:
                _cooldown[key] = time.monotonic() + pipeline._RETRY_BACKOFF_SEC
            try:  # анти-тайтлуп: отодвинуть next_due, чтобы упавшая запись не крутилась
                db.update(
                    ctx.db,
                    key,
                    next_due_at=(
                        datetime.now(timezone.utc)
                        + timedelta(seconds=pipeline._RETRY_BACKOFF_SEC)
                    ).isoformat(),
                )
            except Exception as ex2:
                log.warning("отсрочка записи %s в БД не записана: %s", key, _err(ex2))
        finally:
            # слот освобождаем В КОНЦЕ: к этому моменту передышка сорвавшейся записи уже
            # выставлена, и диспетчер не успеет выдать её повторно
            with lock:
                inflight.discard(key)

    with ThreadPoolExecutor(
        max_workers=cfg.record_workers, thread_name_prefix="rec"
    ) as pool:
        while True:
            _service(ctx, every=60)          # повторы рассылки не ждут конца долгого прохода
            now = _local_now()
            da = config.in_window(now, cfg.asr_window)  # True если окна нет
            dl = config.in_window(now, cfg.llm_window)
            if not (da or dl):
                break  # все окна закрыты — новые не стартуем
            with lock:
                free = cfg.record_workers - len(inflight)
                busy = len(inflight)
                for k in [k for k, until in _cooldown.items() if until <= time.monotonic()]:
                    del _cooldown[k]
                resting = set(_cooldown)
            if free <= 0:
                time.sleep(0.2)  # все воркеры заняты — ждём слот
                continue
            # берём записи ТЕКУЩЕЙ фазы (фильтр в SQL — иначе старые llm-записи вытеснят
            # свежие asr из-под limit и транскрибация «голодает»); с запасом на inflight.
            phase = None if (da and dl) else ("asr" if da else "llm")
            rows = db.due_records(
                ctx.db, _now_iso(),
                limit=free + busy + cfg.record_workers + len(resting), phase=phase
            )
            launched = 0
            for r in rows:
                if launched >= free:
                    break
                key = r["recording_key"]
                if key in resting:
                    continue             # обработка недавно сорвалась — даём передышку
                with lock:
                    if key in inflight:
                        continue
                    inflight.add(key)
                pool.submit(_process_one, ctx, key, da, dl).add_done_callback(
                    lambda f, k=key: _done(f, k)
                )
                launched += 1
            if launched < free:  # свободной работы не хватило заполнить слоты
                if busy == 0 and launched == 0:
                    break  # и никто не в работе -> дел на сейчас нет
                time.sleep(0.3)  # ждём завершения текущих (появятся слоты/работа)
    return stats["worked"]


# --- АЛЕРТЫ АДМИНАМ ----------------------------------------------------------
_alert_mm = None            # ленивый MM-клиент только для алертов (даже при SEND_TO_TAG=0)


def _notify_admins(ctx, text, buttons=False):
    """Написать всем админам в Tag (+ всегда в лог ERROR) — от имени АДМИН-БОТА (если его
    токен не задан, от основного). MM-клиент строим лениво и отдельно от рассылки
    участникам, чтобы сообщение уходило даже при SEND_TO_TAG=0.
    buttons=True — приложить кнопки «перезапустить остановленные» / «админ-панель».
    Возвращает False, если Tag настроен, но сообщение не дошло ни до одного админа
    (стоит повторить позже); True — доставлено либо слать некуда (Tag не настроен)."""
    log.error(text)
    cfg = ctx.cfg
    if not (cfg.mm.url and (cfg.mm.admin_bot_token or cfg.mm.bot_token)):
        return True
    admins = db.list_admins(ctx.db)
    if not admins:
        log.error("в БД нет админов — некому слать алерт (задай ADMIN_USERNAME и запусти бота)")
        return False
    global _alert_mm
    sent = 0
    try:
        if _alert_mm is None:
            from mm_client import admin_client
            mm = admin_client(cfg.mm, verify=cfg.mm.verify)
            mm.login()
            _alert_mm = mm
        # кнопки ведут на бота — без его публичного адреса они бесполезны
        attachments = (_alert_mm.admin_alert_attachment(text)
                       if buttons and _alert_mm.public_url else None)
        for u in admins:
            try:
                _alert_mm.send_dm(u, text, attachments=attachments)
                sent += 1
            except Exception as ex:
                log.warning("алерт админу @%s не доставлен: %s", u, ex)
    except Exception as ex:
        log.error("не удалось отправить алерт в Tag: %s", ex)
    return sent > 0


# --- СВОДКА АДМИНАМ: раз в сутки и только если есть о чём -------------------
_REPORT_DATE = "admin_report_date"   # meta: локальная дата последней закрытой сводки
_report_done = None                  # она же в памяти (чтобы не читать БД каждую минуту)
_report_retry_after = 0.0            # монотонное время, раньше которого не повторяем отправку


def _report_text(ctx, rows):
    """Текст суточной сводки: сколько записей встало и на каких стадиях (с примером ошибки)."""
    by_stage = {}
    for r in rows:
        by_stage.setdefault(r.get("fail_stage"), []).append(r)
    lines = []
    for stage, items in sorted(by_stage.items(), key=lambda x: -len(x[1])):
        last = items[-1]
        lines.append(texts.ADMIN_REPORT_STAGE.format(
            status=statuses.failed(stage),
            name=statuses.STAGE_NAMES.get(stage, stage or "?"), n=len(items),
            key=last["recording_key"], error=(last.get("last_error") or "—")[:150]))
    older = sum(db.hold_counts(ctx.db).values()) - len(rows)
    return texts.ADMIN_DAILY_REPORT.format(
        n=len(rows), stages="\n".join(lines),
        older=texts.ADMIN_REPORT_OLDER.format(n=older) if older > 0 else "")


def _admin_report(ctx, now=None):
    """Суточная сводка админам. Раз в сутки, начиная с ADMIN_REPORT_TIME (по умолчанию 9:00),
    шлём ОДНО сообщение обо всех записях, которые встали «до решения админа» с прошлой
    сводки, с разбивкой по стадиям — сколько бы их ни было (если стадия отвалилась целиком,
    админы получат одну сводку, а не сотни сообщений). Никто не встал — молчим. Не дошло
    (Tag недоступен) — пробуем раз в полчаса, пока не уйдёт."""
    global _report_done, _report_retry_after
    now = now or _local_now()
    if (now.hour, now.minute) < tuple(ctx.cfg.admin_report_time):
        return                                          # время сводки ещё не наступило
    today = now.date().isoformat()
    if _report_done == today:
        return
    if db.get_meta(ctx.db, _REPORT_DATE) == today:      # уже закрыта (например, до перезапуска)
        _report_done = today
        return
    rows = db.holds(ctx.db, only_unnotified=True)
    if rows:
        if time.monotonic() < _report_retry_after:
            return
        if not _notify_admins(ctx, _report_text(ctx, rows), buttons=True):
            _report_retry_after = time.monotonic() + 1800
            return
        db.mark_holds_notified(ctx.db, [r["recording_key"] for r in rows])
    db.set_meta(ctx.db, _REPORT_DATE, today)
    _report_done = today


def _report_loop(ctx):
    """Фоновый поток сводки. Отдельный от главного цикла: в час сводки демон обычно спит
    между рабочими окнами либо часами без перерыва разбирает очередь."""
    while True:
        try:
            _admin_report(ctx)
        except Exception as ex:
            log.warning("сводка админам: сбой: %s", _err(ex))
        time.sleep(60)


_last_service = 0.0         # монотонное время последнего служебного прохода


def _service(ctx, every=0):
    """Служебные дела демона между запусками записей: продвинуть фазу old-records. Зовётся
    после каждого прохода и раз в минуту ВНУТРИ него (every=60): разбор «горы» записей может
    идти часами без перерыва, а повторы рассылки должны уходить в срок."""
    global _last_service
    now = time.monotonic()
    if every and now - _last_service < every:
        return
    _last_service = now
    try:
        _maybe_old_records_phase(ctx)
    except Exception as ex:
        log.warning("служебный проход не удался: %s", _err(ex))


# --- OLD-RECORDS: единоразовое уведомление о старых записях ------------------
_last_old_records_check = None      # дата последней суточной проверки фазы
_old_records_thread = None          # фоновый поток рассылки (чтобы не блокировать тик)
# Лестница повторов рассылки хранится в meta (переживает перезапуск демона):
_OLD_SINCE = "old_records_retry_since"      # когда рассылка впервые дошла не до всех (ISO)
_OLD_NEXT_TRY = "old_records_next_try_at"   # когда следующий повтор (ISO) или пусто


def _old_records_phase(ctx):
    """Если бэклог старых записей (старше DELETE_VIDEO_AGE_DAYS) полностью обработан —
    разослать единоразовое уведомление ожидающим пользователям и, когда уведомлены ВСЕ,
    выставить флаг old_records_all_notified=1 и запланировать удаление бэклога.

    До кого-то не достучались — повторяем по той же лестнице, что и стадии записи (через
    5 мин, 30 мин, 3 ч и 23 ч от первой неполной рассылки), после чего идём дальше без них:
    отдельные недоступные пользователи фазу не держат. Но если не получил вообще НИКТО и
    дело в связи — это поломка рассылки: удаление не планируем, зовём админов и завтра
    начинаем заново.

    Ничего не делает при сухом прогоне (нет ни одного канала) или если флаг уже выставлен."""
    cfg = ctx.cfg
    db.set_meta(ctx.db, _OLD_NEXT_TRY, "")              # повтор назначим ниже, если понадобится
    if db.get_meta(ctx.db, pipeline.OLD_RECORDS_FLAG) == "1":
        return                                          # фаза уже завершена
    need_tag, need_mail = cfg.send_to_tag, cfg.send_to_mail
    if not (need_tag or need_mail):
        return                                          # сухой прогон — не уведомляем/не удаляем

    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(days=cfg.delete_video_age_days)).isoformat()
    pending_backlog = db.backlog_pending_count(ctx.db, cutoff)
    if pending_backlog > 0:
        log.info("old-records: бэклог ещё обрабатывается (осталось %d) — рассылка позже",
                 pending_backlog)
        return

    pending_users = db.list_users_pending(ctx.db, need_tag=need_tag, need_mail=need_mail)
    errors = False
    if pending_users:
        log.info("old-records: рассылка уведомлений (%d пользователей, tag=%s, mail=%s)",
                 len(pending_users), need_tag, need_mail)
        for r in notify.send_old_records_notifications(
                pending_users, mm=ctx.mm, tag_enabled=need_tag, send_mail=need_mail):
            errors = errors or r["error"]
            try:
                db.set_notified_of_old_records(
                    ctx.db, r["login"],
                    tag=True if r["tag"] else None,     # ставим флаг только по РЕАЛЬНОЙ доставке
                    mail=True if r["mail"] else None)
            except Exception as ex:                     # одна ошибка БД не рвёт всю рассылку
                log.warning("old-records: не обновил флаг @%s: %s", r["login"], _err(ex))

    left = db.list_users_pending(ctx.db, need_tag=need_tag, need_mail=need_mail)
    if not left and not db.all_users_notified(ctx.db, need_tag=need_tag, need_mail=need_mail):
        return                                          # пользователей ещё нет — рано
    if left:
        since = _parse_dt(db.get_meta(ctx.db, _OLD_SINCE)) or now
        retry_in = pipeline.retry_delay(cfg.retry_delays_sec, since, now)
        if retry_in is not None:
            retry_at = now + timedelta(seconds=retry_in)
            db.set_meta(ctx.db, _OLD_SINCE, since.isoformat())
            db.set_meta(ctx.db, _OLD_NEXT_TRY, retry_at.isoformat())
            log.warning("old-records: не доставлено %d пользователям — повтор в %s",
                        len(left), retry_at.astimezone().strftime("%d.%m %H:%M"))
            return
        db.set_meta(ctx.db, _OLD_SINCE, "")             # лестница пройдена
        if errors and db.users_notified_count(ctx.db) == 0:
            _notify_admins(ctx, texts.ADMIN_OLD_RECORDS_STUCK.format(n=len(left)))
            return                                      # завтра лестница начнётся заново
        log.warning("old-records: %d пользователям так и не доставлено — продолжаем без них: %s",
                    len(left), ", ".join("@" + str(u.get("login")) for u in left[:50]))

    delete_at = now + timedelta(seconds=cfg.delete_delay_sec)
    n = db.mark_backlog_for_deletion(ctx.db, cutoff, delete_at.isoformat())
    db.set_meta(ctx.db, "old_records_notified_at", now.isoformat())
    db.set_meta(ctx.db, pipeline.OLD_RECORDS_FLAG, "1")
    log.info("old-records: рассылка завершена -> флаг выставлен; удаление %d записей бэклога "
             "назначено на %s", n, delete_at.isoformat())
    missed = ", ".join("@" + str(u.get("login")) for u in left[:20]) or "—"
    _notify_admins(ctx, texts.ADMIN_OLD_RECORDS_DONE.format(
        users=db.users_notified_count(ctx.db), missed_n=len(left), missed=missed, records=n,
        delete_at=delete_at.astimezone().strftime("%d.%m.%Y %H:%M")))


def _run_old_records_phase(ctx):
    try:
        _old_records_phase(ctx)
    except Exception as ex:
        log.warning("old-records-фаза: сбой: %s", _err(ex))


def _maybe_old_records_phase(ctx):
    """Гоняем old-records-фазу раз в сутки (по календарной дате), а если назначен повтор
    рассылки по лестнице — в его срок. В ФОНОВОМ потоке: рассылка тысячам пользователей не
    должна блокировать обработку записей в главном цикле."""
    global _last_old_records_check, _old_records_thread
    if _old_records_thread is not None and _old_records_thread.is_alive():
        return                       # предыдущая рассылка ещё идёт — ждём
    retry_at = _parse_dt(db.get_meta(ctx.db, _OLD_NEXT_TRY))
    if retry_at:
        if datetime.now(timezone.utc) < retry_at:
            return                   # ждём срока повтора
    elif _last_old_records_check == datetime.now().date():
        return
    _last_old_records_check = datetime.now().date()
    _old_records_thread = threading.Thread(target=_run_old_records_phase, args=(ctx,),
                                           name="old-records", daemon=True)
    _old_records_thread.start()


_last_mm_attempt = 0.0      # монотонное время последней попытки поднять клиент Tag


def _ensure_mm(ctx):
    """Если SEND_TO_TAG=1, а клиент Tag не поднят (был недоступен на старте демона) —
    пробуем поднять его снова, не чаще раза в 5 минут. Иначе разовый сбой Tag на старте
    отключил бы ЛС до перезапуска демона."""
    global _last_mm_attempt
    if not ctx.cfg.send_to_tag or ctx.mm is not None:
        return
    now = time.monotonic()
    if now - _last_mm_attempt < 300:
        return
    _last_mm_attempt = now
    ctx.mm = _build_mm(ctx.cfg)
    if ctx.mm is not None:
        log.info("клиент Tag поднят повторно (был недоступен ранее)")


def tick(ctx):
    """Один проход: подхват (если ASR-окно открыто) + параллельная обработка."""
    cfg = ctx.cfg
    _ensure_mm(ctx)               # переподнять клиент Tag, если он отвалился на старте
    llm_ok = bool(ctx.llm and ctx.llm.available())
    now = _local_now()
    asr_on = (not cfg.asr_window) or config.in_window(now, cfg.asr_window)
    log.info(
        "=== тик [REAL_DELETE=%d]: LLM=%s, ASR-окно=%s, LLM-окно=%s, воркеров=%d ===",
        int(cfg.real_delete),
        "вкл" if llm_ok else "выкл",
        "откр" if asr_on else "закр",
        (
            "откр"
            if ((not cfg.llm_window) or config.in_window(now, cfg.llm_window))
            else "закр"
        ),
        cfg.record_workers,
    )
    # Подхват и обработка — в РАЗДЕЛЬНЫХ try: сбой discovery (напр. Talk недоступен) не
    # должен ни ронять демон гигантским трейсбеком, ни мешать обработке уже заведённых.
    if asr_on:
        try:
            _retry_short(ctx)                # короткие записи, которые не удалось удалить сразу
        except Exception as ex:
            log.warning("повтор удаления коротких записей не удался: %s", _err(ex))
        try:
            log.info("подхвачено новых: %d", _discover(ctx))
        except Exception as ex:
            log.warning("подхват новых записей не удался (продолжаю обработку): %s", _err(ex))
    try:
        log.info("обработано записей: %d", _run_active(ctx))
    except Exception as ex:
        log.warning("цикл обработки прерван, повтор на след. проходе: %s", _err(ex))


def _build_mm(cfg):
    """MM-клиент для РЕАЛЬНОЙ рассылки — только при SEND_TO_TAG=1 и заданных URL/токене.
    Иначе None -> notify логирует (заглушка). Осторожно: при 1 участники реально получат ЛС.
    """
    if not cfg.send_to_tag:
        log.info("SEND_TO_TAG=0 -> рассылка в Tag отключена (только лог)")
        return None
    if not (cfg.mm.url and cfg.mm.bot_token):
        log.warning("SEND_TO_TAG=1, но не заданы URL/BOT_TOKEN -> рассылка отключена")
        return None
    from mm_client import MMClient

    mm = MMClient(cfg.mm, verify=cfg.mm.verify)
    try:
        mm.login()
    except Exception as ex:
        log.warning("SEND_TO_TAG=1, но Tag недоступен (%s) — рассылка отключена, "
                    "остальное работает", ex)
        return None
    log.info("рассылка в Tag ВКЛЮЧЕНА (%s)", mm.api)
    return mm


def build_ctx():
    cfg = load_cfg()
    conn = db.connect(cfg.db_path)
    ctx = Ctx(client=TalkClient(), asr=get_asr(), llm=get_llm(), cfg=cfg, db=conn)
    ctx.mm = _build_mm(cfg)
    return ctx


def main():
    ap = argparse.ArgumentParser(description="Демон архивации записей Толка")
    ap.add_argument("--once", action="store_true", help="один тик и выход")
    ap.add_argument(
        "--record", default=None, help="обработать одну запись по ключу и выйти"
    )
    args = ap.parse_args()

    logsetup.setup(LOG_DIR)
    ctx = build_ctx()
    cfg = ctx.cfg
    log.info(
        "старт демона: REAL_DELETE=%d, SEND_TO_TAG=%d, SEND_TO_MAIL=%d, out_dir=%s, db=%s, "
        "ASR-окно=%s, LLM-окно=%s, воркеров=%d, повторы стадии (от первой ошибки) через %s, "
        "сводка админам в %02d:%02d",
        int(cfg.real_delete), int(cfg.send_to_tag), int(cfg.send_to_mail),
        cfg.out_dir,
        cfg.db_path,
        cfg.asr_window,
        cfg.llm_window,
        cfg.record_workers,
        ", ".join(pipeline._fmt_delay(d) for d in cfg.retry_delays_sec),
        *cfg.admin_report_time,
    )

    if args.record:
        meta = ctx.client.recording_detail(args.record)
        _enroll(ctx, args.record, meta)
        pipeline.process(ctx, args.record)
        return

    if not args.once:                      # суточная сводка админам — своим потоком
        threading.Thread(target=_report_loop, args=(ctx,), name="admin-report", daemon=True).start()

    idle = False                           # уже сообщили в лог, что ждём открытия окна
    while True:
        # вне всех рабочих окон — записи не обрабатываем (--once игнорирует окна). Но спим
        # не одним куском до открытия, а по 5 минут: служебные дела (повторы единоразовой
        # рассылки по лестнице) должны идти в срок и вне окон.
        if not _any_open(cfg, _local_now()) and not args.once:
            s = _sleep_secs(cfg)
            if not idle:
                log.info("вне рабочих окон — жду открытия (%.0f с)", s)
                idle = True
            try:
                _ensure_mm(ctx)            # рассылке нужен клиент Tag, даже если на старте он не поднялся
                _service(ctx, every=60)
            except Exception as ex:        # служебные дела не должны ронять главный цикл
                log.warning("служебный проход вне окон не удался: %s", _err(ex))
            time.sleep(min(s, 300.0))
            continue
        idle = False
        try:
            tick(ctx)
            _service(ctx)                  # фаза old-records
        except Exception as ex:
            log.exception("сбой тика: %s", ex)
        if args.once:
            break
        s = _sleep_secs(cfg)
        log.info("сон %.0f с до следующего прохода…", s)
        time.sleep(s)


if __name__ == "__main__":
    main()
