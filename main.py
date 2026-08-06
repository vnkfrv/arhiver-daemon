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
  3. Спим до ближайшей смены фазы/открытия окна.

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
from pipeline import Ctx
from config import load_cfg
from talk_api import TalkClient
from asr import get_asr
from llm import get_llm

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

log = logging.getLogger("daemon")


def _parse_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


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


def _seconds_until(hh, mm):
    """Секунд до ближайшего наступления времени HH:MM (сегодня или завтра)."""
    now = _local_now()
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


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
    """Сколько спать до следующей проверки."""
    now = _local_now()
    if _windows(cfg):
        if _any_open(cfg, now):
            b = _next_boundary_secs(cfg)  # спим до смены фазы, но не дольше 5 мин
            return min(b, 300.0) if b else 300.0
        # все окна закрыты — спим до ближайшего открытия
        opens = [config.next_open_local(now, w) for w in _windows(cfg)]
        opens = [o for o in opens if o]
        return max(1.0, (min(opens) - now).total_seconds()) if opens else 300.0
    # окон нет: test — короткий цикл, prod — раз в сутки в RUN_AT
    return cfg.cycle_sec if cfg.mode == "test" else _seconds_until(*cfg.run_at)


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


def _handle_short(ctx, key, meta, dur):
    """Слишком короткая запись: не транскрибируем, УДАЛЯЕМ (prod — реально, test —
    заглушка в лог) и ставим «надгробие» в БД (done=1), чтобы не наткнуться повторно
    и чтобы водяной знак прошёл дальше."""
    cfg = ctx.cfg
    cd = meta.get("createdDate")
    if cfg.simulate_delete:                     # test — только лог
        log.info("короткая запись %s (%sс < %d) — удаление ИМИТАЦИЯ (test)",
                 key, dur, cfg.min_duration)
        extra = {"delete_simulated": 1}
    else:                                       # prod — реально удаляем из Толка
        try:
            ctx.client.delete_recording(key, force=cfg.force_delete)
            log.info("короткая запись %s (%sс < %d) — удалена из Толка", key, dur, cfg.min_duration)
            extra = {"deleted": 1, "deleted_at": _now_iso()}
        except Exception as ex:
            log.warning("короткая запись %s (%sс < %d): удалить не удалось: %s",
                        key, dur, cfg.min_duration, ex)
            extra = {}                          # видео осталось, но запись помечаем терминально
    db.enroll(ctx.db, key, created_date=cd, rec_dir=store.rec_dir(cfg.out_dir, key, cd),
              title=meta.get("title"), duration_sec=dur, can_be_removed=bool(meta.get("canBeRemoved")))
    db.update(ctx.db, key, status=statuses.TOO_SHORT, done=1, next_due_at=None, **extra)


def _discover(ctx):
    """Подхватить ВСЕ записи вплоть до конца прошлого дня (от старых к новым) — одинаково
    для test и prod. Водяной знак — MAX(created_date) из БД (без state-файла).
    Короче MIN_DURATION -> сразу удаляем (не транскрибируем)."""
    cfg = ctx.cfg
    cutoff = _start_of_today_utc()
    since = db.max_created(ctx.db)
    discovered = shortdel = 0
    for meta in ctx.client.list(since=since, order="byTimeOldFirst"):
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
        with lock:
            inflight.discard(key)
        try:
            row = fut.result()
            if row is None:
                return
            with lock:
                stats["worked"] += 1
            still = pipeline.stages_pending(
                row, cfg, llm_ok, bool(row.get("protected"))
            )
            log.info(
                "✓ %s | %s%s",
                key,
                row.get("status"),
                (" | осталось: " + ",".join(still)) if still else "",
            )
        except Exception as ex:
            log.exception("✗ ошибка обработки %s: %s", key, ex)
            try:  # анти-тайтлуп: отодвинуть next_due, чтобы упавшая запись не крутилась
                db.update(
                    ctx.db,
                    key,
                    next_due_at=(
                        datetime.now(timezone.utc)
                        + timedelta(seconds=pipeline._RETRY_BACKOFF_SEC)
                    ).isoformat(),
                )
            except Exception:
                pass

    with ThreadPoolExecutor(
        max_workers=cfg.record_workers, thread_name_prefix="rec"
    ) as pool:
        while True:
            now = _local_now()
            da = config.in_window(now, cfg.asr_window)  # True если окна нет
            dl = config.in_window(now, cfg.llm_window)
            if not (da or dl):
                break  # все окна закрыты — новые не стартуем
            with lock:
                free = cfg.record_workers - len(inflight)
                busy = len(inflight)
            if free <= 0:
                time.sleep(0.2)  # все воркеры заняты — ждём слот
                continue
            # берём записи ТЕКУЩЕЙ фазы (фильтр в SQL — иначе старые llm-записи вытеснят
            # свежие asr из-под limit и транскрибация «голодает»); с запасом на inflight.
            phase = None if (da and dl) else ("asr" if da else "llm")
            rows = db.due_records(
                ctx.db, _now_iso(), limit=free + busy + cfg.record_workers, phase=phase
            )
            launched = 0
            for r in rows:
                if launched >= free:
                    break
                key = r["recording_key"]
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


# --- EMERGENCY: если обработка «залипла» дольше EMERGENCY часов -> алерт админам ---
_alert_mm = None            # ленивый MM-клиент только для алертов (даже при SEND_TO_TAG=0)
_last_emergency = None      # монотонное время последнего алерта (анти-спам)


def _notify_admins(ctx, text):
    """Написать всем админам в Tag (+ всегда в лог ERROR). MM-клиент строим лениво —
    отдельно от рассылки участникам, чтобы алерт уходил даже при SEND_TO_TAG=0."""
    log.error(text)
    cfg = ctx.cfg
    if not (cfg.mm.url and cfg.mm.bot_token):
        return
    admins = db.list_admins(ctx.db)
    if not admins:
        log.error("EMERGENCY: в БД нет админов — некому слать алерт (задай ADMIN_USERNAME)")
        return
    global _alert_mm
    try:
        if _alert_mm is None:
            from mm_client import MMClient
            _alert_mm = MMClient(cfg.mm, verify=cfg.mm.verify)
            _alert_mm.login()
        for u in admins:
            try:
                _alert_mm.send_dm(u, text)
            except Exception as ex:
                log.warning("EMERGENCY: не смог написать @%s: %s", u, ex)
    except Exception as ex:
        log.error("EMERGENCY: не удалось отправить алерт в Tag: %s", ex)


def _check_emergency(ctx):
    """Если самая ранняя «залипшая» запись висит дольше EMERGENCY часов — алерт админам
    (не чаще одного раза за EMERGENCY-окно)."""
    global _last_emergency
    oldest = db.oldest_error(ctx.db)
    if not oldest:
        _last_emergency = None                          # всё ок — сбрасываем анти-спам
        return
    dt = _parse_dt(oldest)
    if dt is None:
        return
    stuck_h = (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    if stuck_h < ctx.cfg.emergency_hours:
        return
    now = time.monotonic()
    if _last_emergency is not None and now - _last_emergency < ctx.cfg.emergency_hours * 3600:
        return                                          # уже недавно предупреждали
    _last_emergency = now
    _notify_admins(ctx, "⚠️ Демон-архиватор: обработка застряла ≥ %dч (с %s). "
                        "Вероятно недоступен шлюз ASR/LLM (проверь VPN/шлюз) или Толк."
                        % (ctx.cfg.emergency_hours, str(oldest)[:19]))


def tick(ctx):
    """Один проход: подхват (если ASR-окно открыто) + параллельная обработка."""
    cfg = ctx.cfg
    llm_ok = bool(ctx.llm and ctx.llm.available())
    now = _local_now()
    asr_on = (not cfg.asr_window) or config.in_window(now, cfg.asr_window)
    log.info(
        "=== тик [MODE=%s]: LLM=%s, ASR-окно=%s, LLM-окно=%s, воркеров=%d ===",
        cfg.mode,
        "вкл" if llm_ok else "выкл",
        "откр" if asr_on else "закр",
        (
            "откр"
            if ((not cfg.llm_window) or config.in_window(now, cfg.llm_window))
            else "закр"
        ),
        cfg.record_workers,
    )
    if asr_on:
        log.info(
            "подхвачено новых: %d", _discover(ctx)
        )  # новые записи для транскрибации
    log.info("обработано записей: %d", _run_active(ctx))


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
        "старт демона: MODE=%s, out_dir=%s, db=%s, ASR-окно=%s, LLM-окно=%s, воркеров=%d",
        cfg.mode,
        cfg.out_dir,
        cfg.db_path,
        cfg.asr_window,
        cfg.llm_window,
        cfg.record_workers,
    )

    if args.record:
        meta = ctx.client.recording_detail(args.record)
        _enroll(ctx, args.record, meta)
        pipeline.process(ctx, args.record)
        return

    while True:
        # вне всех рабочих окон — не обрабатываем, спим до открытия (--once игнорирует окна)
        if not _any_open(cfg, _local_now()) and not args.once:
            s = _sleep_secs(cfg)
            log.info("вне рабочих окон — сон %.0f с до открытия", s)
            time.sleep(s)
            continue
        try:
            tick(ctx)
            _check_emergency(ctx)          # долгая недоступность шлюза -> алерт админам
        except Exception as ex:
            log.exception("сбой тика: %s", ex)
        if args.once:
            break
        s = _sleep_secs(cfg)
        log.info("сон %.0f с до следующего прохода…", s)
        time.sleep(s)


if __name__ == "__main__":
    main()
