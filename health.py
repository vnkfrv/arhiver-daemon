#!/usr/bin/env python3
"""
health.py — быстрая сводка состояния архиватора (оценить «всё ли идёт корректно»).

ТОЛЬКО ЧТЕНИЕ БД: скрипт лишь выполняет SELECT'ы и ничего не меняет, поэтому его можно
запускать в ЛЮБОЙ момент, пока демон и бот работают (SQLite в режиме WAL отдаёт данные
читателю параллельно с записью).

Запуск:
    python health.py
в контейнере:
    docker compose exec talk-daemon python health.py
"""

import os
import sqlite3
from datetime import datetime, timedelta, timezone

import statuses

HERE = os.path.dirname(os.path.abspath(__file__))


def _db_path():
    return os.environ.get("DB_PATH") or os.path.join(HERE, "data", "recordings.db")


def _open_reader(path):
    """Открыть БД как ЧИТАТЕЛЬ (обычное подключение, но выполняем только SELECT). Не
    трогаем схему/миграции — не мешаем работающему демону."""
    if not os.path.exists(path):
        raise SystemExit(f"[!] БД не найдена: {path}\n    Проверь DB_PATH или путь к data/.")
    conn = sqlite3.connect(path, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _one(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()


def _hours_since(iso):
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    except (ValueError, TypeError):
        return None


def main():
    path = _db_path()
    conn = _open_reader(path)
    print(f"БД: {path}")
    print(f"снимок: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 52)

    # --- записи по состояниям ---
    r = _one(conn, """
        SELECT
          COUNT(*)                                           total,
          COALESCE(SUM(saved_to_db=1), 0)                    saved,
          COALESCE(SUM(transcribed=1), 0)                    transcribed,
          COALESCE(SUM(status=?), 0)                         asr_failed,
          COALESCE(SUM(no_audio=1), 0)                       no_audio,
          COALESCE(SUM(deleted=1), 0)                        deleted,
          COALESCE(SUM(delete_simulated=1 AND deleted=0), 0) delete_sim,
          COALESCE(SUM(protected=1), 0)                      protected,
          COALESCE(SUM(text_cleaned=1), 0)                   text_cleaned,
          COALESCE(SUM(messages_sent=1), 0)                  messaged,
          COALESCE(SUM(saved_to_db=0 AND no_audio=0 AND done=0 AND status!=?), 0) pending
        FROM recordings
    """, (statuses.ASR_FAILED, statuses.ASR_FAILED))

    total = r["total"]
    print("ЗАПИСИ")
    print(f"  всего:                 {total}")
    print(f"  готово (в архиве):     {r['saved']}")
    print(f"  транскрибировано:      {r['transcribed']}")
    print(f"  ждут обработки:        {r['pending']}")
    print(f"  уведомлены участники:  {r['messaged']}")
    print(f"  битый звук (no_audio): {r['no_audio']}")
    print(f"  ASR_FAILED (лимит):    {r['asr_failed']}")
    print(f"  защищено (remember):   {r['protected']}")
    print(f"  удалено видео:         {r['deleted']}   (имитация: {r['delete_sim']})")
    print(f"  тексты очищены (5 лет):{r['text_cleaned']}")
    if total:
        bad = r["no_audio"] + r["asr_failed"]
        print(f"  доля непригодных:      {100 * bad / total:.1f}%  (битый звук + ASR_FAILED)")

    # --- фаза old-records (может не быть в старой БД) ---
    print("-" * 52)
    print("OLD-RECORDS ФАЗА")
    try:
        u = _one(conn, "SELECT COUNT(*) t, COALESCE(SUM(notified_of_old_records_tag=1),0) tag, "
                       "COALESCE(SUM(notified_of_old_records_mail=1),0) mail FROM users")
        flag = _one(conn, "SELECT value FROM meta WHERE key='old_records_all_notified'")
        at = _one(conn, "SELECT value FROM meta WHERE key='old_records_notified_at'")
        print(f"  пользователей в БД:    {u['t']}")
        print(f"  уведомлены (Tag):      {u['tag']}")
        print(f"  уведомлены (почта):    {u['mail']}")
        print(f"  флаг «все уведомлены»: {(flag['value'] if flag else '0')}"
              + (f"  (с {at['value'][:19]})" if at and at['value'] else ""))
    except sqlite3.OperationalError:
        print("  (таблицы users/meta ещё нет — демон не запускался на этой БД)")

    # --- залипшие ошибки (риск EMERGENCY) ---
    print("-" * 52)
    print("ОШИБКИ / ЗАЛИПАНИЕ")
    oe = _one(conn, "SELECT MIN(error_since) m, COUNT(*) c FROM recordings "
                    "WHERE error_since IS NOT NULL AND done=0")
    if oe and oe["m"]:
        h = _hours_since(oe["m"])
        htxt = f"≈{h:.1f} ч назад" if h is not None else "?"
        print(f"  записей с «залипшей» ошибкой: {oe['c']}")
        print(f"  самая старая:                {str(oe['m'])[:19]} ({htxt})")
    else:
        print("  залипших ошибок нет ✅")

    # --- активность (liveness): что-то обновлялось недавно ---
    # updated_at пишется нашим кодом в isoformat (+00:00) — сравнение однородное
    cut_iso = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    act = _one(conn, "SELECT COUNT(*) c FROM recordings WHERE updated_at >= ?", (cut_iso,))
    print(f"  обновлено записей за последний час: {act['c']}")

    conn.close()


if __name__ == "__main__":
    main()
