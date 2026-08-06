"""
db.py — SQLite: ЕДИНЫЙ ИСТОЧНИК ПРАВДЫ по записям (витрина + планировщик).

Таблицы:
  recordings   — по строке на запись: поля ТЗ (id/название/дата/участники/длительность/
                 статус) + флаги стадий пайплайна + планирование (done/next_due_at) +
                 путь к папке файлов (rec_dir, шардированный по дате).
  participant  — нормализованные участники (email индексирован) для быстрого поиска
                 записей пользователя без полного скана recordings.
  deleted_reply— ветки, куда бот уже ответил «видео уже удалено» (анти-дубль).
  admins       — администраторы бота.

ПЛАНИРОВАНИЕ. Демон не обходит файловую систему: он берёт из БД только записи,
которым пора работать — `due_records()` (WHERE done=0 AND next_due_at<=now, по индексу).
После обработки записи пайплайн проставляет done/next_due_at: либо «сделать нечего»
(done=1), либо «следующее действие в момент T» (next_due_at=T — постаревшее для удаления
видео, срок удаления, срок очистки текстов). Внешние действия бота (protected/ручное
удаление) вызывают wake() -> запись снова попадёт в выборку.

В папке записи (rec_dir) лежат только выходные файлы (<key>.json, <key>_full.md,
<key>_short.md) — состояние пайплайна целиком в БД.
"""

import json
import os
import sqlite3
import threading
from datetime import datetime, timezone

import statuses

# Одно соединение может использоваться из нескольких потоков (Flask-хендлеры бота +
# фоновые потоки доставки). check_same_thread=False + сериализация локом.
_LOCK = threading.RLock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS recordings (
    recording_key       TEXT PRIMARY KEY,
    title               TEXT,
    created_date        TEXT,
    duration_sec        INTEGER,
    participants        TEXT,          -- JSON: [{"name":..., "email":...}]
    status              TEXT,          -- человекочитаемый статус (statuses.py)
    rec_dir             TEXT,          -- папка файлов (шард по дате: base/YYYY/MM/<key>)
    full_path           TEXT,
    short_path          TEXT,
    -- флаги стадий пайплайна
    transcribed         INTEGER DEFAULT 0,
    improved            INTEGER DEFAULT 0,
    summarized          INTEGER DEFAULT 0,
    saved_to_db         INTEGER DEFAULT 0,
    messages_sent       INTEGER DEFAULT 0,
    deleted             INTEGER DEFAULT 0,
    text_cleaned        INTEGER DEFAULT 0,
    no_audio            INTEGER DEFAULT 0,
    delete_simulated    INTEGER DEFAULT 0,
    protected           INTEGER DEFAULT 0,   -- админ: «хранить в Толке навсегда»
    can_be_removed      INTEGER DEFAULT 0,
    -- расписание/тайминги
    delete_scheduled_at TEXT,
    deleted_at          TEXT,
    text_delete_at      TEXT,
    notified_usernames  TEXT,          -- JSON: кому уже отправили ЛС (анти-дубль)
    last_error          TEXT,
    error_since         TEXT,          -- когда стадия впервые «залипла» (для EMERGENCY-алерта)
    done                INTEGER DEFAULT 0,   -- 1 = делать больше нечего
    next_due_at         TEXT,          -- когда запись снова требует внимания (ISO)
    updated_at          TEXT
);
CREATE INDEX IF NOT EXISTS idx_recordings_sched ON recordings(done, next_due_at);

CREATE TABLE IF NOT EXISTS participant (
    recording_key TEXT NOT NULL,
    email         TEXT NOT NULL,
    name          TEXT,
    PRIMARY KEY (recording_key, email)
);
CREATE INDEX IF NOT EXISTS idx_participant_email ON participant(email);

CREATE TABLE IF NOT EXISTS deleted_reply (
    root_id    TEXT PRIMARY KEY,
    replied_at TEXT
);

CREATE TABLE IF NOT EXISTS admins (
    username  TEXT PRIMARY KEY,
    added_by  TEXT,
    added_at  TEXT
);
"""

# Колонки, которые разрешено менять через update() (защита от произвольного SQL).
_UPDATABLE = {
    "title", "created_date", "duration_sec", "participants", "status", "rec_dir",
    "full_path", "short_path", "transcribed", "improved", "summarized", "saved_to_db",
    "messages_sent", "deleted", "text_cleaned", "no_audio", "delete_simulated",
    "protected", "can_be_removed", "delete_scheduled_at", "deleted_at", "text_delete_at",
    "notified_usernames", "last_error", "error_since", "done", "next_due_at",
}
_JSON_COLS = {"participants", "notified_usernames"}


def _now():
    return datetime.now(timezone.utc).isoformat()


def _locked(fn):
    """Сериализовать доступ к БД между потоками (см. _LOCK выше)."""
    import functools

    @functools.wraps(fn)
    def wrap(*a, **kw):
        with _LOCK:
            return fn(*a, **kw)
    return wrap


def connect(path):
    """Открыть (создав при необходимости) БД и гарантировать схему."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # демон и бот пишут в один файл из разных процессов
    conn.execute("PRAGMA busy_timeout=5000")  # ждать до 5с на блокировке
    conn.executescript(_SCHEMA)
    # мягкая миграция: добить новые колонки в существующую БД (не пересоздавая её)
    have = {r[1] for r in conn.execute("PRAGMA table_info(recordings)")}
    if "error_since" not in have:
        conn.execute("ALTER TABLE recordings ADD COLUMN error_since TEXT")
    conn.commit()
    return conn


# ---- запись/чтение строки recordings --------------------------------------

@_locked
def enroll(conn, key, *, created_date, rec_dir, title=None, duration_sec=None,
           can_be_removed=None):
    """Завести запись (idempotent): создаётся строка со статусом «до преобразования»,
    done=0, next_due_at=NULL (обработать при ближайшем тике). Файлы появятся позже."""
    conn.execute(
        "INSERT OR IGNORE INTO recordings (recording_key, created_date, rec_dir, title, "
        "duration_sec, can_be_removed, status, done, next_due_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,0,NULL,?)",
        (key, created_date, rec_dir, title, duration_sec,
         1 if can_be_removed else 0, statuses.BEFORE, _now()))
    conn.commit()


@_locked
def update(conn, key, **fields):
    """
    Обновить разрешённые колонки строки. bool -> 0/1; для participants/notified_usernames
    список сериализуется в JSON. При обновлении participants синхронизируется таблица
    participant (для индексного поиска).
    """
    parts_for_index = None
    sets, vals = [], []
    for k, v in fields.items():
        if k not in _UPDATABLE:
            raise ValueError(f"db.update: недопустимая колонка {k!r}")
        if k == "participants" and v is not None:
            parts_for_index = v
        if k in _JSON_COLS and v is not None and not isinstance(v, str):
            v = json.dumps(v, ensure_ascii=False)
        if isinstance(v, bool):
            v = int(v)
        sets.append(f"{k}=?")
        vals.append(v)
    sets.append("updated_at=?")
    vals.append(_now())
    conn.execute(f"UPDATE recordings SET {','.join(sets)} WHERE recording_key=?",
                 vals + [key])
    if parts_for_index is not None:
        conn.execute("DELETE FROM participant WHERE recording_key=?", (key,))
        conn.executemany(
            "INSERT OR IGNORE INTO participant (recording_key, email, name) VALUES (?,?,?)",
            [(key, p.get("email"), p.get("name")) for p in parts_for_index if p.get("email")])
    conn.commit()


@_locked
def wake(conn, key):
    """Пометить запись как требующую внимания (done=0, next_due_at=NULL). Зовут бот и
    админ-команды из ДРУГОГО процесса, чтобы демон переоценил запись на след. тике."""
    conn.execute("UPDATE recordings SET done=0, next_due_at=NULL, updated_at=? "
                 "WHERE recording_key=?", (_now(), key))
    conn.commit()


def _row_to_dict(row):
    if row is None:
        return None
    d = dict(row)
    for col in _JSON_COLS:
        d[col] = json.loads(d[col]) if d.get(col) else ([] if col == "notified_usernames" else [])
    return d


@_locked
def get(conn, key):
    row = conn.execute("SELECT * FROM recordings WHERE recording_key=?", (key,)).fetchone()
    return _row_to_dict(row)


@_locked
def exists(conn, key):
    return conn.execute("SELECT 1 FROM recordings WHERE recording_key=?",
                        (key,)).fetchone() is not None


@_locked
def max_created(conn):
    """Максимальная created_date среди заведённых записей — «водяной знак» подхвата
    (ISO8601 сортируется лексикографически). None, если БД пуста."""
    r = conn.execute("SELECT MAX(created_date) FROM recordings").fetchone()
    return r[0] if r and r[0] else None


@_locked
def due_records(conn, now_iso, limit=None, phase=None):
    """Записи, которым ПОРА работать (done=0 и срок наступил), от старых к новым — полными
    строками. Идёт по индексу (done, next_due_at) без скана файлов. limit — размер батча.
    phase фильтрует по фазе, чтобы в ASR-окне старые записи llm-фазы не вытесняли свежие
    из батча (иначе транскрибация «голодает»): 'asr' — не транскрибированные; 'llm' —
    транскрибированные; None — любые (когда открыты оба окна)."""
    where = "done=0 AND (next_due_at IS NULL OR next_due_at<=?)"
    if phase == "asr":
        where += " AND transcribed=0 AND no_audio=0"
    elif phase == "llm":
        where += " AND transcribed=1"
    sql = f"SELECT * FROM recordings WHERE {where} ORDER BY created_date"
    params = [now_iso]
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))
    return [_row_to_dict(r) for r in conn.execute(sql, params).fetchall()]


@_locked
def list_for_email(conn, email, limit=None):
    """Записи, где участвовал пользователь (по ИНДЕКСУ participant(email), без скана
    recordings), от старых к новым. limit ограничивает размер выборки для модалки."""
    sql = ("SELECT r.* FROM recordings r "
           "JOIN participant p ON p.recording_key = r.recording_key "
           "WHERE p.email = ? ORDER BY r.created_date")
    params = [email]
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))
    return [_row_to_dict(r) for r in conn.execute(sql, params).fetchall()]


# ---- анти-дубль ответа «видео уже удалено» --------------------------------

@_locked
def already_replied_deleted(conn, root_id):
    return conn.execute("SELECT 1 FROM deleted_reply WHERE root_id=?",
                        (root_id,)).fetchone() is not None


@_locked
def mark_replied_deleted(conn, root_id):
    conn.execute("INSERT OR IGNORE INTO deleted_reply (root_id, replied_at) VALUES (?,?)",
                 (root_id, _now()))
    conn.commit()


# ---- админы ----------------------------------------------------------------

@_locked
def add_admin(conn, username, added_by):
    conn.execute("INSERT OR IGNORE INTO admins (username, added_by, added_at) VALUES (?,?,?)",
                 (username, added_by, _now()))
    conn.commit()


@_locked
def is_admin(conn, username):
    return conn.execute("SELECT 1 FROM admins WHERE username=?",
                        (username,)).fetchone() is not None


@_locked
def list_admins(conn):
    return [r[0] for r in conn.execute("SELECT username FROM admins ORDER BY added_at").fetchall()]


@_locked
def oldest_error(conn):
    """Самая ранняя метка «залипания» среди НЕзавершённых записей (или None). По ней
    демон понимает, что что-то недоступно уже долго (для EMERGENCY-алерта)."""
    r = conn.execute("SELECT MIN(error_since) FROM recordings "
                     "WHERE error_since IS NOT NULL AND done=0").fetchone()
    return r[0] if r and r[0] else None
