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
  admin_requests — решения обычных админов о составе админов, ждущие периода охлаждения.
  admin_log    — журнал действий админов (кто что удалил/защитил/перезапустил).
  users, meta  — пользователи для old-records-уведомления и служебные флаги фаз.

ПЛАНИРОВАНИЕ. Демон не обходит файловую систему: он берёт из БД только записи,
которым пора работать — `due_records()` (WHERE done=0 AND next_due_at<=now, по индексу).
После обработки записи пайплайн проставляет done/next_due_at: либо «сделать нечего»
(done=1), либо «следующее действие в момент T» (next_due_at=T — постаревшее для удаления
видео, срок удаления, срок очистки текстов). Внешние действия бота (protected/ручное
удаление) вызывают wake() -> запись снова попадёт в выборку.

В папке записи (rec_dir) лежат только выходные файлы (<key>_ASR.md, <key>_LLM.md,
<key>_short.md) — состояние пайплайна целиком в БД.
"""

import functools
import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import statuses

log = logging.getLogger("db")

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
    status              TEXT,          -- код статуса: NEW, SAVED, FAILED_LLM… (statuses.py)
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
    no_channels         INTEGER DEFAULT 0,   -- нет дорожек по спикерам: распознана сведённая
    delete_simulated    INTEGER DEFAULT 0,
    protected           INTEGER DEFAULT 0,   -- админ: «хранить в Толке навсегда»
    protected_by        TEXT,                -- кто из админов запретил удаление
    kept_forever        INTEGER DEFAULT 0,   -- Толк запрещает удалять (DELETE_PROTECTION)
    can_be_removed      INTEGER DEFAULT 0,
    -- расписание/тайминги
    delete_scheduled_at TEXT,
    deleted_at          TEXT,
    deleted_by          TEXT,          -- кто удалил видео: '@ник' админа | auto | short | external
    text_delete_at      TEXT,
    notified_usernames  TEXT,          -- JSON: кому уже отправили ЛС в Tag (анти-дубль)
    notified_mail       TEXT,          -- JSON: кому уже ушло письмо (анти-дубль)
    unreachable         TEXT,          -- JSON: кого так и не удалось уведомить об удалении
    last_error          TEXT,
    -- лестница повторов упавшей стадии (см. pipeline)
    error_since         TEXT,                -- первая ошибка стадии: от неё считаются повторы
    fail_stage          TEXT,                -- какая стадия падает (transcribe/improve/…)
    fail_count          INTEGER DEFAULT 0,   -- сколько раз подряд она упала
    needs_admin         INTEGER DEFAULT 0,   -- повторы исчерпаны: ждёт админа (FAILED_…)
    hold_notified       INTEGER DEFAULT 0,   -- запись уже вошла в суточную сводку админам
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

-- Журнал действий админов: удаление видео, запрет/разрешение удаления, перезапуск
-- остановленных записей, выдача прав, скачивание чужих расшифровок.
CREATE TABLE IF NOT EXISTS admin_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    at            TEXT,
    username      TEXT,
    action        TEXT,
    recording_key TEXT,
    details       TEXT
);

-- Решения об изменении состава админов, поданные обычными админами: вступают в силу не
-- сразу, а в effective_at (период охлаждения); до этого суперадмин может заблокировать,
-- а подавший — отозвать. Суперадмин (meta.superadmin) меняет состав сразу, мимо этой таблицы.
CREATE TABLE IF NOT EXISTS admin_requests (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL,                     -- add | remove
    target       TEXT NOT NULL,                     -- кого добавляют/убирают
    requested_by TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    effective_at TEXT NOT NULL,                     -- когда решение вступит в силу
    status       TEXT NOT NULL DEFAULT 'pending',   -- pending | applied | blocked | withdrawn | cancelled
    resolved_by  TEXT,
    resolved_at  TEXT,
    note         TEXT
);

-- Пользователи (участники конференций) как отдельная сущность (не привязка к записи).
--   id    — локальный автоинкремент; login — идентификатор из Толка (username = тело почты до @).
--   notified_of_old_records_tag / _mail — булевы флаги (0/1): уведомлён ли пользователь
--   о старых записях по каждому каналу (раздельно, т.к. каналы независимы и у почты лимиты).
CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    login       TEXT UNIQUE,
    email       TEXT UNIQUE,
    first_name  TEXT,
    last_name   TEXT,
    notified_of_old_records_tag  INTEGER NOT NULL DEFAULT 0,
    notified_of_old_records_mail INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT,
    updated_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- Служебное key-value состояние демона (флаги фаз и т.п.).
--   old_records_all_notified — «все пользователи уведомлены о старых записях» (0/1).
--   old_records_notified_at  — ISO-момент, когда флаг стал 1 (старт grace-периода удаления).
--   superadmin               — ник суперадмина (может передать права другому админу).
--   superadmin_env           — значение ADMIN_USERNAME, уже учтённое ботом (см. admin.py).
CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT
);
"""

# Колонки, которые разрешено менять через update() (защита от произвольного SQL).
_UPDATABLE = {
    "title", "created_date", "duration_sec", "participants", "status", "rec_dir",
    "full_path", "short_path", "transcribed", "improved", "summarized", "saved_to_db",
    "messages_sent", "deleted", "text_cleaned", "no_audio", "no_channels", "delete_simulated",
    "protected", "protected_by", "kept_forever", "can_be_removed", "delete_scheduled_at",
    "deleted_at", "deleted_by", "text_delete_at", "notified_usernames", "notified_mail",
    "unreachable", "last_error", "error_since", "fail_stage", "fail_count", "needs_admin",
    "hold_notified", "done", "next_due_at",
}
_JSON_COLS = {"participants", "notified_usernames", "notified_mail", "unreachable"}

# Колонки, появившиеся после первой версии схемы: добиваются в существующую БД через ALTER.
_ADDED_COLUMNS = (
    ("error_since", "TEXT"),
    ("no_channels", "INTEGER DEFAULT 0"),
    ("protected_by", "TEXT"),
    ("kept_forever", "INTEGER DEFAULT 0"),
    ("deleted_by", "TEXT"),
    ("notified_mail", "TEXT"),
    ("unreachable", "TEXT"),
    ("fail_stage", "TEXT"),
    ("fail_count", "INTEGER DEFAULT 0"),
    ("needs_admin", "INTEGER DEFAULT 0"),
    ("hold_notified", "INTEGER DEFAULT 0"),
)

# Видео ждёт удаления по графику (участники уведомлены, срок назначен, запретов нет).
_PENDING_DELETE = ("messages_sent=1 AND deleted=0 AND delete_simulated=0 AND protected=0 "
                   "AND kept_forever=0 AND needs_admin=0 AND delete_scheduled_at IS NOT NULL")


def _now():
    return datetime.now(timezone.utc).isoformat()


def _ulower(s):
    return s.lower() if isinstance(s, str) else s


def _like(text):
    """Шаблон LIKE «содержит text» в нижнем регистре (в паре с ulower() и ESCAPE '\\')."""
    t = str(text).lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{t}%"


def _norm_email(email):
    """Почта для сравнения: без пробелов по краям и строчными. Tag хранит почты в нижнем
    регистре, а Толк может отдать «Ivan.Petrov@Corp.ru» — это один и тот же человек."""
    return (email or "").strip().lower()


def _locked(fn):
    """Сериализовать доступ к БД между потоками (см. _LOCK выше).

    Если операция упала — ОТКАТЫВАЕМ транзакцию. sqlite3 сам этого не делает: после сбоя
    записи (например, «database is locked» по таймауту, пока пишет соседний процесс)
    транзакция остаётся открытой на общем соединении. Тогда оно держит блокировку записи
    для соседа либо «закрепляет» старый снимок данных — и все следующие записи этого
    процесса падают до перезапуска. Откат возвращает соединение в чистое состояние."""
    @functools.wraps(fn)
    def wrap(conn, *a, **kw):
        with _LOCK:
            try:
                return fn(conn, *a, **kw)
            except BaseException:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
                raise
    return wrap


def connect(path):
    """Открыть (создав при необходимости) БД и гарантировать схему."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # LIKE/lower() в SQLite не знают регистра кириллицы — для поиска по названиям своя функция
    conn.create_function("ulower", 1, _ulower)
    conn.execute("PRAGMA journal_mode=WAL")   # демон и бот пишут в один файл из разных процессов
    conn.execute("PRAGMA busy_timeout=5000")  # ждать до 5с на блокировке
    conn.executescript(_SCHEMA)
    # мягкая миграция: добить новые колонки в существующую БД (не пересоздавая её)
    have = {r[1] for r in conn.execute("PRAGMA table_info(recordings)")}
    for col, decl in _ADDED_COLUMNS:
        if col in have:
            continue
        try:
            conn.execute(f"ALTER TABLE recordings ADD COLUMN {col} {decl}")
        except sqlite3.OperationalError as ex:
            # демон и бот стартуют вместе и мигрируют одну БД: колонку мог только что
            # добавить соседний процесс — это не ошибка
            if "duplicate column" not in str(ex).lower():
                raise
    conn.commit()
    return conn


# ---- запись/чтение строки recordings --------------------------------------

@_locked
def enroll(conn, key, *, created_date, rec_dir, title=None, duration_sec=None,
           can_be_removed=None):
    """Завести запись (idempotent): создаётся строка со статусом NEW («до преобразования»),
    done=0, next_due_at=NULL (обработать при ближайшем тике). Файлы появятся позже."""
    conn.execute(
        "INSERT OR IGNORE INTO recordings (recording_key, created_date, rec_dir, title, "
        "duration_sec, can_be_removed, status, done, next_due_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,0,NULL,?)",
        (key, created_date, rec_dir, title, duration_sec,
         1 if can_be_removed else 0, statuses.NEW, _now()))
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
        # в индексе почта нормализована (см. _norm_email): ищем записи пользователя по ней
        conn.execute("DELETE FROM participant WHERE recording_key=?", (key,))
        conn.executemany(
            "INSERT OR IGNORE INTO participant (recording_key, email, name) VALUES (?,?,?)",
            [(key, _norm_email(p.get("email")), p.get("name"))
             for p in parts_for_index if _norm_email(p.get("email"))])
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
        d[col] = json.loads(d[col]) if d.get(col) else []
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


def _for_email_where(email, query):
    where, params = "p.email = ?", [_norm_email(email)]
    if query:                                   # часть названия, без учёта регистра
        where += " AND ulower(r.title) LIKE ? ESCAPE '\\'"
        params.append(_like(query))
    return where, params


@_locked
def records_for_email(conn, email, limit=None, query=None):
    """Записи, где участвовал пользователь (по ИНДЕКСУ participant(email), без скана
    recordings), от НОВЫХ к старым — свежие нужны чаще. Для выпадающих списков: только
    ключ, название, дата и длительность. limit — не больше стольких записей; query —
    фильтр по части названия."""
    where, params = _for_email_where(email, query)
    sql = ("SELECT r.recording_key, r.title, r.created_date, r.duration_sec FROM recordings r "
           "JOIN participant p ON p.recording_key = r.recording_key "
           f"WHERE {where} ORDER BY r.created_date DESC")
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


@_locked
def count_for_email(conn, email, query=None):
    """Сколько записей у пользователя (с тем же фильтром, что в records_for_email)."""
    where, params = _for_email_where(email, query)
    return conn.execute(
        "SELECT COUNT(*) FROM recordings r "
        f"JOIN participant p ON p.recording_key = r.recording_key WHERE {where}",
        params).fetchone()[0]


@_locked
def is_participant(conn, key, email):
    """Был ли пользователь (по почте, без учёта регистра) участником записи — проверка
    доступа перед выдачей файлов."""
    if not _norm_email(email):
        return False
    return conn.execute("SELECT 1 FROM participant WHERE recording_key=? AND email=?",
                        (key, _norm_email(email))).fetchone() is not None


def deleted_fields(by, retention_days, now=None):
    """Поля строки «видео удалено». by — кто удалил: '@ник' админа, 'auto' (демон по
    графику), 'short' (короче минимума) или 'external' (в Толке записи уже не было).
    retention_days=None -> тексты храним бессрочно: срок их очистки не назначаем."""
    now = now or datetime.now(timezone.utc)
    f = {"deleted": 1, "deleted_at": now.isoformat(), "deleted_by": by,
         "status": statuses.VIDEO_DELETED}
    if retention_days is not None:
        f["text_delete_at"] = (now + timedelta(days=retention_days)).isoformat()
    return f


@_locked
def short_retries(conn, now_iso):
    """Слишком короткие записи, которые не удалось удалить из Толка при подхвате и у
    которых подошёл срок очередной попытки (см. main._retry_short)."""
    rows = conn.execute(
        "SELECT * FROM recordings WHERE status=? AND done=1 AND deleted=0 AND kept_forever=0 "
        "AND fail_stage='delete' AND next_due_at IS NOT NULL AND next_due_at<=? "
        "ORDER BY next_due_at", (statuses.TOO_SHORT, now_iso)).fetchall()
    return [_row_to_dict(r) for r in rows]


# ---- «до решения админа»: записи, у которых исчерпаны повторы стадии --------

@_locked
def holds(conn, limit=None, only_unnotified=False):
    """Записи, остановленные «до решения админа» (сначала те, что стоят дольше всех).
    only_unnotified — только те, что ещё не вошли в суточную сводку админам."""
    sql = "SELECT * FROM recordings WHERE needs_admin=1"
    if only_unnotified:
        sql += " AND hold_notified=0"
    sql += " ORDER BY COALESCE(error_since, updated_at)"
    params = []
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))
    return [_row_to_dict(r) for r in conn.execute(sql, params).fetchall()]


@_locked
def hold_counts(conn):
    """Сколько записей стоит «до решения админа» по стадиям: {стадия: число}."""
    rows = conn.execute("SELECT COALESCE(fail_stage, '?'), COUNT(*) FROM recordings "
                        "WHERE needs_admin=1 GROUP BY 1 ORDER BY 2 DESC").fetchall()
    return {r[0]: r[1] for r in rows}


@_locked
def mark_holds_notified(conn, keys):
    conn.executemany("UPDATE recordings SET hold_notified=1 WHERE recording_key=?",
                     [(k,) for k in keys])
    conn.commit()


@_locked
def restart_holds(conn, key=None):
    """Вернуть в работу записи «до решения админа» (все или одну по ключу): сбросить
    счётчик повторов и ошибку, вернуть статус по флагам стадий. Флаги стадий не трогаем,
    поэтому демон продолжит с того этапа, на котором запись остановилась. Возвращает число
    перезапущенных записей."""
    sql, params = "SELECT * FROM recordings WHERE needs_admin=1", []
    if key:
        sql += " AND recording_key=?"
        params.append(key)
    rows = [_row_to_dict(r) for r in conn.execute(sql, params).fetchall()]
    now = _now()
    conn.executemany(
        "UPDATE recordings SET needs_admin=0, hold_notified=0, fail_stage=NULL, fail_count=0, "
        "last_error=NULL, error_since=NULL, done=0, next_due_at=NULL, status=?, updated_at=? "
        "WHERE recording_key=?",
        [(statuses.derive(r), now, r["recording_key"]) for r in rows])
    conn.commit()
    return len(rows)


# ---- выборки для админ-панели ----------------------------------------------

@_locked
def search_recordings(conn, q=None, who=None, limit=25):
    """Поиск записей для админа, от новых к старым. q — ID (целиком или начало) либо часть
    названия; who — участник (часть имени, ника или почты). Заданы оба — должны совпасть оба."""
    where, params = [], []
    if q:
        where.append("(recording_key = ? OR recording_key LIKE ? ESCAPE '\\' "
                     "OR ulower(title) LIKE ? ESCAPE '\\')")
        params += [q, _like(q)[1:], _like(q)]          # [1:] — без ведущего %: «начинается с»
    if who:
        where.append("ulower(participants) LIKE ? ESCAPE '\\'")
        params.append(_like(who))
    if not where:
        return []
    sql = (f"SELECT * FROM recordings WHERE {' AND '.join(where)} "
           "ORDER BY created_date DESC LIMIT ?")
    return [_row_to_dict(r) for r in conn.execute(sql, params + [int(limit)]).fetchall()]


@_locked
def upcoming_deletions(conn, limit=15):
    """Ближайшие удаления видео по графику (участники уведомлены, срок назначен)."""
    rows = conn.execute(f"SELECT * FROM recordings WHERE {_PENDING_DELETE} "
                        "ORDER BY delete_scheduled_at LIMIT ?", (int(limit),)).fetchall()
    return [_row_to_dict(r) for r in rows]


@_locked
def list_kept(conn, limit=25):
    """Видео, которые не будут удалены: защищены админом (remember) или запретом Толка."""
    rows = conn.execute("SELECT * FROM recordings WHERE (protected=1 OR kept_forever=1) "
                        "AND deleted=0 ORDER BY created_date DESC LIMIT ?",
                        (int(limit),)).fetchall()
    return [_row_to_dict(r) for r in rows]


@_locked
def stats(conn):
    """Сводка состояния архива для админ-панели (счётчики записей, пользователи, фаза)."""
    d = dict(conn.execute("""
        SELECT
          COUNT(*)                                                                    total,
          COALESCE(SUM(saved_to_db=1), 0)                                             saved,
          COALESCE(SUM(transcribed=0 AND no_audio=0 AND needs_admin=0 AND done=0), 0) wait_asr,
          COALESCE(SUM(transcribed=1 AND saved_to_db=0 AND needs_admin=0), 0)         wait_llm,
          COALESCE(SUM(needs_admin=1), 0)                                             hold,
          COALESCE(SUM(fail_count>0 AND needs_admin=0 AND done=0), 0)                 retrying,
          COALESCE(SUM(no_audio=1), 0)                                                no_audio,
          COALESCE(SUM(no_channels=1), 0)                                             no_channels,
          COALESCE(SUM(COALESCE(status,'')=?), 0)                                     too_short,
          COALESCE(SUM(protected=1 AND deleted=0), 0)                                 protected,
          COALESCE(SUM(kept_forever=1), 0)                                            kept_forever,
          COALESCE(SUM(deleted=1 AND COALESCE(status,'')!=?), 0)                      deleted,
          COALESCE(SUM(delete_simulated=1 AND deleted=0), 0)                          delete_sim,
          COALESCE(SUM(text_cleaned=1), 0)                                            text_cleaned,
          MAX(updated_at)                                                             last_update
        FROM recordings""", (statuses.TOO_SHORT, statuses.TOO_SHORT)).fetchone())
    r = conn.execute(f"SELECT COUNT(*), MIN(delete_scheduled_at) FROM recordings "
                     f"WHERE {_PENDING_DELETE}").fetchone()
    d["delete_pending"], d["next_delete_at"] = r[0], r[1]
    u = conn.execute("SELECT COUNT(*), COALESCE(SUM(notified_of_old_records_tag=1), 0), "
                     "COALESCE(SUM(notified_of_old_records_mail=1), 0) FROM users").fetchone()
    d["users"], d["users_tag"], d["users_mail"] = u[0], u[1], u[2]
    m = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    d["old_records_flag"] = m.get("old_records_all_notified") == "1"
    d["old_records_at"] = m.get("old_records_notified_at")
    return d


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
def admins(conn):
    """Админы с подробностями (кто и когда добавил) — для админ-панели."""
    return [dict(r) for r in conn.execute(
        "SELECT username, added_by, added_at FROM admins ORDER BY added_at").fetchall()]


@_locked
def remove_admin(conn, username):
    cur = conn.execute("DELETE FROM admins WHERE username=?", (username,))
    conn.commit()
    return cur.rowcount


def superadmin(conn):
    """Ник суперадмина (или None, если он ещё не назначен)."""
    return get_meta(conn, "superadmin") or None


def set_superadmin(conn, username):
    set_meta(conn, "superadmin", username)


@_locked
def create_admin_request(conn, kind, target, by, effective_at):
    """Завести решение об админах с периодом охлаждения; возвращает его id."""
    cur = conn.execute(
        "INSERT INTO admin_requests (kind, target, requested_by, requested_at, effective_at) "
        "VALUES (?,?,?,?,?)", (kind, target, by, _now(), effective_at))
    conn.commit()
    return cur.lastrowid


@_locked
def admin_request(conn, req_id):
    r = conn.execute("SELECT * FROM admin_requests WHERE id=?", (req_id,)).fetchone()
    return dict(r) if r else None


@_locked
def admin_requests(conn, due_before=None):
    """Решения об админах, которые ещё ждут вступления в силу (сначала ближайшие по сроку).
    due_before — только те, чей срок уже наступил (ISO-момент «сейчас»)."""
    sql, params = "SELECT * FROM admin_requests WHERE status='pending'", []
    if due_before:
        sql += " AND effective_at<=?"
        params.append(due_before)
    return [dict(r) for r in conn.execute(sql + " ORDER BY effective_at, id", params).fetchall()]


@_locked
def resolve_admin_request(conn, req_id, status, by=None, note=None):
    """Закрыть решение (applied / blocked / withdrawn / cancelled), если оно ещё ждёт.
    True — закрыли мы; False — его уже закрыл кто-то другой (блокировка и срок могут
    совпасть по времени — исполняется только одно)."""
    cur = conn.execute(
        "UPDATE admin_requests SET status=?, resolved_by=?, resolved_at=?, note=? "
        "WHERE id=? AND status='pending'", (status, by, _now(), note, req_id))
    conn.commit()
    return cur.rowcount == 1


@_locked
def log_admin(conn, username, action, key=None, details=None):
    """Записать действие админа в журнал (admin_log) — и в лог процесса: кто удалил видео
    или поменял состав админов, должно быть видно и там."""
    conn.execute("INSERT INTO admin_log (at, username, action, recording_key, details) "
                 "VALUES (?,?,?,?,?)", (_now(), username, action, key, details))
    conn.commit()
    log.info("журнал админов: @%s — %s%s%s", username, action,
             f" [{key}]" if key else "", f" ({details})" if details else "")


@_locked
def admin_log(conn, limit=20):
    """Последние действия админов, от новых к старым."""
    return [dict(r) for r in conn.execute(
        "SELECT * FROM admin_log ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()]


# ---- пользователи (таблица users) ------------------------------------------

@_locked
def upsert_user(conn, *, login, email=None, first_name=None, last_name=None):
    """Создать/обновить пользователя по login (идемпотентно). Флаг notified_of_old_records
    при обновлении НЕ трогаем. COALESCE — не затираем существующие поля, если пришло None.
    Возвращает id пользователя."""
    now = _now()
    conn.execute(
        "INSERT INTO users (login, email, first_name, last_name, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(login) DO UPDATE SET "
        "  email=COALESCE(excluded.email, users.email), "
        "  first_name=COALESCE(excluded.first_name, users.first_name), "
        "  last_name=COALESCE(excluded.last_name, users.last_name), "
        "  updated_at=excluded.updated_at",
        (login, email, first_name, last_name, now, now))
    conn.commit()
    r = conn.execute("SELECT id FROM users WHERE login=?", (login,)).fetchone()
    return r[0] if r else None


@_locked
def get_user(conn, login):
    """Пользователь по login (или None)."""
    r = conn.execute("SELECT * FROM users WHERE login=?", (login,)).fetchone()
    return dict(r) if r else None


@_locked
def get_user_by_email(conn, email):
    """Пользователь по email (или None)."""
    r = conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    return dict(r) if r else None


@_locked
def set_notified_of_old_records(conn, login, *, tag=None, mail=None):
    """Проставить/снять флаги уведомления о старых записях. tag/mail — True/False/None
    (None = не трогать этот канал)."""
    sets, params = [], []
    if tag is not None:
        sets.append("notified_of_old_records_tag=?"); params.append(1 if tag else 0)
    if mail is not None:
        sets.append("notified_of_old_records_mail=?"); params.append(1 if mail else 0)
    if not sets:
        return
    sets.append("updated_at=?"); params.append(_now())
    params.append(login)
    conn.execute(f"UPDATE users SET {', '.join(sets)} WHERE login=?", params)
    conn.commit()


@_locked
def list_users_pending(conn, *, need_tag, need_mail):
    """Пользователи, которым ещё НЕ отправлено old-records-уведомление хотя бы по одному
    ТРЕБУЕМОМУ каналу (need_tag/need_mail — включён ли канал). Если ни один канал не
    требуется — пустой список (слать некуда)."""
    conds = []
    if need_tag:
        conds.append("notified_of_old_records_tag=0")
    if need_mail:
        conds.append("notified_of_old_records_mail=0")
    if not conds:
        return []
    rows = conn.execute(
        f"SELECT * FROM users WHERE {' OR '.join(conds)} ORDER BY id").fetchall()
    return [dict(r) for r in rows]


@_locked
def all_users_notified(conn, *, need_tag, need_mail):
    """True, если по всем ТРЕБУЕМЫМ каналам уведомлены все пользователи (и юзеры вообще
    есть). Если ни один канал не требуется — считаем False (рассылать нечем)."""
    if not (need_tag or need_mail):
        return False
    if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        return False
    conds = []
    if need_tag:
        conds.append("notified_of_old_records_tag=0")
    if need_mail:
        conds.append("notified_of_old_records_mail=0")
    pending = conn.execute(
        f"SELECT COUNT(*) FROM users WHERE {' OR '.join(conds)}").fetchone()[0]
    return pending == 0


@_locked
def list_users(conn):
    """Все пользователи (list[dict])."""
    return [dict(r) for r in conn.execute("SELECT * FROM users ORDER BY id").fetchall()]


@_locked
def users_notified_count(conn):
    """Скольким пользователям old-records-уведомление доставлено хотя бы по одному каналу."""
    return conn.execute("SELECT COUNT(*) FROM users WHERE notified_of_old_records_tag=1 "
                        "OR notified_of_old_records_mail=1").fetchone()[0]


# ---- служебное состояние (таблица meta) ------------------------------------

@_locked
def get_meta(conn, key, default=None):
    r = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return r[0] if r else default


@_locked
def set_meta(conn, key, value):
    conn.execute("INSERT INTO meta (key, value) VALUES (?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                 (key, str(value)))
    conn.commit()


@_locked
def mark_backlog_for_deletion(conn, cutoff_iso, delete_at_iso):
    """При флипе флага «все уведомлены»: пометить ВСЕ старые (created<cutoff) сохранённые
    записи как уже уведомлённые (messages_sent=1, статус NOTIFIED — единоразовая рассылка
    их покрыла) и назначить срок удаления видео. Защищённые (remember), «сохранённые
    бессрочно», ждущие решения админа и уже удалённые не трогаем. Возвращает число помеченных."""
    now = _now()
    cur = conn.execute(
        "UPDATE recordings SET messages_sent=1, status=?, delete_scheduled_at=?, "
        "done=0, next_due_at=?, updated_at=? "
        "WHERE created_date < ? AND saved_to_db=1 AND messages_sent=0 "
        "AND deleted=0 AND protected=0 AND kept_forever=0 AND needs_admin=0",
        (statuses.NOTIFIED, delete_at_iso, delete_at_iso, now, cutoff_iso))
    conn.commit()
    return cur.rowcount


@_locked
def backlog_pending_count(conn, cutoff_iso):
    """Сколько записей СТАРШЕ порога (created_date < cutoff_iso) ещё НЕ доведены до конца
    бэкфилла: не сохранены в БД, не «нет аудио», не терминальны (done). 0 => бэклог
    старых записей полностью перелопачен (можно рассылать old-records-уведомление)."""
    r = conn.execute(
        "SELECT COUNT(*) FROM recordings WHERE created_date < ? "
        "AND saved_to_db=0 AND no_audio=0 AND done=0", (cutoff_iso,)).fetchone()
    return r[0]
