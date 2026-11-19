"""
store.py — ФАЙЛОВОЕ ХРАНИЛИЩЕ выходных файлов записи (состояние пайплайна — в БД).

Раскладка ШАРДИРОВАНА по дате записи, чтобы в одной директории не копились сотни
тысяч подпапок:
    Transcripts/<ГГГГ>/<ММ>/<ID>/
        <ID>_ASR.md           # ДОСЛОВНЫЙ ASR (истина; LLM его НЕ перезаписывает)
        <ID>_LLM.md           # LLM-обработанная расшифровка (чистка терминологии/вид стенограммы)
        <ID>_short.md         # короткая выжимка (LLM)
Шапка (Название/Дата/Длительность/Участники) добавляется в начало _ASR.md и _LLM.md.
Каждый готовый файл пишется атомарно (временный файл + подмена) и кончается проверочной
фразой MARKER — по ней проверяется, что файл записан целиком (problem).

Папка записи (rec_dir) вычисляется из даты + ключа (store.rec_dir) и хранится в БД
(recordings.rec_dir). Промежуточных файлов нет — вся память пайплайна в БД, на диске
только итоговые .md. Дословный _ASR.md отделён от _LLM.md специально: любая LLM-правка
может потерять/исказить текст, поэтому оригинал распознавания хранится нетронутым.
"""

import os
import shutil
import threading

from talk_api import hhmmss
from util import parse_dt as _parse_dt

# Проверочная фраза — последняя строка КАЖДОГО готового файла (_ASR, _LLM, _short): по ней
# видно, что файл записан целиком. При чтении она снимается, поэтому LLM, выжимка и повторное
# сохранение её не видят и не задваивают. Пользователь видит её последней строкой файла.
MARKER = "Транскрибация видеоконференции завершена."
# Отметка в meta: фраза перенесена во все старые целые файлы (fix_broken_md.py --apply) —
# с этого момента файл без фразы считается неполным.
MARKER_MIGRATED = "md_marker_migrated"

# Журнал записанных файлов (пути, по строке) — по нему утренняя проверка смотрит всё, что
# записано с прошлой проверки. Включает демон (set_journal); бот и скрипты пишут без журнала.
_journal_path = None
_journal_lock = threading.Lock()


def rec_dir(base, key, created_date):
    """Путь к папке записи: base/ГГГГ/ММ/<key> (по дате записи). Если даты нет —
    складываем в base/unknown/<key>. Папку НЕ создаём (создастся при записи файлов)."""
    dt = _parse_dt(created_date)
    sub = dt.strftime("%Y/%m") if dt else "unknown"
    return os.path.join(base, sub, key)


def asr_path(rd, key):
    return os.path.join(rd, f"{key}_ASR.md")


def llm_path(rd, key):
    return os.path.join(rd, f"{key}_LLM.md")


def short_path(rd, key):
    return os.path.join(rd, f"{key}_short.md")


def _old_full_path(rd, key):
    # Старое единое имя <ID>_full.md (до разделения на _ASR/_LLM). Оставлено для
    # обратной совместимости: записи, обработанные ДО перехода, читаются отсюда.
    return os.path.join(rd, f"{key}_full.md")


def blocks_to_md(blocks):
    """Читаемая стенограмма из реплик. blocks — КОРТЕЖИ (start, end, speaker, text)."""
    return "\n\n".join(
        f"{sp} [{hhmmss(s)}-{hhmmss(e)}]: {t}" for s, e, sp, t in blocks)


def write_transcript(rd, key, blocks):
    """Записать ДОСЛОВНУЮ ASR-расшифровку в <ID>_ASR.md (пока БЕЗ шапки; шапку добавит
    save_to_db). Этот файл LLM НЕ перезаписывает — он остаётся истиной распознавания."""
    write_asr_md(rd, key, blocks_to_md(blocks))


HEADER_PREFIX = "Название:"
WARN_PREFIX = "⚠️"


def strip_header(md):
    """Убрать ведущий баннер-предупреждение (если есть) И шапку (Название/…/Участники),
    чтобы повторная запись не задваивала их. Каждый блок = до первой пустой строки."""
    t = md.lstrip()
    if t.startswith(WARN_PREFIX):                 # сначала снять баннер усушки
        parts = t.split("\n\n", 1)
        t = (parts[1] if len(parts) == 2 else "").lstrip()
    if t.startswith(HEADER_PREFIX):               # затем снять шапку
        parts = t.split("\n\n", 1)
        return parts[1] if len(parts) == 2 else ""
    return t


def strip_marker(text):
    """Текст файла без проверочной фразы в конце (и без хвостовых пробелов)."""
    t = text.rstrip()
    return t[:-len(MARKER)].rstrip() if t.endswith(MARKER) else t


def _fsync_dir(d):
    """Сбросить на диск саму папку — чтобы подмена файла пережила и внезапное выключение."""
    try:
        fd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _write(path, md):
    """Записать готовый файл АТОМАРНО и с проверкой: текст и проверочная фраза пишутся во
    временный файл рядом, сбрасываются на диск, перечитываются и сверяются байт в байт — и
    только потом одной операцией подменяют прежний. Кончилось место или процесс убили посреди
    записи — на диске остаётся прежняя целая версия, а не обнулённый или обрезанный файл."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = (strip_marker(md) + "\n\n" + MARKER + "\n").encode("utf-8")
    tmp = path + ".tmp"                  # запись одной записи ведёт один воркер — имя не делится
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        with open(tmp, "rb") as f:
            if f.read() != data:
                raise OSError(f"{os.path.basename(path)}: записанное не совпало с прочитанным")
        os.replace(tmp, path)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
    _fsync_dir(os.path.dirname(path))
    _journal(path)


def _read(path):
    """Текст файла без проверочной фразы (None — файла нет)."""
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:   # кодировка явно: не зависим от локали хоста
        return strip_marker(f.read())


def problem(path, strict=True):
    """Что не так с готовым файлом; None — файл цел. Читает файл целиком: пустой, битый
    посреди буквы, без текста под шапкой — неполный. strict — обязательна проверочная фраза;
    иначе файл без неё (записанный до появления фразы) считается целым, если кончается ровно
    так, как архиватор всегда дописывал файлы: текст и один перевод строки."""
    if not os.path.exists(path):
        return "нет файла"
    with open(path, "rb") as f:
        raw = f.read()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return "обрезан посреди буквы"
    if not text.strip():
        return "пустой"
    if text.rstrip().endswith(MARKER):
        body = strip_marker(text)
    elif strict:
        return "нет проверочной фразы"
    elif not text.endswith("\n") or text[-2].isspace():
        return "обрезан (нет конца файла)"
    else:
        body = text
    if not strip_header(body).strip():
        return "только шапка, текста нет"
    return None


# ---- журнал записанных файлов (для утренней проверки) -----------------------

def set_journal(path):
    """Включить журнал: каждый записанный файл дописывается строкой в path."""
    global _journal_path
    _journal_path = path


def _journal(path):
    if not _journal_path:
        return
    with _journal_lock, open(_journal_path, "a", encoding="utf-8") as f:
        f.write(path + "\n")


def take_journal():
    """Забрать журнал на проверку: он переименовывается (новые записи пойдут в свежий файл), а
    недоразобранный остаток прошлой проверки подхватывается. Возвращает множество путей."""
    if not _journal_path:
        return set()
    checking = _journal_path + ".checking"
    with _journal_lock:
        if os.path.exists(_journal_path):
            if os.path.exists(checking):
                with open(_journal_path, encoding="utf-8") as src, \
                        open(checking, "a", encoding="utf-8") as dst:
                    dst.write(src.read())
                os.remove(_journal_path)
            else:
                os.replace(_journal_path, checking)
    if not os.path.exists(checking):
        return set()
    with open(checking, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def done_journal():
    """Проверка по журналу закончена — разобранную часть удалить."""
    if _journal_path:
        try:
            os.remove(_journal_path + ".checking")
        except FileNotFoundError:
            pass


def write_asr_md(rd, key, md):
    """Записать/перезаписать <ID>_ASR.md (дословный ASR + шапка на стадии save_to_db)."""
    _write(asr_path(rd, key), md)


def write_llm_md(rd, key, md):
    """Записать/перезаписать <ID>_LLM.md (LLM-обработанная расшифровка)."""
    _write(llm_path(rd, key), md)


def write_short_md(rd, key, md):
    """Записать <ID>_short.md — короткую выжимку LLM."""
    _write(short_path(rd, key), md)


def read_asr_md(rd, key):
    """Дословный ASR (<ID>_ASR.md); вход для стадии improve. Fallback на старый
    <ID>_full.md — чтобы записи, транскрибированные ДО перехода, не потеряли текст."""
    return _read(asr_path(rd, key)) or _read(_old_full_path(rd, key))


def read_llm_md(rd, key):
    """LLM-обработанная расшифровка (<ID>_LLM.md); вход для стадии summarize."""
    return _read(llm_path(rd, key))


def read_short_md(rd, key):
    """Короткая выжимка (<ID>_short.md) — чтобы добавить в неё шапку на стадии save."""
    return _read(short_path(rd, key))


def _first_existing(*paths):
    """Первый из путей, который существует на диске (или None). Пустые/None пропускаем."""
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None


def existing_path(row, kind):
    """Путь к ГОТОВОМУ текстовому файлу записи по типу (asr/llm/short) или None, если файла
    нет. row — строка БД. С фоллбэком на старое имя <ID>_full.md: так читаются записи,
    обработанные до разделения на _ASR/_LLM."""
    rd, key = row.get("rec_dir") or "", row["recording_key"]
    if kind == "asr":            # базовая (дословная) расшифровка
        return _first_existing(asr_path(rd, key), _old_full_path(rd, key))
    if kind == "llm":            # улучшенная (LLM) расшифровка
        return _first_existing(llm_path(rd, key), row.get("full_path"), _old_full_path(rd, key))
    return _first_existing(row.get("short_path"), short_path(rd, key))   # краткая выжимка


def remove_record_dir(rd, key):
    """Удалить папку записи целиком (очистка текстов по сроку хранения). Запись остаётся
    «надгробием» в БД. Две самопроверки: перед удалением — что это папка именно ЭТОЙ записи
    (путь оканчивается её ключом), после — что папки действительно больше нет. Не удалось
    — исключение: пометить тексты удалёнными, когда они лежат на диске, нельзя."""
    if not rd or not key or os.path.basename(os.path.normpath(rd)) != key:
        raise ValueError(f"путь {rd!r} не похож на папку записи {key!r} — удалять не буду")
    shutil.rmtree(rd, ignore_errors=True)
    if os.path.exists(rd):
        raise OSError(f"папку {rd} удалить не удалось (нет прав или файлы заняты)")
