"""
store.py — ФАЙЛОВОЕ ХРАНИЛИЩЕ выходных файлов записи (состояние пайплайна — в БД).

Раскладка ШАРДИРОВАНА по дате записи, чтобы в одной директории не копились сотни
тысяч подпапок:
    Transcripts/<ГГГГ>/<ММ>/<ID>/
        <ID>_ASR.md           # ДОСЛОВНЫЙ ASR (истина; LLM его НЕ перезаписывает)
        <ID>_LLM.md           # LLM-обработанная расшифровка (чистка терминологии/вид стенограммы)
        <ID>_short.md         # короткая выжимка (LLM)
Шапка (Название/Дата/Длительность/Участники) добавляется в начало _ASR.md и _LLM.md.

Папка записи (rec_dir) вычисляется из даты + ключа (store.rec_dir) и хранится в БД
(recordings.rec_dir). Промежуточных файлов нет — вся память пайплайна в БД, на диске
только итоговые .md. Дословный _ASR.md отделён от _LLM.md специально: любая LLM-правка
может потерять/исказить текст, поэтому оригинал распознавания хранится нетронутым.
"""

import os
import shutil

from talk_api import hhmmss
from util import parse_dt as _parse_dt


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


def _write(path, md):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:    # кодировка явно: не зависим от локали хоста
        f.write(md.rstrip() + "\n")


def _read(path):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return f.read()


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
