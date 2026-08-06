"""
store.py — ФАЙЛОВОЕ ХРАНИЛИЩЕ выходных файлов записи (состояние пайплайна — в БД).

Раскладка ШАРДИРОВАНА по дате записи, чтобы в одной директории не копились сотни
тысяч подпапок:
    Transcripts/<ГГГГ>/<ММ>/<ID>/
        <ID>_full.md          # шапка (Название/Дата/Длительность/Участники) + расшифровка
        <ID>_short.md         # короткая выжимка (LLM)

Папка записи (rec_dir) вычисляется из даты + ключа (store.rec_dir) и хранится в БД
(recordings.rec_dir). Промежуточных файлов нет — вся память пайплайна в БД, на диске
только два итоговых .md.
"""

import os
import shutil
from datetime import datetime

from talk_api import hhmmss


def _parse_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError:
        return None


def rec_dir(base, key, created_date):
    """Путь к папке записи: base/ГГГГ/ММ/<key> (по дате записи). Если даты нет —
    складываем в base/unknown/<key>. Папку НЕ создаём (создастся при записи файлов)."""
    dt = _parse_dt(created_date)
    sub = dt.strftime("%Y/%m") if dt else "unknown"
    return os.path.join(base, sub, key)


def full_path(rd, key):
    return os.path.join(rd, f"{key}_full.md")


def short_path(rd, key):
    return os.path.join(rd, f"{key}_short.md")


def blocks_to_md(blocks):
    """Читаемая стенограмма из реплик. blocks — КОРТЕЖИ (start, end, speaker, text)."""
    return "\n\n".join(
        f"{sp} [{hhmmss(s)}-{hhmmss(e)}]: {t}" for s, e, sp, t in blocks)


def write_transcript(rd, key, blocks):
    """Записать сырую расшифровку в <ID>_full.md (пока БЕЗ шапки и LLM-правок —
    стадия improve перезапишет тело, стадия save_to_db добавит шапку)."""
    write_full_md(rd, key, blocks_to_md(blocks))


HEADER_PREFIX = "Название:"


def strip_header(md):
    """Убрать ведущую шапку (Название/…/Участники) из <ID>_full.md, если она есть —
    чтобы повторная запись шапки не задваивалась. Шапка = блок до первой пустой строки."""
    if md.lstrip().startswith(HEADER_PREFIX):
        parts = md.split("\n\n", 1)
        return parts[1] if len(parts) == 2 else ""
    return md


def write_full_md(rd, key, md):
    """Записать/перезаписать <ID>_full.md (полная расшифровка)."""
    os.makedirs(rd, exist_ok=True)
    with open(full_path(rd, key), "w") as f:
        f.write(md.rstrip() + "\n")


def write_short_md(rd, key, md):
    """Записать <ID>_short.md — короткую выжимку LLM."""
    os.makedirs(rd, exist_ok=True)
    with open(short_path(rd, key), "w") as f:
        f.write(md.rstrip() + "\n")


def read_full_md(rd, key):
    """Прочитать текущий <ID>_full.md (вход для стадий improve/summarize)."""
    p = full_path(rd, key)
    return open(p).read() if os.path.exists(p) else None


def remove_record_dir(rd):
    """Удалить папку записи целиком (5-летняя очистка текстов). Запись остаётся
    «надгробием» в БД."""
    shutil.rmtree(rd, ignore_errors=True)
