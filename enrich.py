"""
enrich.py — LLM-пост-обработка расшифровки (чистка терминологии, вид стенограммы).

improve_transcript(text, llm) -> строка markdown (вычищенная расшифровка);
summarize_text(text, llm) -> строка markdown (короткая выжимка). Файлы читает/пишет
pipeline.py; здесь только логика общения с LLM (включая разбивку длинного текста на
части и объединение таблиц замен в одну). На вход — уже готовый ТЕКСТ расшифровки.
"""

import logging
import re

from talk_api import load_env

log = logging.getLogger("enrich")

SYSTEM_PROMPT = (
    "Ты файловый редактор текста после транскрибации записи видеоконференции. "
    "Проверь текст на корректность терминологии, англицизмов. Приведи текст к "
    "очищенному, структурированному и профессиональному виду стенограммы планёрки. "
    "Сохрани оригинальные таймкоды и все имена/фамилии; исправь ошибки "
    "автоматического распознавания речи (в том числе технические термины и названия "
    "систем). Сведи основные замены в таблицу в формате "
    "`ЧТО БЫЛО (ОШИБКА/ЖАРГОН) | СТАЛО | ПРИМЕЧАНИЕ`. Верни ГОТОВЫЙ Markdown-текст БЕЗ обёртки в кодовый блок (не заворачивай ответ в ```)."
)

# Системный промт для КОРОТКОЙ ВЫЖИМКИ (файл <ID>_short). Отдельная задача: не
# редактура, а сжатие уже вычищенной расшифровки до сути встречи.
SUMMARY_SYSTEM_PROMPT = (
    "Ты помощник, который делает краткую деловую выжимку по расшифровке "
    "видеоконференции. На вход — полная стенограмма с таймкодами и именами. "
    "Сделай сжатую выжимку в .md: 2–4 предложения общего резюме, затем разделы "
    "«Ключевые темы» (маркированный список), «Принятые решения» и «Задачи/договорённости» "
    "(кто — что; если явных нет, напиши «не зафиксировано»). Пиши по-русски, кратко и "
    "по делу, без воды. Не выдумывай факты, которых нет в тексте. Верни ГОТОВЫЙ "
    "Markdown-текст БЕЗ обёртки в кодовый блок (не заворачивай ответ в ```)."
)


def _unfence(text):
    """Снять обёртку ```markdown … ``` (LLM часто заворачивает ответ в кодовый блок —
    из-за этого в превью .md ничего не рендерится, включая таблицы)."""
    t = (text or "").strip()
    if t.startswith("```"):
        lines = t.split("\n")
        lines = lines[1:]                       # убрать первую строку ```lang
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]                  # убрать закрывающую ```
        t = "\n".join(lines).strip()
    return t


def _chunk_text(text, budget):
    """Разбить текст на части не длиннее budget символов по строкам (ограничение
    контекста LLM). Границы — по строкам, чтобы не рвать реплики посередине."""
    cur, size = [], 0
    for ln in text.splitlines():
        if cur and size + len(ln) + 1 > budget:
            yield "\n".join(cur)
            cur, size = [], 0
        cur.append(ln)
        size += len(ln) + 1
    if cur:
        yield "\n".join(cur)


def _split_table(md):
    """Разделить ответ LLM на (тело_без_таблицы, строки_таблицы) — для сборки одной
    таблицы замен из нескольких частей."""
    body, rows, in_table = [], [], False
    for ln in md.splitlines():
        if re.match(r"\s*\|.*\|\s*$", ln):
            in_table = True
            if re.match(r"\s*\|[\s:\-|]+\|\s*$", ln):
                continue                       # строка-разделитель |---|
            rows.append(ln.strip())
        else:
            if in_table and not ln.strip():
                continue
            in_table = False
            body.append(ln)
    return "\n".join(body).strip(), rows


def improve_transcript(text, llm):
    """
    Прогнать ТЕКСТ расшифровки через LLM и вернуть строку markdown.
    Короткий текст — один запрос. Длинный — по частям + объединение таблиц.
    Бросает исключение при сбое LLM (pipeline поймает и оставит improved=false).
    """
    max_chars = int(load_env().get("LLM_MAX_CHARS", "30000"))
    text = text.strip()

    if len(text) <= max_chars:
        return _unfence(llm.chat(SYSTEM_PROMPT, text))

    bodies, all_rows, header = [], [], None
    parts = list(_chunk_text(text, max_chars))
    log.info("LLM-правка: длинный текст, частей=%d", len(parts))
    for i, part in enumerate(parts, 1):
        log.info("  LLM-правка часть %d/%d", i, len(parts))
        body, rows = _split_table(_unfence(llm.chat(SYSTEM_PROMPT, part)))
        bodies.append(body)
        for r in rows:
            if header is None:
                header = r
            elif r != header and r not in all_rows:
                all_rows.append(r)
    md = "\n\n".join(bodies)
    if header:
        md += "\n\n## Основные замены\n\n" + header + "\n| --- | --- | --- |\n" + "\n".join(all_rows)
    return md


def summarize_text(full_md, llm):
    """
    Короткая выжимка (<ID>_short) из уже готовой полной расшифровки (<ID>_full).
    Если полный текст длиннее лимита — сжимаем «в два прохода»: каждую часть в
    мини-резюме, затем резюме резюме. Бросает исключение при сбое LLM (pipeline
    поймает и оставит summarized=false -> повтор позже).
    """
    max_chars = int(load_env().get("LLM_MAX_CHARS", "30000"))
    text = full_md.strip()
    if len(text) <= max_chars:
        return _unfence(llm.chat(SUMMARY_SYSTEM_PROMPT, text))

    # длинный текст: режем по абзацам на куски, резюмируем каждый, потом объединяем
    parts, cur, size = [], [], 0
    for para in text.split("\n\n"):
        if cur and size + len(para) > max_chars:
            parts.append("\n\n".join(cur))
            cur, size = [], 0
        cur.append(para)
        size += len(para) + 2
    if cur:
        parts.append("\n\n".join(cur))

    log.info("выжимка: длинный текст, частей=%d + сведение", len(parts))
    partials = [_unfence(llm.chat(SUMMARY_SYSTEM_PROMPT, p)) for p in parts]
    if len(partials) == 1:
        return partials[0]
    return _unfence(llm.chat(SUMMARY_SYSTEM_PROMPT,
                    "Сведи промежуточные выжимки частей встречи в одну итоговую "
                    "выжимку по тем же разделам:\n\n" + "\n\n---\n\n".join(partials)))
