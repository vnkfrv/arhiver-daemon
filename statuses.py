"""
statuses.py — СТАТУСЫ видеозаписи.

В БД (поле status) пишется КОД — латиницей, без пробелов: его удобно фильтровать в SQL и
искать в логах, и он не меняется, если захочется переформулировать текст. Человеку (админу
в карточке записи) показываем расшифровку — LABELS; для стадий конвейера это формулировки
из ТЗ.

Статус = последняя пройденная стадия; порядок = порядок стадий пайплайна:
    NEW -> TRANSCRIBED -> IMPROVED -> SUMMARIZED -> SAVED -> NOTIFIED -> VIDEO_DELETED
        -> TEXT_DELETED
Если стадия не удалась после всех повторов, запись ждёт решения админа (restart_passed) со
статусом FAILED_<стадия>: FAILED_ASR, FAILED_LLM и т.д. (см. failed()).
"""

NEW = "NEW"
TRANSCRIBED = "TRANSCRIBED"
IMPROVED = "IMPROVED"
SUMMARIZED = "SUMMARIZED"
SAVED = "SAVED"
NOTIFIED = "NOTIFIED"
VIDEO_DELETED = "VIDEO_DELETED"
TEXT_DELETED = "TEXT_DELETED"
# Запись короче MIN_DURATION: не расшифровывается, удаляется при подхвате. Удалено ли видео
# на самом деле — говорят флаги строки (deleted / delete_simulated / kept_forever), не статус.
TOO_SHORT = "TOO_SHORT"
# Толк запрещает удалять это видео: оставляем его навсегда и больше не трогаем.
DELETE_PROTECTION = "DELETE_PROTECTION"

# Стадия пайплайна -> окончание статуса FAILED_… (оно же — в суточной сводке админам).
STAGE_SUFFIX = {
    "transcribe": "ASR",
    "improve": "LLM",
    "summarize": "SHORT",
    "save_to_db": "SAVE",
    "messages": "NOTIFY",
    "delete": "DELETE",
    "cleanup_text": "CLEANUP",
}

# Названия стадий для людей (сводка админам, админ-панель).
STAGE_NAMES = {
    "transcribe": "транскрибация",
    "improve": "LLM-правка",
    "summarize": "выжимка",
    "save_to_db": "сохранение",
    "messages": "рассылка участникам",
    "delete": "удаление видео",
    "cleanup_text": "очистка текстов",
}


def failed(stage):
    """Статус записи, у которой стадия stage не удалась после всех повторов и которая ждёт
    решения админа: FAILED_ASR, FAILED_LLM, …"""
    return f"FAILED_{STAGE_SUFFIX.get(stage) or str(stage or 'unknown').upper()}"


# Расшифровка статусов для людей.
LABELS = {
    NEW: "до преобразования",
    TRANSCRIBED: "выполнена транскрибация",
    IMPROVED: "выполнено ЛЛМ-преобразование",
    SUMMARIZED: "выполнена короткая выжимка",
    SAVED: "выполнено сохранение в систему",
    NOTIFIED: "разосланы сообщения в Tag",
    VIDEO_DELETED: "видеозапись удалена",
    TEXT_DELETED: "тексты удалены (срок хранения истёк)",
    TOO_SHORT: "короче минимальной длины — не расшифровывается, удаляется при подхвате",
    DELETE_PROTECTION: "сохранено бессрочно (Толк запрещает удалять видео)",
}
LABELS.update({failed(stage): f"до решения админа: не удалась стадия «{name}»"
               for stage, name in STAGE_NAMES.items()})


def label(status):
    """Расшифровка статуса по-русски (незнакомый код возвращаем как есть)."""
    return LABELS.get(status, status or "—")


def derive(row):
    """Статус по флагам стадий — когда запись возвращают в работу из FAILED_… (там статус
    затёрт, а продолжать нужно с того этапа, на котором она остановилась)."""
    if row.get("text_cleaned"):
        return TEXT_DELETED
    if row.get("deleted"):
        return VIDEO_DELETED
    if row.get("kept_forever"):
        return DELETE_PROTECTION
    if row.get("messages_sent"):
        return NOTIFIED
    if row.get("saved_to_db"):
        return SAVED
    if row.get("summarized"):
        return SUMMARIZED
    if row.get("improved"):
        return IMPROVED
    if row.get("transcribed"):
        return TRANSCRIBED
    return NEW
