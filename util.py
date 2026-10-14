"""
util.py — мелкие общие утилиты без внешних зависимостей (чтобы не дублировать по модулям).
"""

from datetime import datetime

from texts import RECORD_LABEL


def err(ex):
    """Короткое представление ошибки «Тип: сообщение» (обрезано) — чтобы в логах не было
    простыней трейсбеков на рутинных сбоях (таймаут Talk/шлюза и т.п.)."""
    return f"{type(ex).__name__}: {ex}"[:300]


def username_from_email(email):
    """username в Tag = тело корпоративной почты до «@» — строчными: ники в Tag всегда в
    нижнем регистре, а почта из Толка может прийти как «Ivan.Petrov@corp.ru»."""
    return (email or "").split("@", 1)[0].strip().lower()


def parse_dt(s):
    """ISO-строка -> datetime (понимает суффикс 'Z'); None при пустом/битом значении."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def fmt_dt(iso, fmt="%d.%m.%Y %H:%M"):
    """ISO-строка -> локальные дата/время для показа человеку («—», если даты нет)."""
    dt = parse_dt(iso)
    return dt.astimezone().strftime(fmt) if dt else "—"


def fmt_date(iso):
    return fmt_dt(iso, "%d.%m.%Y")


def record_label(row):
    """Подпись записи: «ДД.ММ.ГГГГ - Название - X мин.» (пункт списка и подпись к файлу)."""
    minutes = round((row.get("duration_sec") or 0) / 60)
    return RECORD_LABEL.format(date=fmt_date(row.get("created_date")),
                               title=row.get("title") or "без названия", minutes=minutes)
