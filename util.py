"""
util.py — мелкие общие утилиты без внешних зависимостей (чтобы не дублировать по модулям).
"""

from datetime import datetime


def err(ex):
    """Короткое представление ошибки «Тип: сообщение» (обрезано) — чтобы в логах не было
    простыней трейсбеков на рутинных сбоях (таймаут Talk/шлюза и т.п.)."""
    return f"{type(ex).__name__}: {ex}"[:300]


def parse_dt(s):
    """ISO-строка -> datetime (понимает суффикс 'Z'); None при пустом/битом значении."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
