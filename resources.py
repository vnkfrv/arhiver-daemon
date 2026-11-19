"""
resources.py — сколько занято на диске и в ОЗУ ВМ: для паузы расшифровок, тревог суперадмину
и кнопки «Память» в админ-панели. Общий для демона и бота (оба в контейнерах на одной ВМ).
"""

import os
import shutil
import tempfile

# Временные папки расшифровки (transcribe.py, asr.py): скачанный звук лежит в них до конца попытки.
TMP_PREFIXES = ("talk_", "asr_")


def _existing(p):
    """Ближайшая существующая папка на пути p (самой папки может ещё не быть)."""
    while p and not os.path.exists(p):
        parent = os.path.dirname(p)
        p = parent if parent != p else ""
    return p or "/"


def disk(cfg):
    """Самое заполненное из мест, где архиватор держит файлы (временная папка со скачанным
    звуком, Transcripts, база): (занято %, как Use% в df; занято байт; всего байт; свободно байт)."""
    worst = (0.0, 0, 0, 0)
    for p in {tempfile.gettempdir(), cfg.out_dir, os.path.dirname(os.path.abspath(cfg.db_path))}:
        try:
            u = shutil.disk_usage(_existing(p))
        except OSError:
            continue
        size = u.used + u.free                  # без резерва root — как считает df
        if size and 100.0 * u.used / size > worst[0]:
            worst = (100.0 * u.used / size, u.used, size, u.free)
    return worst


def ram():
    """ОЗУ ВМ из /proc/meminfo (в контейнере Docker там цифры самой ВМ): словарь total,
    available, used_pct, swap_total, swap_used (байты); None, если узнать нельзя."""
    try:
        with open("/proc/meminfo", encoding="ascii") as f:
            lines = f.read().splitlines()
    except OSError:
        return None
    kb = {}
    for line in lines:
        name, _, rest = line.partition(":")
        parts = rest.split()
        if parts and parts[0].isdigit():
            kb[name] = int(parts[0]) * 1024
    total, avail = kb.get("MemTotal"), kb.get("MemAvailable")
    if not total or avail is None:
        return None
    swap = kb.get("SwapTotal", 0)
    return {"total": total, "available": avail, "used_pct": 100.0 * (total - avail) / total,
            "swap_total": swap, "swap_used": swap - kb.get("SwapFree", swap)}


def tmp_dirs():
    """Сколько сейчас временных папок расшифровки (в работе + оставшиеся от прерванных попыток)."""
    try:
        return sum(n.startswith(TMP_PREFIXES) for n in os.listdir(tempfile.gettempdir()))
    except OSError:
        return 0


def gb(n):
    return f"{n / 1024 ** 3:.1f} ГБ"
