"""
logsetup.py — единая настройка логов демона.

Проблема: при параллельной обработке строки разных записей/воркеров мешаются, и
непонятно, что к чему относится. Решение:
  • в КАЖДОЙ строке есть контекст: [воркер|ключ-записи] (ключ ставится на время обработки
    записи через set_record());
  • отдельный файл на каждого воркера: logs/worker_<имя-потока>.log — там строго
    последовательный лог того, что этот воркер делал;
  • общий logs/daemon.log + консоль (то же самое, для обзора).

Использование:
    logsetup.setup(LOG_DIR)                # один раз на старте
    logsetup.set_record(key) / clear_record()   # вокруг обработки одной записи
"""

import logging
import os
import threading
from logging.handlers import RotatingFileHandler

# Лимиты ротации: файл не растёт больше _MAX_BYTES, храним _BACKUPS прошлых кусков.
# Без ротации на «горе» (десятки тысяч записей) логи забьют диск.
_MAX_BYTES = 50 * 1024 * 1024        # 50 МБ на файл
_BACKUPS = 5                          # daemon.log.1 … .5
_WORKER_MAX_BYTES = 20 * 1024 * 1024  # 20 МБ на лог воркера
_WORKER_BACKUPS = 3

_ctx = threading.local()


def set_record(key):
    _ctx.key = key


def clear_record():
    _ctx.key = None


class _RecordFilter(logging.Filter):
    """Добавляет в каждую запись поле rec — текущий ключ записи (или '-')."""
    def filter(self, record):
        record.rec = getattr(_ctx, "key", None) or "-"
        return True


class PerWorkerFileHandler(logging.Handler):
    """Пишет лог каждого потока в отдельный файл logs/<threadName>.log. Внутри — по
    RotatingFileHandler на поток (лимит размера + backupCount), чтобы файлы не росли вечно."""
    def __init__(self, log_dir, formatter):
        super().__init__()
        self.log_dir = log_dir
        self.setFormatter(formatter)
        self._handlers = {}
        self._lock = threading.Lock()

    def _handler(self, name):
        h = self._handlers.get(name)
        if h is None:
            h = RotatingFileHandler(os.path.join(self.log_dir, f"{name}.log"),
                                    maxBytes=_WORKER_MAX_BYTES, backupCount=_WORKER_BACKUPS,
                                    encoding="utf-8")
            h.setFormatter(self.formatter)
            self._handlers[name] = h
        return h

    def emit(self, record):
        try:
            with self._lock:
                self._handler(record.threadName).emit(record)   # ротацию делает RotatingFileHandler
        except Exception:
            self.handleError(record)


def setup(log_dir, per_worker=True, level=logging.INFO):
    """Настроить корневой логгер: консоль + daemon.log + (опц.) файл на воркер. Формат
    включает [воркер|запись], чтобы строки не путались при параллельной обработке."""
    os.makedirs(log_dir, exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-5s [%(threadName)s|%(rec)s] %(name)s: %(message)s",
        "%Y-%m-%d %H:%M:%S")
    filt = _RecordFilter()
    root = logging.getLogger()
    root.setLevel(level)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.addFilter(filt)
    root.addHandler(console)

    daemon = RotatingFileHandler(os.path.join(log_dir, "daemon.log"),
                                 maxBytes=_MAX_BYTES, backupCount=_BACKUPS, encoding="utf-8")
    daemon.setFormatter(fmt)
    daemon.addFilter(filt)
    root.addHandler(daemon)

    if per_worker:
        pw = PerWorkerFileHandler(log_dir, fmt)
        pw.addFilter(filt)
        root.addHandler(pw)
