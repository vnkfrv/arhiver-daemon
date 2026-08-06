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
    """Пишет лог каждого потока в отдельный файл logs/<threadName>.log."""
    def __init__(self, log_dir, formatter):
        super().__init__()
        self.log_dir = log_dir
        self.setFormatter(formatter)
        self._streams = {}
        self._lock = threading.Lock()

    def _stream(self, name):
        s = self._streams.get(name)
        if s is None:
            s = open(os.path.join(self.log_dir, f"{name}.log"), "a", encoding="utf-8")
            self._streams[name] = s
        return s

    def emit(self, record):
        try:
            msg = self.format(record)
            with self._lock:
                s = self._stream(record.threadName)
                s.write(msg + "\n")
                s.flush()
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

    daemon = logging.FileHandler(os.path.join(log_dir, "daemon.log"), encoding="utf-8")
    daemon.setFormatter(fmt)
    daemon.addFilter(filt)
    root.addHandler(daemon)

    if per_worker:
        pw = PerWorkerFileHandler(log_dir, fmt)
        pw.addFilter(filt)
        root.addHandler(pw)
