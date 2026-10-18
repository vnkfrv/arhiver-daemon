"""
logsetup.py — единая настройка логов демона.

Проблема: при параллельной обработке строки разных записей/воркеров мешаются, и
непонятно, что к чему относится. Решение:
  • в КАЖДОЙ строке есть контекст: [воркер|ключ-записи] (ключ ставится на время обработки
    записи через set_record());
  • отдельный файл на каждого воркера: logs/worker_<имя-потока>.log — там строго
    последовательный лог того, что этот воркер делал;
  • общий logs/daemon.log + консоль (то же самое, для обзора).

Ошибки — одной строкой, без простыней трейсбеков: фильтр на обработчиках
сворачивает любую запись с трейсбеком (log.exception, «Exception on /action» из
Flask и т.п.) в строку «… — Тип: сообщение [файл.py:строка функция > …]». Полные
трейсбеки для отладки включает LOG_TRACEBACKS=1 в .env.

Использование:
    logsetup.setup(LOG_DIR)                # демон: один раз на старте
    logsetup.condense_tracebacks()         # бот: после своего basicConfig
    logsetup.set_record(key) / clear_record()   # вокруг обработки одной записи
"""

import logging
import os
import sys
import threading
import traceback
from logging.handlers import RotatingFileHandler

# Лимиты ротации: файл не растёт больше _MAX_BYTES, храним _BACKUPS прошлых кусков.
# Без ротации на «горе» (десятки тысяч записей) логи забьют диск.
_MAX_BYTES = 50 * 1024 * 1024        # 50 МБ на файл
_BACKUPS = 5                          # daemon.log.1 … .5
_WORKER_MAX_BYTES = 20 * 1024 * 1024  # 20 МБ на лог воркера
_WORKER_BACKUPS = 3

_ctx = threading.local()

_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
_MAX_MESSAGE = 300   # длинные сообщения исключений (тела ответов API) обрезаем


def brief(e):
    """Исключение одной строкой: тип, сообщение (≤300 симв.) и место — два последних
    кадра НАШЕГО кода (не библиотек): «[bot.py:210 _deliver > mm_client.py:103 _request]»."""
    if e is None:
        return "нет данных об ошибке"
    frames = traceback.extract_tb(e.__traceback__) if e.__traceback__ else []
    ours = [f for f in frames
            if f.filename.startswith(_PROJECT_DIR) and "site-packages" not in f.filename]
    where = (" [" + " > ".join(f"{os.path.basename(f.filename)}:{f.lineno} {f.name}" for f in ours[-2:]) + "]"
             if ours else "")
    msg = " ".join(str(e).split())
    if len(msg) > _MAX_MESSAGE:
        msg = msg[:_MAX_MESSAGE] + "…"
    return f"{type(e).__name__}: {msg}{where}" if msg else f"{type(e).__name__}{where}"


class _CondenseTracebacks(logging.Filter):
    """Сворачивает трейсбек записи в одну строку (если полные трейсбеки не включены)."""
    def __init__(self, full_tracebacks=False):
        super().__init__()
        self.full = full_tracebacks

    def filter(self, record):
        if record.exc_info and not self.full:
            try:
                text = record.getMessage()
            except Exception:  # кривые аргументы записи не должны ронять лог
                text = str(record.msg)
            record.msg = f"{text} — {brief(record.exc_info[1])}"
            record.args = None
            record.exc_info = None
            record.exc_text = None
        return True


def tracebacks_from_env():
    """LOG_TRACEBACKS из .env/окружения (тот же разбор, что у config)."""
    from talk_api import load_env, env_flag   # импорт здесь: talk_api тянет requests
    return env_flag(load_env(), "LOG_TRACEBACKS", "0")


def _install_excepthooks():
    """Необработанное исключение (в главном или фоновом потоке) — в лог, а не голым
    трейсбеком в stderr мимо логирования: так и оно сворачивается в одну строку."""
    crash = logging.getLogger("crash")

    def main_hook(exc_type, exc, tb):
        if issubclass(exc_type, KeyboardInterrupt):
            return sys.__excepthook__(exc_type, exc, tb)
        crash.critical("необработанная ошибка — процесс завершается", exc_info=(exc_type, exc, tb))

    def thread_hook(args):
        if args.exc_type is SystemExit:
            return
        crash.error("необработанная ошибка в потоке %s — поток остановлен",
                    getattr(args.thread, "name", "?"),
                    exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

    sys.excepthook = main_hook
    threading.excepthook = thread_hook


def condense_tracebacks(handlers=None, full=None):
    """Повесить на обработчики (по умолчанию — корневого логгера) фильтр «трейсбек →
    одна строка» и направить необработанные исключения в лог. full=None — взять из
    LOG_TRACEBACKS. Возвращает, включены ли полные трейсбеки."""
    filt = _CondenseTracebacks(tracebacks_from_env() if full is None else full)
    for h in (handlers if handlers is not None else logging.getLogger().handlers):
        h.addFilter(filt)
    _install_excepthooks()
    return filt.full


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

    handlers = [console, daemon]
    if per_worker:
        pw = PerWorkerFileHandler(log_dir, fmt)
        pw.addFilter(filt)
        root.addHandler(pw)
        handlers.append(pw)
    condense_tracebacks(handlers)
