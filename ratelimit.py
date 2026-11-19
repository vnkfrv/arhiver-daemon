"""
ratelimit.py — общий ОГРАНИЧИТЕЛЬ ЧАСТОТЫ запросов к шлюзу MagnitGPT (ASR + LLM).

Не быстрее ASR_LLM_RPM запросов в минуту (скользящее окно 60с). Лимит ОБЩИЙ для ASR и LLM
(один шлюз). Применяется на КАЖДЫЙ HTTP-запрос к шлюзу, включая ретраи.

Конкурентность (сколько запросов одновременно) задаётся ЧИСЛОМ ВОРКЕРОВ фазы
(RECORD_WORKERS_ASR / RECORD_WORKERS_LLM):
воркеры обрабатывают разные записи параллельно, каждый шлёт свои куски последовательно.
"""

import collections
import threading
import time

from talk_api import load_env, env_int


class RateLimiter:
    """Не более rpm вызовов acquire() за любые 60 секунд (скользящее окно).
    Потокобезопасен: acquire() блокирует поток, пока не освободится слот."""

    def __init__(self, rpm):
        self.rpm = max(1, int(rpm))
        self._lock = threading.Lock()
        self._times = collections.deque()      # монотонные метки последних вызовов

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                while self._times and now - self._times[0] >= 60.0:
                    self._times.popleft()
                if len(self._times) < self.rpm:
                    self._times.append(now)
                    return
                wait = 60.0 - (now - self._times[0]) + 0.01
            time.sleep(min(max(wait, 0.0), 5.0))   # ждём вне лока; крупный сон дробим


_LIMITER = None
_INIT = threading.Lock()


def get_limiter():
    global _LIMITER
    if _LIMITER is None:
        with _INIT:
            if _LIMITER is None:
                _LIMITER = RateLimiter(env_int(load_env(), "ASR_LLM_RPM", 30, minimum=1))
    return _LIMITER
