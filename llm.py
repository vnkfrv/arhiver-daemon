"""
llm.py — КЛИЕНТ ЧАТ-МОДЕЛИ MagnitGPT (OpenAI-совместимый /chat/completions).

Отдельный клиент от ASR: у ASR-ключа доступа к чат-модели нет, поэтому здесь свои
переменные (MAGNITGPT_LLM_*). Модуль максимально тонкий: умеет только «отправить
system+user промт и вернуть текст ответа». Что именно просить у модели — задаёт
enrich.py.

Конфиг (env/.env):
    MAGNITGPT_LLM_API_KEY   — ключ чат-модели
    MAGNITGPT_LLM_MODEL     — имя чат-модели (напр. MagnitGPT)
    MAGNITGPT_LLM_BASE_URL  — по умолч. как у ASR (…/v1)
    MAGNITGPT_VERIFY_SSL    — 1/0 (общий с ASR)
"""

import time

import requests
import urllib3

from ratelimit import get_limiter
from talk_api import load_env, env_flag

_RETRY_ATTEMPTS = 3      # логика ретраев такая же, как в asr._call (см. комментарии там)
_RETRY_BACKOFF = 3.0


class MagnitLLM:
    def __init__(self):
        env = load_env()
        self.key = env.get("MAGNITGPT_LLM_API_KEY") or ""
        # Базовый URL: сначала спец-переменная LLM, иначе тот же шлюз, что у ASR.
        self.base = (env.get("MAGNITGPT_LLM_BASE_URL")
                     or env.get("MAGNITGPT_BASE_URL")
                     or "https://llmlite.ai.corp.tander.ru/v1").rstrip("/")
        self.model = env.get("MAGNITGPT_LLM_MODEL") or ""
        self.verify = env_flag(env, "MAGNITGPT_VERIFY_SSL", "1")
        # Ризонинг («мышление» модели) для наших задач (чистка/выжимка) НЕ нужен — только
        # лишние токены и задержка. По умолчанию выключаем; LLM_THINKING=1 — включить.
        self.thinking = env_flag(env, "LLM_THINKING", "0")
        if not self.verify:
            urllib3.disable_warnings()
        # ВАЖНО: тут НЕ бросаем ошибку при пустом ключе (в отличие от ASR). LLM-шаг
        # необязательный, поэтому просто даём метод available() ниже, а решение
        # «делать или пропустить» принимает вызывающий код (maybe_enrich).

    def available(self) -> bool:
        """Настроен ли LLM (есть ключ и модель). Если нет — LLM-шаг пропускается."""
        return bool(self.key and self.model)

    def chat(self, system: str, user: str, temperature: float = 0.2,
             timeout: int = 600) -> str:
        """
        Отправить одно сообщение в чат-модель и вернуть текст ответа.
        system — инструкция («кто ты и что делаешь»), user — сам текст на обработку.
        temperature=0.2 — низкая «креативность»: для редактуры нам нужна
        предсказуемость, а не фантазия модели.
        """
        last = None
        for attempt in range(_RETRY_ATTEMPTS):
            try:
                get_limiter().acquire()      # общий лимит шлюза (30/мин), считая ретраи
                r = requests.post(
                    f"{self.base}/chat/completions",
                    headers={"Authorization": f"Bearer {self.key}",
                             "Content-Type": "application/json"},
                    # json=... сам сериализует dict и ставит правильный Content-Type.
                    json={"model": self.model,
                          "messages": [{"role": "system", "content": system},
                                       {"role": "user", "content": user}],
                          "temperature": temperature,
                          "stream": False,
                          # отключаем «мышление» модели (быстрее, без лишних токенов)
                          "chat_template_kwargs": {"thinking": self.thinking}},
                    verify=self.verify, timeout=timeout)
                if 400 <= r.status_code < 500:      # клиентская ошибка — не повторяем
                    r.raise_for_status()
                if r.status_code >= 500:            # серверная — в retry
                    raise requests.HTTPError(f"{r.status_code} server error", response=r)
                data = r.json()
                # Стандартная форма ответа OpenAI-совместимого API.
                return data["choices"][0]["message"]["content"]
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
                code = getattr(getattr(e, "response", None), "status_code", None)
                if code and 400 <= code < 500:
                    raise
                last = e
                if attempt < _RETRY_ATTEMPTS - 1:
                    time.sleep(_RETRY_BACKOFF * (attempt + 1))
        raise last


_LLM = None      # ленивый синглтон (как и ASR)


def get_llm():
    global _LLM
    if _LLM is None:
        _LLM = MagnitLLM()
    return _LLM
