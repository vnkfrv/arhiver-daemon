"""
asr.py — КЛИЕНТ РАСПОЗНАВАНИЯ РЕЧИ (MagnitGPT_ASR).

Задача модуля: получить путь к аудиодорожке одного спикера и вернуть список СЛОВ
с таймкодами. Дальше transcribe.py из этих слов собирает диалог.

Почему именно слова, а не сегменты: у MagnitGPT_ASR сегменты нарезаны
ФИКСИРОВАННЫМИ окнами ~40 секунд (весь текст спикера за 40с сваливается в один
кусок с временем начала окна). Для восстановления порядка реплик это бесполезно.
А вот пословные таймкоды точные — по ним и выстраиваем диалог.

API — OpenAI-совместимый: POST /audio/transcriptions, multipart-файл, Bearer-ключ.
Конфиг (env/.env): MAGNITGPT_API_KEY, MAGNITGPT_BASE_URL, MAGNITGPT_MODEL,
MAGNITGPT_VERIFY_SSL, ASR_CHUNK_SEC.
"""

import glob        # найти созданные ffmpeg куски chunk_*.wav
import logging
import os
import shutil      # удалить временную папку
import subprocess  # запустить ffmpeg как внешнюю программу
import tempfile    # временная папка для конвертированного аудио
import time        # пауза между повторами (backoff)

import requests
import urllib3

from ratelimit import get_limiter
from talk_api import load_env, env_flag, env_int

log = logging.getLogger("asr")

# Сколько раз повторять запрос при ВРЕМЕННОМ сбое и с какой нарастающей паузой.
# Зачем: корп-сеть/шлюз иногда моргают; без повторов один сбой на одном куске
# ронял бы всю запись целиком.
_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF = 3.0

# Предел на конвертацию одной дорожки. Обычно это секунды; зависший на битом файле ffmpeg
# без предела занял бы воркер навсегда — запись при этом не падает и не уходит в повторы.
_FFMPEG_TIMEOUT_SEC = 3600


class MagnitASR:
    def __init__(self):
        env = load_env()
        self.key = env.get("MAGNITGPT_API_KEY") or ""
        self.base = (env.get("MAGNITGPT_BASE_URL")
                     or "https://llmlite.ai.corp.tander.ru/v1").rstrip("/")
        self.model = env.get("MAGNITGPT_MODEL") or "MagnitGPT_ASR"
        self.verify = env_flag(env, "MAGNITGPT_VERIFY_SSL", "1")
        # ASR_CHUNK_SEC — на какие по длине куски резать дорожку перед отправкой.
        # Из .env всё приходит строкой; пусто/криво -> 600, меньше 30 с не режем.
        self.chunk_sec = env_int(env, "ASR_CHUNK_SEC", 600, minimum=30)
        if not self.key:
            raise SystemExit("[!] Не задан MAGNITGPT_API_KEY (env или .env)")
        if not self.verify:
            urllib3.disable_warnings()

    # ---- один файл в API (с ретраями на временные сбои) ---------------
    def _call(self, wav_path, language, timeout=600):
        """
        Отправить ОДИН wav в ASR и вернуть распарсенный JSON-ответ.
        Ведущее подчёркивание в имени = «внутренний метод» (соглашение Python:
        снаружи класса вызывать не стоит).
        """
        last = None
        for attempt in range(_RETRY_ATTEMPTS):
            try:
                get_limiter().acquire()      # общий лимит шлюза (30/мин), считая ретраи
                # Файл открываем ВНУТРИ цикла: при повторной попытке нужен свежий
                # файловый объект (после отправки поток «прочитан до конца»).
                with open(wav_path, "rb") as f:
                    r = requests.post(
                        f"{self.base}/audio/transcriptions",
                        headers={"Authorization": f"Bearer {self.key}"},
                        # files=... делает запрос multipart/form-data (как загрузка файла)
                        files={"file": (os.path.basename(wav_path), f, "audio/wav")},
                        # data — остальные поля формы. Список кортежей, а не dict,
                        # потому что ключ timestamp_granularities[] повторяется дважды.
                        data=[("model", self.model),
                              ("response_format", "verbose_json"),   # verbose_json => есть words/segments
                              ("timestamp_granularities[]", "word"),
                              ("timestamp_granularities[]", "segment"),
                              ("language", language)],
                        verify=self.verify, timeout=timeout)
                # 4xx = наша ошибка (плохой запрос/битый файл) — повторять бессмысленно,
                # сразу бросаем исключение.
                if 400 <= r.status_code < 500:
                    r.raise_for_status()
                # 5xx = ошибка сервера, обычно временная — бросаем, чтобы попасть в retry.
                if r.status_code >= 500:
                    raise requests.HTTPError(f"{r.status_code} server error", response=r)
                return r.json()
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
                # Достаём HTTP-код, если он есть. Для 4xx — не повторяем (пробрасываем).
                code = getattr(getattr(e, "response", None), "status_code", None)
                if code and 400 <= code < 500:
                    raise
                last = e
                if attempt < _RETRY_ATTEMPTS - 1:
                    time.sleep(_RETRY_BACKOFF * (attempt + 1))   # 3с, 6с, ... — «вежливый» backoff
        raise last   # все попытки исчерпаны — пробрасываем последнюю ошибку

    # ---- дорожка (любой формат) -> СЛОВА с таймкодами -----------------
    def transcribe_file(self, path, language="ru", offset=0.0):
        """
        Главный публичный метод. Вернёт [(start, end, word), ...].

        offset — «смещение канала»: у Толка дорожка спикера начинается с секунды его
        подключения (startAt). Прибавляя offset ко всем временам, приводим таймкоды
        одного спикера к общей шкале встречи, чтобы потом корректно слить с другими.

        Шаги: (1) конвертируем в mono WAV 16кГц и режем на куски (одним вызовом
        ffmpeg); (2) каждый кусок шлём в ASR; (3) собираем слова, добавляя к их
        времени и offset канала, и i*chunk_sec — смещение куска внутри дорожки.
        """
        work = tempfile.mkdtemp(prefix="asr_")   # уникальная временная папка
        try:
            out = os.path.join(work, "chunk_%05d.wav")   # шаблон имени: ffmpeg подставит номер
            # ffmpeg: -ac 1 (моно), -ar 16000 (16кГц) — рекомендованный формат ASR;
            # -f segment + -segment_time режут выход на куски по chunk_sec секунд;
            # -reset_timestamps 1 — каждый кусок начинает отсчёт времени с нуля
            # (поэтому реальное время = i*chunk_sec + время_внутри_куска).
            # -vn — берём только звук (для видео: картинку не декодируем вовсе).
            # -nostdin + stdin=DEVNULL — ffmpeg не ждёт ввода с терминала. stderr забираем
            # себе: при сбое его последние строки уходят в текст ошибки (и в сводку админам),
            # а не теряются в выводе контейнера.
            name = os.path.basename(path)
            try:
                subprocess.run(
                    ["ffmpeg", "-nostdin", "-v", "error", "-i", path, "-vn", "-ac", "1", "-ar", "16000",
                     "-f", "segment", "-segment_time", str(self.chunk_sec),
                     "-reset_timestamps", "1", out],
                    check=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE, timeout=_FFMPEG_TIMEOUT_SEC)
            except subprocess.TimeoutExpired:
                raise RuntimeError(f"ffmpeg завис на дорожке {name} (дольше "
                                   f"{_FFMPEG_TIMEOUT_SEC // 60} мин) и был остановлен") from None
            except subprocess.CalledProcessError as ex:
                tail = (ex.stderr or b"").decode("utf-8", "replace").strip().splitlines()[-4:]
                raise RuntimeError(f"ffmpeg не смог преобразовать дорожку {name}: "
                                   + (" | ".join(tail) or f"код {ex.returncode}")) from None
            chunks = sorted(glob.glob(os.path.join(work, "chunk_*.wav")))  # sorted => по порядку 00000,00001
            # Куски дорожки шлём в ASR ПОСЛЕДОВАТЕЛЬНО (частота под лимитером внутри _call).
            # Параллелизм — на уровне ЗАПИСЕЙ: RECORD_WORKERS_ASR воркеров обрабатывают разные
            # записи одновременно (это и есть число одновременных запросов к шлюзу).
            words = []
            for i, ch in enumerate(chunks):
                base = offset + i * self.chunk_sec     # абсолютное время начала этого куска
                log.debug("  ASR кусок %d/%d", i + 1, len(chunks))
                j = self._call(ch, language)
                if not isinstance(j, dict):
                    raise RuntimeError(f"ASR вернул не объект, а {type(j).__name__}")
                jwords = j.get("words")
                if jwords:
                    for w in jwords:
                        tx = (w.get("word") or "").strip()
                        st = w.get("start")
                        # пропускаем пустые слова и слова без времени
                        if tx and st is not None:
                            en = w.get("end")               # бывает и "end": null
                            words.append((base + st, base + (st if en is None else en), tx))
                elif j.get("segments"):
                    # ОТКАТ: если модель вдруг не вернула пословные таймкоды —
                    # используем сегменты как крупные «слова», чтобы не потерять текст.
                    for s in (j.get("segments") or []):
                        tx = (s.get("text") or "").strip()
                        if tx and s.get("start") is not None:
                            en = s.get("end")
                            words.append((base + s["start"],
                                          base + (s["start"] if en is None else en), tx))
                elif (j.get("text") or "").strip():
                    # САМОПРОВЕРКА: текст есть, а таймкодов нет — формат ответа не тот. Это сбой
                    # (повторим по лестнице), а не тишина: иначе запись ушла бы в «без аудио».
                    raise RuntimeError("ASR вернул текст без таймкодов слов и фраз — "
                                       "проверь response_format шлюза")
            return words
        finally:
            # finally выполнится ВСЕГДА (даже при ошибке) — чистим временные файлы,
            # чтобы не забивать диск. ignore_errors — не падать, если уже удалено.
            shutil.rmtree(work, ignore_errors=True)


# --- ленивый синглтон -------------------------------------------------------
# Один экземпляр ASR-клиента на процесс: не хотим на каждую запись перечитывать
# .env и заново настраивать сессию. Создаём при первом обращении (лениво).
_ASR = None


def get_asr():
    global _ASR
    if _ASR is None:
        _ASR = MagnitASR()
    return _ASR
