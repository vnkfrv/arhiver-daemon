"""
transcribe.py — сборка расшифровки из аудио (без записи на диск).

build_transcript() качает аудио-архив записи, распознаёт КАЖДЫЙ канал (спикера)
через ASR, сливает слова по времени и группирует в реплики. Если дорожек по спикерам
в архиве нет — распознаёт сведённую дорожку целиком (запись «без дорожек»). Если Толк не
отдаёт аудиоархив вовсе, звук берётся из самой видеозаписи (тоже «без дорожек»). Возвращает
данные; сохранение файлов делает store.py, оркестрацию — pipeline.py.
"""

import logging
import os
import shutil
import tempfile
from typing import NamedTuple

from talk_api import NoAudio

log = logging.getLogger("transcribe")

# Так ffmpeg сообщает, что в файле нет звуковой дорожки (видео без звука).
_NO_SOUND = ("does not contain any stream", "matches no streams")

# Сведённая дорожка — один «спикер» на всю встречу, поэтому реплики режем сами (иначе
# получится одна строка на час речи, которую не разобрать ни человеку, ни LLM):
_PAUSE_SEC = 2.0        # пауза между словами, после которой начинаем новый абзац
_SOFT_LEN = 600         # абзац длиннее -> рвём на ближайшем конце предложения
_HARD_LEN = 1500        # предел длины абзаца, если концов предложений нет
_MIXED_SPEAKER = "?"    # кто говорит — неизвестно


class Transcript(NamedTuple):
    info: dict           # info.json из архива (имена каналов/спикеров)
    blocks: list         # [(start, end, speaker, text)] — реплики
    words: list          # [(start, end, speaker, word)] — сырые слова
    no_channels: bool    # True — дорожек по спикерам не было: общая дорожка или звук из видео


def _new_paragraph(block, start):
    """Пора ли начать новый абзац того же спикера (только для сведённой дорожки)."""
    text = block[3]
    return (start - block[1] >= _PAUSE_SEC
            or len(text) >= _HARD_LEN
            or (len(text) >= _SOFT_LEN and text.endswith((".", "!", "?", "…"))))


def build_turns(words, paragraphs=False):
    """
    Склеить подряд идущие СЛОВА одного спикера в реплику.
    words: [(start, end, speaker, word)] отсортированы по времени. Смена спикера
    по времени = новая реплика. Так диалог идёт строго по времени, а длинный
    монолог естественно рвётся там, где вклинился другой.
    paragraphs=True — дополнительно резать реплику одного спикера на абзацы по паузам и
    длине (для сведённой дорожки, где спикер один на всю запись).
    """
    turns = []
    for st, en, sp, w in words:
        if turns and turns[-1][2] == sp and not (paragraphs and _new_paragraph(turns[-1], st)):
            b = turns[-1]
            turns[-1] = (b[0], en, sp, " ".join((b[3] + " " + w).split()))
        else:
            turns.append((st, en, sp, w))
    return turns


def _mixed_files(info, work):
    """Сведённые дорожки записи по порядку: из info.json (items[].audio), иначе audio.m4a."""
    items = sorted(info.get("items") or [], key=lambda i: i.get("order") or 0)
    paths = [os.path.join(work, i["audio"]) for i in items if i.get("audio")]
    paths = [p for p in paths if os.path.exists(p)]
    if not paths:
        m4a = os.path.join(work, "audio.m4a")
        paths = [m4a] if os.path.exists(m4a) else []
    return paths


def _channel_words(info, work, asr, language):
    """Слова по дорожкам спикеров из аудиоархива: [(start, end, спикер, слово)] и сколько
    файлов дорожек реально нашлось (0 — дорожек нет, нужна общая)."""
    channels = info.get("channels") or []
    words, found = [], 0
    if channels:
        log.info("транскрибация: каналов=%d", len(channels))
    for ci, ch in enumerate(channels, 1):
        name = ch.get("name", "?")
        for f in ch.get("files", []):
            p = os.path.join(work, f["path"])
            if not os.path.exists(p):
                log.warning("  канал %d/%d: %s — файла %s нет в архиве, дорожка пропущена",
                            ci, len(channels), name, f["path"])
                continue
            found += 1
            off = float(f.get("startAt", 0))
            log.info("  канал %d/%d: %s (offset %.0fs)", ci, len(channels), name, off)
            words.extend((s, e, name, t) for s, e, t in asr.transcribe_file(p, language, off))
    return words, found


def _mixed_words(files, asr, language):
    """Слова общей (сведённой) дорожки; несколько частей идут друг за другом по шкале времени."""
    words, off = [], 0.0
    for p in files:
        part = asr.transcribe_file(p, language, off)
        words.extend((s, e, _MIXED_SPEAKER, t) for s, e, t in part)
        if part:                                 # следующая часть продолжает шкалу времени
            off = max(e for _, e, _ in part)
    return words


def _clear(work):
    """Убрать из рабочей папки всё скачанное: перед видео не держим на диске ещё и архив."""
    for name in os.listdir(work):
        p = os.path.join(work, name)
        if os.path.isdir(p):
            shutil.rmtree(p, ignore_errors=True)
        else:
            os.remove(p)


def _video_words(client, key, asr, language, work, why):
    """Звук из самой видеозаписи — когда в аудиоархиве нет пригодного аудио (why — почему).
    Видео нет или в нём нет звуковой дорожки — NoAudio; сбой связи — исключение как есть
    (стадия повторится по лестнице)."""
    path = os.path.join(work, "video.mp4")
    try:
        client.download_video(key, path)
    except Exception as ex:
        code = getattr(getattr(ex, "response", None), "status_code", None)
        if code in (400, 404):
            raise NoAudio(f"{why}; видео тоже нет (HTTP {code})") from None
        raise
    log.info("транскрибация: %s — распознаём звук из видео (%.0f МБ)",
             why, os.path.getsize(path) / 1024 / 1024)
    try:
        words = asr.transcribe_file(path, language)
    except RuntimeError as ex:
        if any(m in str(ex) for m in _NO_SOUND):
            raise NoAudio(f"{why}; в видео нет звуковой дорожки") from None
        raise
    finally:
        try:
            os.remove(path)                      # видео тяжёлое — освобождаем место сразу
        except OSError:
            pass
    return [(s, e, _MIXED_SPEAKER, t) for s, e, t in words]


def build_transcript(client, key, asr, language="ru"):
    """
    Скачать звук записи, распознать и собрать реплики. Возвращает Transcript. Источник звука —
    первый пригодный из трёх:
      1) аудиоархив Толка, дорожки по спикерам — реплики подписаны именами;
      2) тот же архив, общая (сведённая) дорожка — если дорожек по спикерам нет;
      3) сама видеозапись — если Толк не отдал аудиоархив или в нём нет ни одной дорожки.
    В 2) и 3) реплики не привязаны к участникам (no_channels=True).
    talk_api.NoAudio («запись без аудио») — если звука нет нигде (видео тоже нет или в нём нет
    звуковой дорожки) либо в звуке не распознано ни слова. Ничего не пишет на диск, кроме
    временной папки, которую сам чистит.
    """
    work = tempfile.mkdtemp(prefix=f"talk_{key}_")
    try:
        info, source, why = {}, None, None
        try:
            info, names = client.download_audio(key, work)
            info = info if isinstance(info, dict) else {}
        except NoAudio as ex:                    # Толк не отдаёт аудиоархив
            why = f"аудиоархива нет ({ex})"
        if why is None:
            words, found = _channel_words(info, work, asr, language)
            if found and words:
                source = "channels"
            else:
                if found:                        # дорожки есть, но в них ни слова — пробуем общую
                    log.warning("транскрибация: в дорожках спикеров не распознано ни слова — "
                                "пробую общую дорожку")
                mixed = _mixed_files(info, work)
                if mixed:
                    log.info("транскрибация: без дорожек по спикерам, общая дорожка (файлов: %d)",
                             len(mixed))
                    words, source = _mixed_words(mixed, asr, language), "mixed"
                elif found:
                    source = "channels"          # общей дорожки нет — итог по дорожкам (пусто)
                else:
                    why = ("в аудиоархиве нет ни дорожек по спикерам, ни общей дорожки (файлы: %s)"
                           % (", ".join(list(names)[:8]) or "нет"))
        if why is not None:
            _clear(work)
            words, source = _video_words(client, key, asr, language, work, why), "video"

        if not words:
            # САМОПРОВЕРКА: в звуке не распознано ни слова — это запись без аудио, а не пустая
            # «расшифровка», по которой потом удалили бы видео
            where = {"channels": "в дорожках спикеров", "mixed": "в общей дорожке",
                     "video": "в звуке видео"}[source]
            raise NoAudio((f"{why}; " if why else "") + f"{where} не распознано ни слова")
        words.sort(key=lambda x: x[0])           # строгий порядок по времени
        no_channels = source != "channels"
        blocks = build_turns(words, paragraphs=no_channels)
        return Transcript(dict(info, source=source), blocks, words, no_channels)
    finally:
        shutil.rmtree(work, ignore_errors=True)
