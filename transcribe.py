"""
transcribe.py — сборка расшифровки из аудио (без записи на диск).

build_transcript() качает аудио-архив записи, распознаёт КАЖДЫЙ канал (спикера)
через ASR, сливает слова по времени и группирует в реплики. Если дорожек по спикерам
в архиве нет — распознаёт сведённую дорожку целиком (запись «без дорожек»). Возвращает
данные; сохранение файлов делает store.py, оркестрацию — pipeline.py.
"""

import logging
import os
import shutil
import tempfile
from typing import NamedTuple

log = logging.getLogger("transcribe")

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
    no_channels: bool    # True — дорожек по спикерам не было, распознана сведённая


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


def build_transcript(client, key, asr, language="ru"):
    """
    Скачать аудио записи, распознать и собрать реплики. Возвращает Transcript.
    Ничего не пишет на диск (кроме временной папки, которую сам чистит).
    Нет аудио вовсе -> talk_api.NoAudio (бросает клиент Толка).
    """
    work = tempfile.mkdtemp(prefix=f"talk_{key}_")
    try:
        info, names = client.download_audio(key, work)
        channels = info.get("channels") or []
        all_words = []
        found = 0                                # сколько файлов дорожек реально есть в архиве

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
                for s, e, t in asr.transcribe_file(p, language, off):
                    all_words.append((s, e, name, t))

        no_channels = found == 0
        if no_channels:
            # дорожек по спикерам нет (или они пустые) — распознаём сведённую дорожку
            mixed = _mixed_files(info, work)
            if not mixed:
                # САМОПРОВЕРКА: аудио в архиве не нашли вовсе. Это не «тишина», а сбой
                # (неполный архив, другая раскладка файлов): пустая «расшифровка» прошла бы
                # по конвейеру как настоящая, и видео в свой срок было бы удалено без текста.
                raise RuntimeError("в аудио-архиве нет ни дорожек по спикерам, ни сведённой "
                                   "дорожки (файлы: %s)" % (", ".join(list(names)[:8]) or "нет"))
            log.info("транскрибация: без дорожек по спикерам, сведённых файлов=%d", len(mixed))
            off = 0.0
            for p in mixed:
                part = asr.transcribe_file(p, language, off)
                all_words.extend((s, e, _MIXED_SPEAKER, t) for s, e, t in part)
                if part:                         # следующая часть продолжает шкалу времени
                    off = max(e for _, e, _ in part)

        if not all_words:
            log.warning("транскрибация: в записи не распознано ни одного слова — "
                        "расшифровка будет пустой")
        all_words.sort(key=lambda x: x[0])       # строгий порядок по времени
        blocks = build_turns(all_words, paragraphs=no_channels)
        return Transcript(info, blocks, all_words, no_channels)
    finally:
        shutil.rmtree(work, ignore_errors=True)
