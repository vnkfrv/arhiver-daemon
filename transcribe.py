"""
transcribe.py — сборка расшифровки из аудио (без записи на диск).

build_transcript() качает аудио-архив записи, распознаёт КАЖДЫЙ канал (спикера)
через ASR, сливает слова по времени и группирует в реплики. Возвращает данные;
сохранение файлов делает store.py, оркестрацию — pipeline.py.
"""

import logging
import os
import shutil
import tempfile

log = logging.getLogger("transcribe")


def build_turns(words):
    """
    Склеить подряд идущие СЛОВА одного спикера в реплику.
    words: [(start, end, speaker, word)] отсортированы по времени. Смена спикера
    по времени = новая реплика. Так диалог идёт строго по времени, а длинный
    монолог естественно рвётся там, где вклинился другой.
    """
    turns = []
    for st, en, sp, w in words:
        if turns and turns[-1][2] == sp:
            b = turns[-1]
            turns[-1] = (b[0], en, sp, " ".join((b[3] + " " + w).split()))
        else:
            turns.append((st, en, sp, w))
    return turns


def build_transcript(client, key, asr, language="ru"):
    """
    Скачать аудио записи, распознать по каналам и собрать реплики.
    Возвращает (audio_info, blocks, words):
      audio_info — info.json из архива (нужны имена каналов/спикеров);
      blocks     — [(start, end, speaker, text)] реплики;
      words      — [(start, end, speaker, word)]  сырые слова.
    Ничего не пишет на диск (кроме временной папки, которую сам чистит).
    """
    work = tempfile.mkdtemp(prefix=f"talk_{key}_")
    try:
        info, _ = client.download_audio(key, work)
        channels = info.get("channels", [])
        all_words = []

        if channels:
            log.info("транскрибация: каналов=%d", len(channels))
            for ci, ch in enumerate(channels, 1):
                name = ch.get("name", "?")
                for f in ch.get("files", []):
                    p = os.path.join(work, f["path"])
                    if not os.path.exists(p):
                        continue
                    off = float(f.get("startAt", 0))
                    log.info("  канал %d/%d: %s (offset %.0fs)", ci, len(channels), name, off)
                    for s, e, t in asr.transcribe_file(p, language, off):
                        all_words.append((s, e, name, t))
        else:
            m4a = os.path.join(work, "audio.m4a")
            if os.path.exists(m4a):
                log.info("транскрибация: без каналов, сведённый audio.m4a")
                for s, e, t in asr.transcribe_file(m4a, language):
                    all_words.append((s, e, "?", t))

        all_words.sort(key=lambda x: x[0])       # строгий порядок по времени
        blocks = build_turns(all_words)
        return info, blocks, all_words
    finally:
        shutil.rmtree(work, ignore_errors=True)
