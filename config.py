"""
config.py — единая загрузка и ВАЛИДАЦИЯ конфигурации (читаем env один раз, на входе).

Обязательные переменные проверяем сразу (fail-fast с понятным списком). LLM —
необязателен: без ключа шаг «улучшения»/«выжимки» пропускается, демон вернётся к нему,
когда LLM появится.

РЕЖИМЫ (MODE): различаются ТОЛЬКО удалением и темпом — логика подхвата одинакова
(все записи из API, от старых к новым).
  prod — раз в сутки (в RUN_AT, по умолч. 00:01) транскрибируем записи ПРОШЛОГО дня
         (текст доступен на следующий день); видео старше года — уведомляем участников
         и РЕАЛЬНО удаляем через неделю. Тексты храним 5 лет после удаления видео.
  test — то же самое, но удаление ИМИТИРУЕМ (только лог, видео не трогаем), задержка
         короче (по умолч. 180с) и цикл частый (TEST_CYCLE_SEC) — «сухой прогон» прода.
"""

import os
from dataclasses import dataclass, field
from datetime import timedelta

from talk_api import load_env, env_flag, require_env

HERE = os.path.dirname(os.path.abspath(__file__))

# Задержки удаления по умолчанию (можно переопределить в .env).
_TEST_DELETE_DELAY_SEC = 3 * 60            # test: «через 3 минуты»
_PROD_DELETE_DELAY_SEC = 7 * 24 * 3600     # prod: «через неделю»


@dataclass
class MMCfg:
    """Настройки Tag/Mattermost. На Этапе A могут быть пустыми."""
    url: str = ""
    scheme: str = "https"
    port: int = 443
    bot_token: str = ""
    bot_public_url: str = ""
    button_port: int = 8081
    button_secret: str = ""
    verify: bool = True


@dataclass
class Cfg:
    mode: str               # "test" | "prod"
    out_dir: str            # куда складывать папки записей (Transcripts/)
    db_path: str            # SQLite-файл витрины записей
    language: str           # язык распознавания
    delete_video_age_days: int  # видео старше N дней -> уведомить и удалить (по ТЗ — год)
    text_retention_days: int    # сколько хранить тексты после удаления видео (5 лет)
    min_duration: int           # короче N секунд -> не транскрибируем, удаляем при подхвате
    emergency_hours: int        # если обработка «залипла» дольше N часов -> алерт админам
    cycle_sec: float        # период «тика» в test-режиме (секунды)
    run_at: tuple           # prod: время суточного запуска (час, минута)
    delete_delay_sec: int   # через сколько после рассылки удалять видео
    simulate_delete: bool   # true (test) -> удаление только логируем, файл не трогаем
    force_delete: bool      # удалять даже защищённые Толком (immutable)
    send_to_tag: bool       # реально слать ЛС в Tag (иначе notify — только лог)
    admin_username: str     # bootstrap-админ (заносится в таблицу admins при старте)
    asr_window: tuple       # окно транскрибации ((ч,м),(ч,м)) или None — работать всегда
    llm_window: tuple       # окно LLM-обработки (improve/summarize/…) или None
    record_workers: int     # сколько записей обрабатывать параллельно (воркеров)
    mm: MMCfg = field(default_factory=MMCfg)


def _load_mm(env):
    return MMCfg(
        url=env.get("URL", ""),
        scheme=env.get("SCHEME", "https"),
        port=int(env.get("PORT", "443") or "443"),
        bot_token=env.get("BOT_TOKEN", ""),
        bot_public_url=env.get("BOT_PUBLIC_URL", ""),
        button_port=int(env.get("BUTTON_PORT", "8081") or "8081"),
        button_secret=env.get("BUTTON_SECRET", ""),
        verify=env_flag(env, "MM_VERIFY_SSL", "1"),
    )


def _parse_run_at(s):
    """'HH:MM' -> (час, минута). По умолчанию 00:01."""
    try:
        hh, mm = (s or "00:01").split(":")
        return int(hh), int(mm)
    except (ValueError, AttributeError):
        return 0, 1


def _parse_hhmm(s):
    """'HH:MM' -> (час, минута) или None, если пусто/некорректно (окно не задано)."""
    if not s or ":" not in s:
        return None
    try:
        hh, mm = s.split(":")
        return int(hh), int(mm)
    except ValueError:
        return None


# ---- утилиты рабочих окон (используются и демоном, и пайплайном) -----------

def in_window(now_local, window):
    """now (local datetime) внутри окна window=((ч,м),(ч,м))? None -> всегда True.
    Окно может идти через полночь (напр. 22:00–06:00)."""
    if not window:
        return True
    (sh, sm), (eh, em) = window
    cur = now_local.hour * 60 + now_local.minute
    s, e = sh * 60 + sm, eh * 60 + em
    if s == e:
        return True
    if s < e:
        return s <= cur < e
    return cur >= s or cur < e


def next_open_local(now_local, window):
    """Ближайший момент ОТКРЫТИЯ окна (local datetime). None, если окна нет (всегда
    открыто) или оно уже открыто сейчас."""
    if not window or in_window(now_local, window):
        return None
    (sh, sm), _ = window
    t = now_local.replace(hour=sh, minute=sm, second=0, microsecond=0)
    if t <= now_local:
        t += timedelta(days=1)
    return t


def _window(env, prefix, fb_start, fb_end):
    """Окно из TIME_START_<prefix>/TIME_END_<prefix>, с фоллбэком на общие TIME_START/END."""
    s = _parse_hhmm(env.get(f"TIME_START_{prefix}", "")) or fb_start
    e = _parse_hhmm(env.get(f"TIME_END_{prefix}", "")) or fb_end
    return (s, e) if (s and e) else None


def load_cfg():
    env = load_env()
    require_env(env, "TALK_BASE_URL", "TALK_API_TOKEN",
                "MAGNITGPT_API_KEY", "MAGNITGPT_BASE_URL", "MAGNITGPT_MODEL")

    mode = (env.get("MODE", "test") or "test").strip().lower()
    is_test = mode != "prod"

    default_delay = _TEST_DELETE_DELAY_SEC if is_test else _PROD_DELETE_DELAY_SEC
    delete_delay_sec = int(env.get("DELETE_DELAY_SEC", str(default_delay)))

    return Cfg(
        mode="test" if is_test else "prod",
        out_dir=os.path.join(HERE, "Transcripts"),
        db_path=env.get("DB_PATH") or os.path.join(HERE, "data", "recordings.db"),
        language=env.get("ASR_LANGUAGE", "ru"),
        delete_video_age_days=int(env.get("DELETE_VIDEO_AGE_DAYS", "365")),
        text_retention_days=int(env.get("TEXT_RETENTION_DAYS", str(5 * 365))),
        min_duration=int(env.get("MIN_DURATION", "60")),
        emergency_hours=int(env.get("EMERGENCY", "24")),
        cycle_sec=float(env.get("TEST_CYCLE_SEC", "60")),
        run_at=_parse_run_at(env.get("RUN_AT", "00:01")),
        delete_delay_sec=delete_delay_sec,
        simulate_delete=is_test,
        force_delete=env_flag(env, "FORCE_DELETE", "0"),
        # Безопасный выключатель: 0 — рассылка в Tag только логируется (заглушка).
        send_to_tag=env_flag(env, "SEND_TO_TAG", "0"),
        admin_username=env.get("ADMIN_USERNAME", "").strip(),
        # Раздельные окна: транскрибация (ASR) — обычно шире, LLM-обработка — уже.
        # TIME_START_ASR/END_ASR и TIME_START_LLM/END_LLM; фоллбэк — общие TIME_START/END.
        # Работают в ОБОИХ режимах (test и prod). Все пустые -> работать всегда (без окон).
        asr_window=_window(env, "ASR", _parse_hhmm(env.get("TIME_START", "")),
                           _parse_hhmm(env.get("TIME_END", ""))),
        llm_window=_window(env, "LLM", _parse_hhmm(env.get("TIME_START", "")),
                           _parse_hhmm(env.get("TIME_END", ""))),
        record_workers=max(1, int(env.get("RECORD_WORKERS", "4"))),
        mm=_load_mm(env),
    )
