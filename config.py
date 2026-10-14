"""
config.py — единая загрузка и ВАЛИДАЦИЯ конфигурации (читаем env один раз, на входе).

Обязательные переменные проверяем сразу (fail-fast с понятным списком). LLM —
необязателен: без ключа шаг «улучшения»/«выжимки» пропускается, демон вернётся к нему,
когда LLM появится.

Вместо единого MODE — ортогональные флаги (логика подхвата одинакова всегда: все
записи из API, от старых к новым; темп всегда задаётся рабочими окнами):
  REAL_DELETE  — 1: реально удаляем видео из Talk; 0: удаление ИМИТИРУЕМ (только лог).
  SEND_TO_TAG  — 1: реально шлём ЛС в Tag; 0: заглушка в лог.
  SEND_TO_MAIL — 1: дублируем уведомление на корпоративную почту; 0: не шлём.
«Сухой прогон» = все три флага в 0. Рабочие окна (TIME_*_ASR / TIME_*_LLM) ОБЯЗАТЕЛЬНЫ —
без них демон не стартует.
"""

import os
from dataclasses import dataclass, field
from datetime import timedelta

from talk_api import load_env, env_flag, env_int, require_env

HERE = os.path.dirname(os.path.abspath(__file__))

# Задержки удаления по умолчанию (можно переопределить в .env).
_TEST_DELETE_DELAY_SEC = 3 * 60            # test: «через 3 минуты»
_PROD_DELETE_DELAY_SEC = 7 * 24 * 3600     # prod: «через неделю»

# Лестница повторов упавшей стадии (минуты ОТ ПЕРВОЙ ОШИБКИ): повторы через 5 мин, 30 мин,
# 180 мин и 23 ч после неё. Не помог и последний — «до решения админа» (см. pipeline).
_RETRY_DELAYS_MIN = (5, 30, 180, 23 * 60)


@dataclass
class MMCfg:
    """Настройки Tag/Mattermost. На Этапе A могут быть пустыми."""
    url: str = ""
    scheme: str = "https"
    port: int = 443
    bot_token: str = ""            # основной бот: уведомления участникам, скачивание расшифровок
    admin_bot_token: str = ""      # админ-бот: панель, команды, сводки (пусто -> всё в основном)
    bot_public_url: str = ""
    button_port: int = 8081
    button_secret: str = ""
    verify: bool = True


@dataclass
class Cfg:
    out_dir: str            # куда складывать папки записей (Transcripts/)
    db_path: str            # SQLite-файл витрины записей
    language: str           # язык распознавания
    delete_video_age_days: int  # видео старше N дней -> уведомить и удалить (по ТЗ — год)
    text_retention_days: int    # сколько хранить тексты после удаления видео; None -> НЕ удалять
    min_duration: int           # короче N секунд -> не транскрибируем, удаляем при подхвате
    retry_delays_sec: tuple     # лестница повторов упавшей стадии (сек от первой ошибки)
    admin_report_time: tuple    # (ч, м): когда слать админам суточную сводку об остановленных
    admin_change_delay_sec: int  # период охлаждения решений обычного админа о составе админов
    shrink_warn_pct: int        # LLM-версия короче ASR более чем на N% -> баннер в _LLM.md
    delete_delay_sec: int   # через сколько после рассылки удалять видео
    real_delete: bool       # REAL_DELETE=1 -> реально удалять видео; 0 -> имитация (лог)
    simulate_delete: bool   # = not real_delete (удаление только логируем, файл не трогаем)
    force_delete: bool      # удалять даже защищённые Толком (immutable)
    send_to_tag: bool       # реально слать ЛС в Tag (иначе notify — только лог)
    send_to_mail: bool      # дублировать уведомление участнику на корпоративную почту (SMTP)
    admin_username: str     # bootstrap-админ (заносится в таблицу admins при старте)
    asr_window: tuple       # окно транскрибации ((ч,м),(ч,м)) — ОБЯЗАТЕЛЬНО
    llm_window: tuple       # окно LLM-обработки (improve/summarize/…) — ОБЯЗАТЕЛЬНО
    record_workers: int     # сколько записей обрабатывать параллельно (воркеров)
    mm: MMCfg = field(default_factory=MMCfg)


def _load_mm(env):
    bot_token = (env.get("BOT_TOKEN") or "").strip()
    admin_token = (env.get("ADMIN_BOT_TOKEN") or "").strip()
    return MMCfg(
        url=env.get("URL", ""),
        scheme=env.get("SCHEME", "https"),
        port=env_int(env, "PORT", 443, minimum=1),
        bot_token=bot_token,
        # тот же токен, что у основного бота, — это НЕ второй бот: иначе одна учётка слушала
        # бы личку дважды и отвечала на каждое сообщение и меню, и панелью
        admin_bot_token="" if admin_token == bot_token else admin_token,
        bot_public_url=env.get("BOT_PUBLIC_URL", ""),
        button_port=env_int(env, "BUTTON_PORT", 8081, minimum=1),
        button_secret=env.get("BUTTON_SECRET", ""),
        verify=env_flag(env, "MM_VERIFY_SSL", "1"),
    )


def _parse_hhmm(s):
    """'ЧЧ:ММ' -> (час, минута); None — если пусто. «24:00» понимаем как полночь (00:00).
    Некорректное время — ValueError: молча считать его «не заданным» нельзя, а час 24 или
    минута 60 позже уронили бы расчёт границ окна."""
    s = (s or "").strip()
    if not s:
        return None
    hh, mm = (int(x) for x in s.split(":"))     # не два числа через «:» -> ValueError
    if (hh, mm) == (24, 0):
        return 0, 0
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        raise ValueError(s)
    return hh, mm


def _time(env, key):
    """Время ЧЧ:ММ из env: None — если не задано; кривое значение — понятная ошибка на старте."""
    raw = (env.get(key) or "").strip()
    try:
        return _parse_hhmm(raw)
    except ValueError:
        raise SystemExit(f"[!] {key}={raw!r}: ожидается время в формате ЧЧ:ММ (от 00:00 до "
                         "23:59). Исправь .env и повтори.")


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
    s = _time(env, f"TIME_START_{prefix}") or fb_start
    e = _time(env, f"TIME_END_{prefix}") or fb_end
    return (s, e) if (s and e) else None


def _strict_int(env, key):
    """Целое неотрицательное число из env для настроек, где молча подставить значение по
    умолчанию нельзя (сроки удаления видео и текстов). Пусто -> None; кривое или
    отрицательное значение — понятная ошибка на старте, а не трейсбек."""
    raw = (env.get(key) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError:
        value = -1
    if value < 0:
        raise SystemExit(f"[!] {key}={raw!r}: ожидается целое число не меньше 0. "
                         "Исправь .env и повтори.")
    return value


def _hours(env, key, default):
    """Срок в часах из env (можно дробный — удобно в тесте) -> секунды. Пусто/криво/меньше
    нуля -> default часов."""
    try:
        hours = float((env.get(key) or "").strip() or default)
    except ValueError:
        hours = default
    return int((hours if hours >= 0 else default) * 3600)


def _retry_delays(env):
    """Лестница повторов из RETRY_DELAYS_MIN («5,30,180,1380» — минуты от первой ошибки;
    можно дробные — удобно в тесте). Пусто/криво -> значения по умолчанию."""
    raw = (env.get("RETRY_DELAYS_MIN") or "").strip()
    try:
        mins = [float(x) for x in raw.split(",") if x.strip()]
    except ValueError:
        mins = []
    if not mins or min(mins) < 0:
        mins = _RETRY_DELAYS_MIN
    return tuple(sorted({int(m * 60) for m in mins}))


def load_cfg():
    env = load_env()
    require_env(env, "TALK_BASE_URL", "TALK_API_TOKEN",
                "MAGNITGPT_API_KEY", "MAGNITGPT_BASE_URL", "MAGNITGPT_MODEL")

    # REAL_DELETE=1 -> реально удаляем видео из Talk; 0 -> имитация (только лог).
    real_delete = env_flag(env, "REAL_DELETE", "0")

    default_delay = _PROD_DELETE_DELAY_SEC if real_delete else _TEST_DELETE_DELAY_SEC
    # DELETE_AFTER_NOTIFICATION (в секундах) — через сколько после ЛС удалять видео.
    # Фоллбэк на старое имя DELETE_DELAY_SEC; пусто/не задано -> дефолт (real=неделя / имит.=3мин).
    delete_delay_sec = _strict_int(env, "DELETE_AFTER_NOTIFICATION")
    if delete_delay_sec is None:
        delete_delay_sec = _strict_int(env, "DELETE_DELAY_SEC")
    if delete_delay_sec is None:
        delete_delay_sec = default_delay

    # TEXT_RETENTION_DAYS не задан (пусто) -> тексты НЕ удаляются никогда (None).
    text_retention_days = _strict_int(env, "TEXT_RETENTION_DAYS")

    # Рабочие окна ОБЯЗАТЕЛЬНЫ. Фоллбэк — общие TIME_START/END. Если ни специфичные,
    # ни общие не заданы -> окно = None, и демон не стартует (fail-fast ниже).
    common = _time(env, "TIME_START"), _time(env, "TIME_END")
    asr_window = _window(env, "ASR", *common)
    llm_window = _window(env, "LLM", *common)
    missing = [n for n, w in (("ASR", asr_window), ("LLM", llm_window)) if not w]
    if missing:
        raise SystemExit(
            "[!] Не заданы рабочие окна: " + ", ".join(missing) + ". Демон работает ТОЛЬКО "
            "по временным окнам — задай TIME_START_ASR/TIME_END_ASR и TIME_START_LLM/"
            "TIME_END_LLM (или общие TIME_START/TIME_END) в .env и повтори.")

    return Cfg(
        out_dir=os.path.join(HERE, "Transcripts"),
        db_path=env.get("DB_PATH") or os.path.join(HERE, "data", "recordings.db"),
        language=env.get("ASR_LANGUAGE", "ru"),
        delete_video_age_days=env_int(env, "DELETE_VIDEO_AGE_DAYS", 365),
        text_retention_days=text_retention_days,
        min_duration=env_int(env, "MIN_DURATION", 60),
        retry_delays_sec=_retry_delays(env),
        admin_report_time=_time(env, "ADMIN_REPORT_TIME") or (9, 0),
        admin_change_delay_sec=_hours(env, "ADMIN_CHANGE_DELAY_HOURS", 24),
        shrink_warn_pct=env_int(env, "LLM_SHRINK_WARN_PCT", 5),
        delete_delay_sec=delete_delay_sec,
        real_delete=real_delete,
        simulate_delete=not real_delete,
        force_delete=env_flag(env, "FORCE_DELETE", "0"),
        # Каналы уведомления: реальная рассылка в Tag / на почту (иначе — заглушка в лог).
        send_to_tag=env_flag(env, "SEND_TO_TAG", "0"),
        send_to_mail=env_flag(env, "SEND_TO_MAIL", "0"),
        # ник в Tag — строчными и без «@»: именно так он приходит от Tag при проверке прав
        admin_username=(env.get("ADMIN_USERNAME") or "").strip().lstrip("@").lower(),
        asr_window=asr_window,
        llm_window=llm_window,
        record_workers=max(1, env_int(env, "RECORD_WORKERS", 4)),
        mm=_load_mm(env),
    )
