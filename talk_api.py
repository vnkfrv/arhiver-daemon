"""
talk_api.py — КЛИЕНТ к API записей Контур.Толка + общие утилиты проекта.

Это "фундамент": почти все остальные модули импортируют отсюда конфиг (load_env),
форматирование времени (hhmmss) и сам клиент (TalkClient).

Что делает клиент:
  - перечисляет записи пространства (с фильтром по дате) — метод list();
  - отдаёт детали записи и участников с почтами — recording_detail()/participants_with_emails();
  - скачивает аудио-архив и видео записи — download_audio()/download_video().

Конфиг берётся из файла .env (см. .env.example): TALK_BASE_URL, TALK_API_TOKEN,
TALK_VERIFY_SSL.
"""

import json
import logging
import os
import re          # срезать инлайн-комментарии в .env
import zipfile     # аудио-архив приходит как ZIP
from urllib.parse import quote

import requests    # HTTP-клиент
import urllib3     # нужен только чтобы гасить предупреждение при verify=False

log = logging.getLogger("talk")

# Полный список переменных конфигурации проекта. Используется в load_env, чтобы
# знать, какие ключи разрешено переопределять через окружение (export VAR=...),
# даже если их нет в .env-файле.
_KNOWN_KEYS = (
    "TALK_BASE_URL", "TALK_API_TOKEN", "TALK_VERIFY_SSL",
    "MAGNITGPT_API_KEY", "MAGNITGPT_BASE_URL", "MAGNITGPT_MODEL", "MAGNITGPT_VERIFY_SSL",
    "MAGNITGPT_LLM_API_KEY", "MAGNITGPT_LLM_MODEL", "MAGNITGPT_LLM_BASE_URL", "LLM_THINKING",
    "ASR_CHUNK_SEC", "LLM_MAX_CHARS", "LLM_MAX_TOKENS", "LLM_TEMPERATURE", "LLM_SHRINK_WARN_PCT",
    # флаги/демон/БД
    "REAL_DELETE", "TIME_START", "TIME_END",
    "TIME_START_ASR", "TIME_END_ASR", "TIME_START_LLM", "TIME_END_LLM",
    "RECORD_WORKERS", "DELETE_DELAY_SEC", "DELETE_AFTER_NOTIFICATION",
    "DELETE_VIDEO_AGE_DAYS", "TEXT_RETENTION_DAYS", "MIN_DURATION", "RETRY_DELAYS_MIN",
    "ADMIN_REPORT_TIME", "ADMIN_CHANGE_DELAY_HOURS",
    "ASR_LANGUAGE", "FORCE_DELETE", "TALK_VIDEO_QUALITY",
    "DB_PATH", "ADMIN_USERNAME", "ASR_LLM_RPM",
    # Tag/Mattermost
    "URL", "SCHEME", "PORT", "MM_VERIFY_SSL", "BOT_TOKEN", "ADMIN_BOT_TOKEN",
    "BOT_PUBLIC_URL", "BUTTON_PORT", "BUTTON_SECRET", "SEND_TO_TAG",
    # Почта (SMTP)
    "SEND_TO_MAIL", "SMTP_SERVER", "SMTP_PORT", "SMTP_SECURITY",
    "SMTP_SENDER", "SMTP_LOGIN", "SMTP_PASSWORD",
    # Логи: 1 — полные трейсбеки (по умолчанию ошибка пишется одной строкой)
    "LOG_TRACEBACKS",
)


def load_env(path=".env"):
    """
    Простой парсер .env (свой, чтобы не тащить зависимость python-dotenv).

    ЗАЧЕМ так: секреты (ключи API) нельзя хранить в коде/git. Их держим в .env,
    который лежит в .gitignore. Функция читает KEY=VALUE построчно.

    Приоритет: сначала берём значения из .env-файла, затем ПЕРЕЗАПИСЫВАЕМ их
    переменными окружения (если заданы). Это удобно для запусков вида
    `MAGNITGPT_VERIFY_SSL=0 python ...` — окружение важнее файла.
    """
    env = {}
    # __file__ — путь к ЭТОМУ файлу; ищем .env рядом с ним, а не в текущей папке,
    # чтобы скрипт работал из любого каталога (например, из cron).
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:       # кодировка явно: не зависим от локали хоста
            for line in f:
                line = line.strip()
                # пропускаем пустые строки и комментарии (#...)
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)          # split с maxsplit=1: значение может содержать "="
                    v = re.sub(r"\s+#.*$", "", v)       # срезаем инлайн-комментарий « #...» (с пробелом перед #)
                    env.setdefault(k.strip(), v.strip())  # setdefault: первая запись побеждает
    # Наложение окружения поверх файла. Берём объединение ключей из файла и известных.
    for k in set(list(env) + list(_KNOWN_KEYS)):
        if os.getenv(k) is not None:               # именно "is not None": пустая строка тоже валидна
            env[k] = os.getenv(k)
    return env


def env_flag(env, key, default="1"):
    """
    Превратить строковое значение флага (1/0/true/false/yes/no) в bool.
    Нужно, потому что в .env всё — строки, а нам нужен настоящий True/False,
    например для verify SSL.
    """
    return str(env.get(key, default)).strip().lower() not in ("0", "false", "no", "")


def env_int(env, key, default, minimum=None):
    """
    Целое число из env. Пусто или не число -> default (а не падение на int('') посреди
    работы); minimum — нижняя граница значения.
    """
    try:
        value = int(str(env.get(key) or "").strip() or default)
    except (TypeError, ValueError):
        value = int(default)
    return value if minimum is None else max(minimum, value)


def require_env(env, *keys):
    """
    Проверить, что ОБЯЗАТЕЛЬНЫЕ переменные заданы (непустые). Если чего-то нет —
    остановиться с понятным списком ВСЕХ недостающих (а не падать позже с непонятной
    ошибкой). Так конфиг не «хардкодится» в коде, а честно требуется из .env.
    """
    missing = [k for k in keys if not str(env.get(k) or "").strip()]
    if missing:
        raise SystemExit(
            "[!] В .env не заданы обязательные переменные: " + ", ".join(missing)
            + "\n    Заполни их (образец — .env.example) и повтори."
        )


def hhmmss(sec):
    """Секунды (число) -> строка 'ЧЧ:ММ:СС'. Используется для таймкодов в расшифровке."""
    sec = int(sec)     # таймкоды приходят float (85.32) -> для // и % нужно целое
    return f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"


# Участники записи: Толк отдаёт их страницами не больше 100; предохранитель от бесконечной пагинации.
_PARTICIPANTS_PAGE = 100
_PARTICIPANTS_MAX_PAGES = 100


def _participant_id(p):
    """Чем участники различаются между страницами: login, а у анонимов — их id или имя."""
    ui = p.get("userInfo") or {}
    return ui.get("login") or p.get("anonymousId") or p.get("anonymousName") or id(p)


class NoAudio(Exception):
    """Толк сообщил, что у записи НЕТ аудио — транскрибировать нечего (терминально).
    Отдельный тип, чтобы не путать с 4xx от шлюза ASR: тот — сбой, а не «нет звука»."""


def _talk_error(body):
    """Текст ошибки из ответа Толка ({"errorMessage": "..."}) или '' — если это не он."""
    try:
        data = json.loads(body)
    except (TypeError, ValueError):
        return ""
    return str(data.get("errorMessage") or "") if isinstance(data, dict) else ""


class TalkClient:
    """
    Обёртка над HTTP-сессией к API Толка. Один экземпляр = одна настроенная сессия
    с нужным заголовком авторизации; переиспользуется для всех запросов.
    """

    def __init__(self):
        env = load_env()
        self.base = (env.get("TALK_BASE_URL") or "").rstrip("/")  # rstrip('/') чтобы не было '//' в URL
        token = env.get("TALK_API_TOKEN") or ""
        verify = env_flag(env, "TALK_VERIFY_SSL", "0")            # проверять ли TLS-сертификат
        # Качество видео для скачивания (GET /api/Recordings/{key}/file[/{качество}]). По умолчанию —
        # без качества (Толк отдаёт исходное видео); не вышло — пробуем «900p» из документации.
        self.video_qualities = list(dict.fromkeys(
            [(env.get("TALK_VIDEO_QUALITY") or "").strip(), "", "900p", "900 p"]))
        # Ранняя и понятная ошибка вместо загадочных падений позже, если конфиг пуст.
        if not self.base or not token:
            raise SystemExit("[!] Заполни TALK_BASE_URL и TALK_API_TOKEN в .env")
        # Session хранит общие заголовки и переиспользует TCP-соединение (keep-alive) —
        # быстрее, чем requests.get(...) каждый раз с нуля.
        self.s = requests.Session()
        self.s.headers["X-Auth-Token"] = token     # Толк ждёт ключ именно в этом заголовке
        self.s.verify = verify
        if not verify:
            urllib3.disable_warnings()              # иначе на каждый запрос сыплется InsecureRequestWarning

    # ---- записи --------------------------------------------------------
    def list(self, since=None, until=None, order="byTimeOldFirst",
             query=None, limit=None, page_size=200):
        """
        Генератор записей (yield по одной dict). Почему генератор, а не список:
        записей могут быть тысячи; так вызывающий код обрабатывает их по мере
        поступления и может остановиться в любой момент, не качая всё сразу.

        Пагинация: API отдаёт записи страницами и даёт nextPageToken на следующую.
        ВАЖНАЯ ЗАЩИТА ОТ ЗАЦИКЛИВАНИЯ: у этого API токен иногда не "пустеет" на
        последней странице, и наивный цикл крутился бы вечно, качая дубликаты.
        Поэтому ведём множество seen (уже отданных ключей) и останавливаемся, как
        только страница не принесла НИ ОДНОГО нового ключа.

        since / until — фильтр по дате создания (startFrom / startTo). Позволяет
        начать перебор не с самого начала истории, а с нужного момента.
        """
        seen = set()
        page_token = None       # курсор на следующую страницу (НЕ путать с TALK_API_TOKEN — тот про авторизацию)
        yielded = 0
        while True:
            # top — размер страницы; orderMode — сортировка (старые/новые первыми)
            params = {"top": page_size, "orderMode": order}
            if since:
                params["startFrom"] = since
            if until:
                params["startTo"] = until
            if query:
                params["query"] = query
            if page_token:
                params["pageTokenString"] = page_token     # просим следующую страницу
            r = self.s.get(f"{self.base}/api/Domain/recordings/v2",
                           params=params, timeout=60)
            r.raise_for_status()                            # бросит исключение на HTTP-ошибке (4xx/5xx)
            data = r.json()
            ents = data.get("entities", [])                 # сами записи лежат в поле entities
            new = [e for e in ents if e.get("key") not in seen]
            for e in new:
                seen.add(e.get("key"))
                yield e
                yielded += 1
                if limit and yielded >= limit:              # ограничение сверху, если задано
                    return
            page_token = data.get("nextPageToken")
            # Три условия остановки: неполная страница (значит последняя),
            # нет курсора, либо страница целиком из дубликатов (защита от петли).
            if len(ents) < page_size or not page_token or not new:
                return

    def recording_detail(self, key, max_participants=None):
        """Метаданные записи (title, createdDate, duration, participantsCount, canBeRemoved…).
        ВАЖНО: участников здесь Толк отдаёт не больше maxParticipantCount (по умолчанию 6) —
        полный список даёт participants()."""
        params = {"maxParticipantCount": max_participants} if max_participants else None
        r = self.s.get(f"{self.base}/api/Domain/recordings/{key}", params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def participants(self, key):
        """ВСЕ участники записи — постранично, GET /api/Domain/recordings/{key}/participants
        (по 100 за запрос). Элементы — как в recording_detail: {userInfo: {login, firstname,
        surname, ...}} или {anonymousName, anonymousId}. Если метода участников на этой
        установке Толка нет (404) — берём из деталей записи с большим пределом."""
        out, seen = [], set()
        for page in range(_PARTICIPANTS_MAX_PAGES):
            r = self.s.get(f"{self.base}/api/Domain/recordings/{key}/participants",
                           params={"skip": page * _PARTICIPANTS_PAGE, "top": _PARTICIPANTS_PAGE},
                           timeout=30)
            if r.status_code == 404 and page == 0:
                log.warning("метод участников записи недоступен (404) — беру их из деталей записи")
                return list(self.recording_detail(key, max_participants=1000).get("participants") or [])
            r.raise_for_status()
            items = r.json()
            if not isinstance(items, list):
                raise ValueError(f"участники записи: ожидали список, пришло {type(items).__name__}")
            new = [p for p in items if _participant_id(p) not in seen]
            for p in new:
                seen.add(_participant_id(p))
            out.extend(new)
            if len(items) < _PARTICIPANTS_PAGE or not new:   # последняя страница / одни повторы
                return out
        raise RuntimeError(f"участников записи больше {len(out)} — пагинация не закончилась")

    def user_info(self, login):
        """
        Профиль пользователя по его login (UUID) — GET /api/users/{login}.

        ВАЖНО: в деталях записи (recording_detail) у участников поле email НЕ
        отдаётся (только login + ФИО), даже у активных. Почта есть ТОЛЬКО через
        этот эндпоинт. Он и решает задачу «участник -> корпоративная почта».
        """
        r = self.s.get(f"{self.base}/api/users/{login}", timeout=30)
        r.raise_for_status()
        return r.json()

    def participants_with_emails(self, key):
        """
        Список участников записи с почтами: [{"name","email","login"}].

        Берём ВСЕХ участников (participants(), постранично), затем каждому резолвим email через
        user_info(login). Имя собираем из firstname+surname. Если у участника нет
        login (анонимный) или его профиля в Толке больше нет — email=None (обработаем выше).

        СБОЙ запроса профиля (сеть, 5xx, неверный ключ) пробрасываем: участник без почты
        не увидит запись в боте и не получит уведомление об удалении, а список участников
        сохраняется один раз — стадию надо повторить, а не терять людей молча. Так же
        поступаем, если Толк не отдал НИ ОДНОГО профиля и хотя бы раз ответил «нет доступа»:
        это не «у людей нет профилей», а у API-ключа нет прав их читать.
        """
        out = []
        resolved = denied = 0                   # сколько профилей получили / на сколько «нет доступа»
        for p in self.participants(key):
            ui = p.get("userInfo") or {}
            login = ui.get("login")
            name = " ".join(x for x in (ui.get("firstname"), ui.get("surname")) if x).strip()
            email, disabled = None, False
            if login:
                try:
                    info = self.user_info(login)    # один запрос: и email, и статus disabled
                except requests.HTTPError as ex:
                    code = getattr(ex.response, "status_code", None)
                    if code not in (400, 403, 404, 410):
                        raise                       # не «профиля нет», а сбой — повторим стадию
                    denied += code == 403
                    log.warning("профиль участника %s (%s) в Толке недоступен (HTTP %s) — "
                                "он останется без почты", name or "?", login, code)
                else:
                    resolved += 1
                    email = (info.get("email") or "").strip() or None
                    disabled = bool(info.get("disabled"))
            out.append({"name": name or (p.get("anonymousName") or "?"),
                        "email": email, "login": login, "disabled": disabled})
        if denied and not resolved:
            raise requests.HTTPError("Толк не отдал ни одного профиля участников (403 — нет "
                                     "доступа): проверь права API-ключа на чтение пользователей")
        return out

    def download_video(self, key, dest_path):
        """
        Скачать ВИДЕО записи (mp4) в dest_path — GET /api/Recordings/{key}/file/{качество}.
        Стримом, чтобы не держать большой файл целиком в памяти. Нужно, когда у записи нет
        аудиоархива: звук берём из видео (transcribe.py).

        Качество — из TALK_VIDEO_QUALITY (по умолчанию без качества — исходное видео); если Толк так
        не отдаёт (400/404), пробуем «900p» из документации.
        Сработавший вариант запоминаем и дальше пробуем первым. 400/404 на всех вариантах —
        HTTPError с этим кодом: видео у записи нет.
        """
        os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
        last = None
        for q in self.video_qualities:
            url = f"{self.base}/api/Recordings/{key}/file" + (f"/{quote(q, safe='')}" if q else "")
            with self.s.get(url, stream=True, timeout=600) as r:
                if r.status_code in (400, 404):
                    last = requests.HTTPError(f"{r.status_code}: {_talk_error(r.content) or r.reason}",
                                              response=r)
                    continue                            # нет такого качества (или видео) — дальше
                r.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in r.iter_content(1 << 20):   # по 1 МБ
                        if chunk:
                            f.write(chunk)
            if q != self.video_qualities[0]:
                log.info("видео отдаётся с качеством %r — дальше пробую его первым", q or "без качества")
                self.video_qualities.remove(q)
                self.video_qualities.insert(0, q)
            return dest_path
        raise last

    def delete_recording(self, key, force=False):
        """
        УДАЛИТЬ запись из Толка. Необратимо! Вызывается только после того, как все
        обязательные стадии успешны и файлы сохранены (см. pipeline).
        force=True — удалять даже защищённые (immutable) записи.
        """
        params = {"forceDelete": "true"} if force else None
        r = self.s.delete(f"{self.base}/api/Recordings/{key}", params=params, timeout=60)
        r.raise_for_status()
        return r.status_code

    # ---- аудио ---------------------------------------------------------
    def audio_zip(self, key, dest_path):
        """
        Скачать аудио-архив записи в файл dest_path. Это НЕПУБЛИЧНЫЙ эндпоинт: он
        отдаёт ZIP (application/octet-stream), а не JSON. Внутри — сведённый
        audio.m4a, поканальные дорожки channels/*.webm (по одной на спикера) и
        info.json с именами спикеров и их startAt.

        Стримом на диск, а не в память: архив многолюдной часовой встречи весит сотни
        мегабайт, а записи обрабатывают несколько воркеров одновременно.
        timeout=600 — на каждую порцию данных, не на весь архив.

        «Аудио нет» — NoAudio: Толк отвечает {"errorMessage": "Recording has no audio record"}
        либо 404 (записи уже нет). Прочие ошибки — исключение: это сбой, стадию повторим.
        """
        with self.s.get(f"{self.base}/api/recordings/{key}/audio", stream=True, timeout=600) as r:
            if r.status_code in (400, 404):
                msg = _talk_error(r.content)
                if r.status_code == 404 or "no audio" in msg.lower():
                    raise NoAudio(f"{r.status_code}: {msg or 'аудио нет'}")
                raise requests.HTTPError(f"{r.status_code}: {msg or r.reason}", response=r)
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(1 << 20):   # по 1 МБ
                    if chunk:
                        f.write(chunk)
        return dest_path

    def download_audio(self, key, out_dir):
        """
        Скачать архив и РАСПАКОВАТЬ его в out_dir. Возвращает (info, список файлов).
        Сам архив — временный файл в той же папке; после распаковки он удаляется.
        """
        os.makedirs(out_dir, exist_ok=True)
        zip_path = os.path.join(out_dir, "_audio.zip")
        self.audio_zip(key, zip_path)
        try:
            z = zipfile.ZipFile(zip_path)
        except zipfile.BadZipFile:
            # вместо архива пришёл JSON с той же ошибкой «нет аудио» (но с кодом 200)
            with open(zip_path, "rb") as f:
                msg = _talk_error(f.read(65536))
            if "no audio" in msg.lower():
                raise NoAudio(msg)
            raise
        with z:
            names = z.namelist()                               # имена файлов внутри ZIP
            z.extractall(out_dir)                              # распаковываем все дорожки на диск
            info = {}
            if "info.json" in names:
                info = json.loads(z.read("info.json"))         # метаданные: кто говорит, с какой секунды
        os.remove(zip_path)                                    # архив больше не нужен — освобождаем место
        return info, names
