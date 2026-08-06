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

import io          # обёртка над байтами в "файл в памяти" (для ZIP без записи на диск)
import json
import os
import re          # срезать инлайн-комментарии в .env
import zipfile     # аудио-архив приходит как ZIP

import requests    # HTTP-клиент
import urllib3     # нужен только чтобы гасить предупреждение при verify=False

# Полный список переменных конфигурации проекта. Используется в load_env, чтобы
# знать, какие ключи разрешено переопределять через окружение (export VAR=...),
# даже если их нет в .env-файле.
_KNOWN_KEYS = (
    "TALK_BASE_URL", "TALK_API_TOKEN", "TALK_VERIFY_SSL",
    "MAGNITGPT_API_KEY", "MAGNITGPT_BASE_URL", "MAGNITGPT_MODEL", "MAGNITGPT_VERIFY_SSL",
    "MAGNITGPT_LLM_API_KEY", "MAGNITGPT_LLM_MODEL", "MAGNITGPT_LLM_BASE_URL", "LLM_THINKING",
    "ASR_CHUNK_SEC", "LLM_MAX_CHARS",
    # режимы/демон/БД
    "MODE", "TEST_CYCLE_SEC", "RUN_AT", "TIME_START", "TIME_END",
    "TIME_START_ASR", "TIME_END_ASR", "TIME_START_LLM", "TIME_END_LLM",
    "RECORD_WORKERS", "DELETE_DELAY_SEC",
    "DELETE_VIDEO_AGE_DAYS", "TEXT_RETENTION_DAYS", "MIN_DURATION", "ASR_LANGUAGE", "FORCE_DELETE",
    "DB_PATH", "ADMIN_USERNAME", "ASR_LLM_RPM", "EMERGENCY",
    # Tag/Mattermost
    "URL", "SCHEME", "PORT", "MM_VERIFY_SSL", "BOT_TOKEN",
    "BOT_PUBLIC_URL", "BUTTON_PORT", "BUTTON_SECRET", "SEND_TO_TAG",
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
        for line in open(p):
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

    def recording_detail(self, key):
        """Полные метаданные записи (title, createdDate, duration, participantsCount,
        roomName, size, canBeRemoved и т.д.) — для technical_info.json."""
        r = self.s.get(f"{self.base}/api/Domain/recordings/{key}", timeout=30)
        r.raise_for_status()
        return r.json()

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

        Берём participants из recording_detail, затем каждому резолвим email через
        user_info(login). Имя собираем из firstname+surname. Если у участника нет
        login (анонимный) или почта недоступна — email=None (обработаем выше).
        """
        detail = self.recording_detail(key)
        out = []
        for p in detail.get("participants", []):
            ui = p.get("userInfo") or {}
            login = ui.get("login")
            name = " ".join(x for x in (ui.get("firstname"), ui.get("surname")) if x).strip()
            email = None
            if login:
                try:
                    email = self.user_info(login).get("email")
                except Exception:
                    email = None                    # недоступен профиль — оставим None
            out.append({"name": name or (p.get("anonymousName") or "?"),
                        "email": email, "login": login})
        return out

    def download_video(self, key, dest_path):
        """
        Скачать ВИДЕО записи (mp4) в dest_path — GET /api/recordings/{key}/file.
        Стримом, чтобы не держать большой файл целиком в памяти. Нужен боту (Этап B)
        для кнопки «Видеозапись» и для проверки, что видео ещё не удалено.
        """
        os.makedirs(os.path.dirname(os.path.abspath(dest_path)), exist_ok=True)
        with self.s.get(f"{self.base}/api/recordings/{key}/file",
                        stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(1 << 20):   # по 1 МБ
                    if chunk:
                        f.write(chunk)
        return dest_path

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
    def audio_zip(self, key) -> bytes:
        """
        Скачать аудио-архив записи как сырые байты. Это НЕПУБЛИЧНЫЙ эндпоинт: он
        отдаёт ZIP (application/octet-stream), а не JSON. Внутри — сведённый
        audio.m4a, поканальные дорожки channels/*.webm (по одной на спикера) и
        info.json с именами спикеров и их startAt.
        timeout=600: архив многолюдной часовой встречи может быть большим.
        """
        r = self.s.get(f"{self.base}/api/recordings/{key}/audio", timeout=600)
        r.raise_for_status()                # если аудио нет — прилетит 400/404, обработаем выше (в batch)
        return r.content

    def download_audio(self, key, out_dir):
        """
        Скачать архив и РАСПАКОВАТЬ его в out_dir. Возвращает (info, список файлов).
        io.BytesIO оборачивает байты в «файл в памяти», чтобы zipfile мог с ними
        работать без сохранения промежуточного .zip на диск.
        """
        os.makedirs(out_dir, exist_ok=True)
        z = zipfile.ZipFile(io.BytesIO(self.audio_zip(key)))
        z.extractall(out_dir)                              # распаковываем все дорожки на диск
        info = {}
        if "info.json" in z.namelist():                    # namelist() — имена файлов внутри ZIP
            info = json.loads(z.read("info.json"))         # метаданные: кто говорит, с какой секунды
        return info, z.namelist()
