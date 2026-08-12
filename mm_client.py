"""
mm_client.py — КЛИЕНТ Tag (Mattermost) на чистом REST API v4 (через requests).

Зачем свой тонкий клиент, а не mattermostdriver: нам нужен небольшой набор операций
(найти пользователя, открыть ЛС, отправить пост/файл В ВЕТКУ, открыть модалку), и на
requests это прозрачнее и без лишних зависимостей у демона. WebSocket-приём событий —
отдельно, в bot.py (там нужен websockets).

Используется двумя процессами:
  - демон (main.py) — только отправляет участникам ЛС с кнопками (send_archive_dm);
  - бот (bot.py)   — отправляет файлы/сообщения в ветку, открывает модалку.

Все сообщения и файлы кладутся В ВЕТКУ (root_id) — эфемерные не используются.
"""

import os

import requests
import urllib3

# Лейблы кнопок и варианты скачивания — в texts.py (единый источник строк).
from texts import BTN_FIND, BTN_DOWNLOAD, FILE_ACTIONS


class MMClient:
    def __init__(self, mm_cfg, verify=True):
        # base REST-URL: https://host:port/api/v4
        self.scheme = mm_cfg.scheme or "https"
        self.host = mm_cfg.url
        self.port = mm_cfg.port
        self.public_url = (mm_cfg.bot_public_url or "").rstrip("/")
        self.api = f"{self.scheme}://{self.host}:{self.port}/api/v4"
        # Общий секрет: подмешиваем в URL кнопок/модалки, бот его проверяет. Так
        # callback'и, пришедшие не по нашему URL, отсекаются (см. bot._authorized).
        self.secret = mm_cfg.button_secret or ""
        self.verify = verify
        if not verify:
            urllib3.disable_warnings()
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {mm_cfg.bot_token}"
        self.s.verify = verify
        self.bot_user_id = None

    # ---- низкоуровневое ----
    def _get(self, path, **kw):
        r = self.s.get(self.api + path, timeout=30, **kw)
        r.raise_for_status()
        return r.json()

    def _post(self, path, payload=None, **kw):
        r = self.s.post(self.api + path, json=payload, timeout=60, **kw)
        r.raise_for_status()
        return r.json() if r.content else {}

    def login(self):
        """Узнать id самого бота (нужно и для DM, и чтобы игнорировать свои посты)."""
        me = self._get("/users/me")
        self.bot_user_id = me["id"]
        return me

    # ---- пользователи ----
    def user_by_username(self, username):
        return self._get(f"/users/username/{username}")

    def user(self, user_id):
        return self._get(f"/users/{user_id}")

    # ---- каналы/посты ----
    def direct_channel(self, user_id):
        """Канал ЛС между ботом и пользователем (создаётся при необходимости)."""
        return self._post("/channels/direct", [self.bot_user_id, user_id])["id"]

    def post(self, channel_id, message="", props=None, root_id=None):
        payload = {"channel_id": channel_id, "message": message}
        if props:
            payload["props"] = props
        if root_id:
            payload["root_id"] = root_id
        return self._post("/posts", payload)

    def upload_file(self, channel_id, file_path):
        """Загрузить файл в канал, вернуть его file_id (для прикрепления к посту).
        Файл стримится (requests сам читает его порциями) — большое видео не грузим
        целиком в память."""
        with open(file_path, "rb") as f:
            r = self.s.post(self.api + "/files", params={"channel_id": channel_id},
                            files={"files": (os.path.basename(file_path), f)},
                            timeout=600)
        r.raise_for_status()
        return r.json()["file_infos"][0]["id"]

    def post_file(self, channel_id, file_path, message="", root_id=None):
        """Отправить файл В ВЕТКУ (root_id) с опциональным текстом."""
        return self.post_files(channel_id, [file_path], message=message, root_id=root_id)

    def post_files(self, channel_id, file_paths, message="", root_id=None):
        """Отправить НЕСКОЛЬКО файлов одним постом В ВЕТКУ (для действия «Всё»)."""
        file_ids = [self.upload_file(channel_id, p) for p in file_paths]
        payload = {"channel_id": channel_id, "message": message, "file_ids": file_ids}
        if root_id:
            payload["root_id"] = root_id
        return self._post("/posts", payload)

    # ---- модальные диалоги ----
    def open_dialog(self, trigger_id, url, dialog):
        return self._post("/actions/dialogs/open",
                          {"trigger_id": trigger_id, "url": url, "dialog": dialog})

    # ---- кнопки/вложения ----
    def _q(self):
        """Query-суффикс с секретом (или пусто, если секрет не задан)."""
        return f"?t={self.secret}" if self.secret else ""

    def action_url(self):
        return f"{self.public_url}/action{self._q()}"

    def dialog_url(self):
        return f"{self.public_url}/dialog{self._q()}"

    def _action(self, action_id, label, context):
        return {"id": action_id, "name": label,
                "integration": {"url": self.action_url(), "context": context}}

    def archive_attachment(self, text, recording_key):
        """Вложение per-record уведомления: текст + кнопки скачивания текста
        (Базовая/Улучшенная расшифровка, Краткая выжимка, Всё). Видео — из Talk напрямую."""
        return [{
            "text": text,
            "actions": [self._action(val, label, {"action": val, "key": recording_key})
                        for val, label in FILE_ACTIONS],
        }]

    def help_attachment(self, text):
        """Вложение help-сообщения: текст + кнопка «Найти видеозапись» (открывает модалку)."""
        return [{"text": text,
                 "actions": [self._action("find", BTN_FIND, {"action": "find"})]}]

    def old_records_attachment(self, text):
        """Вложение единоразового old-records уведомления: текст + кнопка «Скачать запись».
        Кнопка открывает ТУ ЖЕ модалку выбора записи/типа, что и «Найти видеозапись»."""
        return [{"text": text,
                 "actions": [self._action("find", BTN_DOWNLOAD, {"action": "find"})]}]

    def send_archive_dm(self, username, text, recording_key):
        """
        Отправить участнику ЛС с текстом уведомления и 3 кнопками.
        username = тело корпоративной почты до «@». Вызывается демоном.
        """
        user = self.user_by_username(username)
        channel_id = self.direct_channel(user["id"])
        return self.post(channel_id, message="",
                         props={"attachments": self.archive_attachment(text, recording_key)})

    def send_old_records_dm(self, username, text):
        """Единоразовое old-records ЛС: текст + кнопка «Скачать запись» (открывает модалку)."""
        user = self.user_by_username(username)
        channel_id = self.direct_channel(user["id"])
        return self.post(channel_id, message="",
                         props={"attachments": self.old_records_attachment(text)})

    def send_dm(self, username, text):
        """Простое ЛС пользователю (без кнопок) — например, алерт админу."""
        user = self.user_by_username(username)
        channel_id = self.direct_channel(user["id"])
        return self.post(channel_id, message=text)
