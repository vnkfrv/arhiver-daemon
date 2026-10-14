"""
mm_client.py — КЛИЕНТ Tag (Mattermost) на чистом REST API v4 (через requests).

Зачем свой тонкий клиент, а не mattermostdriver: нам нужен небольшой набор операций
(найти пользователя, открыть ЛС, отправить пост/файл В ВЕТКУ, открыть модалку), и на
requests это прозрачнее и без лишних зависимостей у демона. WebSocket-приём событий —
отдельно, в bot.py (там нужен websockets).

Используется двумя процессами:
  - демон (main.py) — отправляет участникам ЛС с кнопками и сводки админам;
  - бот (bot.py)   — отправляет файлы/сообщения в ветку, открывает модалки, рисует панели.

БОТОВ В Tag ДВА (две учётки, два токена): основной — для участников записей, и админ-бот
(admin_client) — для админов. Оба обслуживает один процесс bot.py.

Все сообщения и файлы — обычные посты (в ветку или в ЛС); эфемерные не используются.

ПОДПИСИ. Callback'и кнопок и модалок приходят боту по HTTP. URL модалки виден клиенту Tag,
поэтому одного секрета в URL мало: context'ы кнопок и state модалок подписаны ключом,
который не покидает сервер (sign/signed, pack/unpack). Подделать «нажатие админа» запросом
мимо Tag нельзя, даже зная адрес бота.
"""

import base64
import hashlib
import hmac
import json
import os
import time
from urllib.parse import quote

import requests
import urllib3

# Лейблы кнопок и варианты скачивания — в texts.py (единый источник строк).
from texts import BTN_DOWNLOAD, BTN_ADMIN_PANEL, BTN_ADMIN_RESTART_ALL, FILE_ACTIONS


class UserUnavailable(Exception):
    """Пользователя нет в Tag или его учётка деактивирована — написать ему нельзя.
    Это НЕ сбой связи: повтор через минуту ничего не изменит."""


def _status(ex):
    return getattr(getattr(ex, "response", None), "status_code", None)


_RATE_RETRIES = 3        # сколько раз повторить запрос, если Tag ответил «слишком часто» (429)


def _retry_after(response):
    """Сколько секунд подождать перед повтором после 429: столько, сколько просит Tag
    (Retry-After / X-Ratelimit-Reset), но от 1 до 30 с; заголовков нет — 2 с."""
    for header in ("Retry-After", "X-Ratelimit-Reset"):
        try:
            return min(30.0, max(1.0, float(response.headers.get(header))))
        except (AttributeError, TypeError, ValueError):
            continue
    return 2.0


def admin_client(mm_cfg, verify=True):
    """Клиент АДМИН-БОТА — отдельной учётки Tag (ADMIN_BOT_TOKEN), через которую идёт вся
    админская работа: панель, команды, сводки. Так она не смешивается в личке админа с
    уведомлениями о его собственных записях. Токен не задан -> тот же основной бот."""
    return MMClient(mm_cfg, verify=verify, token=getattr(mm_cfg, "admin_bot_token", "") or None)


class MMClient:
    def __init__(self, mm_cfg, verify=True, token=None):
        """token — чьим токеном ходить в Tag: по умолчанию основной бот (BOT_TOKEN); для
        админ-бота передаётся его токен (см. admin_client)."""
        # base REST-URL: https://host:port/api/v4
        self.scheme = mm_cfg.scheme or "https"
        self.host = mm_cfg.url
        self.port = mm_cfg.port
        self.public_url = (mm_cfg.bot_public_url or "").rstrip("/")
        self.api = f"{self.scheme}://{self.host}:{self.port}/api/v4"
        # Общий секрет: подмешиваем в URL кнопок, бот его проверяет. Так callback'и,
        # пришедшие не по нашему URL, отсекаются (см. bot._authorized).
        self.secret = mm_cfg.button_secret or ""
        self.token = token or mm_cfg.bot_token
        # Ключ подписи context'ов кнопок и state модалок (см. шапку модуля). Он ОДИН на всю
        # установку (от токена основного бота), поэтому подписи обоих ботов проверяются одинаково.
        self._key = hashlib.sha256(
            f"talk-archiver|{mm_cfg.bot_token}|{self.secret}".encode()).digest()
        # В URL модалки (его видит клиент Tag) кладём не сам секрет, а производный токен.
        self.dialog_token = self._mac("dialog-url")[:32] if self.secret else ""
        self.verify = verify
        if not verify:
            urllib3.disable_warnings()
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {self.token}"
        self.s.verify = verify
        self.bot_user_id = None
        self.username = None

    # ---- низкоуровневое ----
    def _request(self, method, path, timeout, **kw):
        """Запрос к API Tag. На 429 («слишком часто») ждём, сколько просит сервер, и
        повторяем: массовая рассылка не должна терять адресатов из-за лимита частоты."""
        for attempt in range(_RATE_RETRIES + 1):
            r = getattr(self.s, method)(self.api + path, timeout=timeout, **kw)
            if r.status_code != 429 or attempt == _RATE_RETRIES:
                break
            time.sleep(_retry_after(r))
        r.raise_for_status()
        return r

    def _get(self, path, **kw):
        return self._request("get", path, 30, **kw).json()

    def _post(self, path, payload=None, **kw):
        r = self._request("post", path, 60, json=payload, **kw)
        return r.json() if r.content else {}

    def _put(self, path, payload=None, **kw):
        r = self._request("put", path, 60, json=payload, **kw)
        return r.json() if r.content else {}

    def login(self):
        """Узнать id самого бота (нужно и для DM, и чтобы игнорировать свои посты)."""
        me = self._get("/users/me")
        self.bot_user_id = me["id"]
        self.username = me.get("username")
        return me

    # ---- подписи ----
    def _mac(self, text):
        return hmac.new(self._key, text.encode(), hashlib.sha256).hexdigest()

    def sign(self, ctx):
        """Подпись context'а кнопки."""
        return self._mac("|".join(f"{k}={ctx[k]}" for k in sorted(ctx)))[:32]

    def signed(self, ctx):
        """Верна ли подпись context'а нажатой кнопки. selected_option добавляет сам Tag
        (выбор в выпадающем списке) — в подпись он не входит."""
        body = {k: v for k, v in ctx.items() if k not in ("sig", "selected_option")}
        return hmac.compare_digest(str(ctx.get("sig") or ""), self.sign(body))

    def pack(self, data, ttl=3600):
        """Подписанный state модалки: данные + срок годности. Клиент Tag видит его, но
        изменить (или выписать себе чужой) не может."""
        body = dict(data, exp=int(time.time()) + ttl)
        raw = base64.urlsafe_b64encode(
            json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()).decode()
        return f"{raw}.{self._mac(raw)[:32]}"

    def unpack(self, token):
        """Данные из подписанного state или None (подделан/просрочен/пуст)."""
        raw, _, mac = str(token or "").rpartition(".")
        if not raw or not hmac.compare_digest(mac, self._mac(raw)[:32]):
            return None
        try:
            data = json.loads(base64.urlsafe_b64decode(raw.encode()))
        except ValueError:
            return None
        if not isinstance(data, dict) or data.get("exp", 0) < time.time():
            return None
        return data

    # ---- пользователи ----
    def user_by_username(self, username):
        return self._get(f"/users/username/{username}")

    def user(self, user_id):
        return self._get(f"/users/{user_id}")

    def find_user(self, username=None, email=None):
        """Активный пользователь Tag: сначала по нику (= тело почты до «@»), а если такого
        ника нет — по почте. UserUnavailable — в Tag его нет или учётка деактивирована;
        сетевые/серверные сбои пробрасываются как есть (их имеет смысл повторить).
        Ник и почту ищем строчными — так их хранит Tag; 400 на ник (недопустимые для ника
        символы) значит то же, что 404: пользователя с таким ником нет."""
        user = None
        username = (username or "").strip().lower()
        email = (email or "").strip().lower()
        if username:
            try:
                user = self.user_by_username(quote(username, safe=""))
            except requests.HTTPError as ex:
                if _status(ex) not in (400, 404):
                    raise
        if user is None and email:
            try:
                user = self._get(f"/users/email/{quote(email, safe='@')}")
            except requests.HTTPError as ex:
                if _status(ex) not in (400, 403, 404):
                    raise
        if user is None:
            raise UserUnavailable("нет в Tag")
        if user.get("delete_at"):
            raise UserUnavailable("учётка в Tag деактивирована")
        return user

    # ---- каналы/посты ----
    def direct_channel(self, user_id):
        """Канал ЛС между ботом и пользователем (создаётся при необходимости)."""
        return self._post("/channels/direct", [self.bot_user_id, user_id])["id"]

    def dm_channel(self, username=None, email=None):
        """Канал ЛС с пользователем по нику/почте. UserUnavailable — писать некому."""
        return self.direct_channel(self.find_user(username, email)["id"])

    def post(self, channel_id, message="", props=None, root_id=None):
        payload = {"channel_id": channel_id, "message": message}
        if props:
            payload["props"] = props
        if root_id:
            payload["root_id"] = root_id
        return self._post("/posts", payload)

    def get_post(self, post_id):
        return self._get(f"/posts/{post_id}")

    def patch_post(self, post_id, message=None, props=None):
        """Изменить уже отправленный пост (обновить карточку/панель на месте)."""
        payload = {}
        if message is not None:
            payload["message"] = message
        if props is not None:
            payload["props"] = props
        return self._put(f"/posts/{post_id}/patch", payload)

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
    def open_dialog(self, trigger_id, dialog):
        return self._post("/actions/dialogs/open",
                          {"trigger_id": trigger_id, "url": self.dialog_url(), "dialog": dialog})

    # ---- кнопки/вложения ----
    def action_url(self):
        return f"{self.public_url}/action" + (f"?t={self.secret}" if self.secret else "")

    def dialog_url(self):
        return f"{self.public_url}/dialog" + (f"?t={self.dialog_token}" if self.dialog_token else "")

    def _control(self, kind, label, action, ctx):
        # значения — строками: так context приходит обратно из Tag ровно таким, каким подписан
        ctx = {"action": action, **{k: str(v) for k, v in ctx.items() if v is not None}}
        ctx["sig"] = self.sign(ctx)
        # id обязан быть уникальным в посте и состоять ТОЛЬКО из букв/цифр (иначе Tag не
        # найдёт действие) — берём его из подписи.
        return {"id": kind[0] + ctx["sig"][:12], "name": label, "type": kind,
                "integration": {"url": self.action_url(), "context": ctx}}

    def button(self, label, action, style=None, **ctx):
        """Кнопка сообщения. action и ctx бот получит в context нажатия."""
        b = self._control("button", label, action, ctx)
        if style:
            b["style"] = style
        return b

    def menu(self, label, action, options, **ctx):
        """Выпадающий список в сообщении. options — [(текст, значение)]; выбранное значение
        Tag пришлёт в context.selected_option."""
        m = self._control("select", label, action, ctx)
        m["options"] = [{"text": t, "value": v} for t, v in options]
        return m

    def file_buttons(self, recording_key):
        """Кнопки скачивания текстов записи (Базовая/Улучшенная/Краткая/Всё)."""
        return [self.button(label, val, key=recording_key) for val, label in FILE_ACTIONS]

    def archive_attachment(self, text, recording_key):
        """Вложение per-record уведомления: текст + кнопки скачивания текста
        (Базовая/Улучшенная расшифровка, Краткая выжимка, Всё). Видео — из Talk напрямую."""
        return [{"text": text, "actions": self.file_buttons(recording_key)}]

    def old_records_attachment(self, text):
        """Вложение единоразового old-records уведомления: текст + кнопка «Скачать запись».
        Кнопка открывает ТУ ЖЕ модалку выбора записи/типа, что и «Найти видеозапись»."""
        return [{"text": text, "actions": [self.button(BTN_DOWNLOAD, "find")]}]

    def admin_alert_attachment(self, text):
        """Вложение алерта админам: текст + кнопки «перезапустить остановленные записи» и
        «админ-панель» (нажатие обрабатывает бот и сам проверяет, что нажал админ)."""
        return [{"text": text, "color": "#d24b4e",
                 "actions": [self.button(BTN_ADMIN_RESTART_ALL, "admrestart"),
                             self.button(BTN_ADMIN_PANEL, "admpanel")]}]

    def post_archive(self, channel_id, text, recording_key):
        """Per-record уведомление в уже открытый канал ЛС (текст + кнопки скачивания)."""
        return self.post(channel_id, message="",
                         props={"attachments": self.archive_attachment(text, recording_key)})

    def send_old_records_dm(self, username, text, email=None):
        """Единоразовое old-records ЛС: текст + кнопка «Скачать запись» (открывает модалку)."""
        return self.post(self.dm_channel(username, email), message="",
                         props={"attachments": self.old_records_attachment(text)})

    def send_dm(self, username, text, attachments=None):
        """ЛС пользователю: простой текст либо вложения с кнопками (алерт админу)."""
        channel_id = self.dm_channel(username)
        if attachments:
            return self.post(channel_id, message="", props={"attachments": attachments})
        return self.post(channel_id, message=text)
