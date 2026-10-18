#!/usr/bin/env python3
"""
bot.py — ИНТЕРАКТИВНЫЙ БОТ Tag (Mattermost). Этап B.

Отдельный процесс (демон main.py только РАССЫЛАЕТ ЛС с кнопками; всё, что происходит
по нажатию, обрабатывает этот сервер). Бот общается ТОЛЬКО в ЛС.

БОТОВ ДВА — две учётки Tag, обе обслуживает этот процесс:
  • основной (BOT_TOKEN) — для участников записей: уведомления, меню, расшифровки;
  • админ-бот (ADMIN_BOT_TOKEN) — для админов: панель, команды, сводки, решения о составе
    админов. У админа тоже есть свои записи, и так админская работа не смешивается с
    уведомлениями о них. Нет ADMIN_BOT_TOKEN — админские функции остаются в основном боте.

Пользователю:
  1. Любое сообщение боту -> меню: «Найти видеозапись» и «Поиск по названию».
  2. «Найти видеозапись» / «Скачать запись» -> окно: выпадающий список всех записей
     пользователя (сначала новые) + тип файла. Файл приходит В ВЕТКУ.
  3. «Поиск по названию» -> окно с полем для части названия; если нашлось несколько
     записей — сообщение с выпадающим списком найденного.
  4. Кнопки под уведомлением об удалении видео -> тексты этой записи в ветку.

Админу — в админ-боте (подробно — admin.py):
  archiever_admin или любое сообщение админ-боту — админ-панель: сводка, поиск и карточка любой записи, скачивание
  расшифровок, запрет/разрешение удаления, удаление видео, перезапуск остановленных,
  админы, журнал. Длинные ответы (разделы, результаты поиска, карточка) уходят в тред:
  в личке — одна строка-заголовок. Текстовые команды: restart_passed, newadmin_{ник},
  remember_{ID}, delete_{ID}.
  Состав админов: суперадмин меняет его сразу; решение обычного админа ждёт период
  охлаждения (ADMIN_CHANGE_DELAY_HOURS) — все админы получают уведомление, суперадмин
  может заблокировать, подавший — отозвать.

ЗАЩИТА CALLBACK'ОВ. Нажатия кнопок и отправки модалок приходят по HTTP. Мы проверяем:
секрет в URL; подпись context'а кнопки; подписанный state модалки, привязанный к
пользователю; что действие пришло из ЛС именно этого пользователя (файлы уходят только
туда); права админа — на каждое действие панели; участие в записи — перед выдачей её
текстов.

Тяжёлые операции (выгрузка файлов, удаление видео) выполняются в фоновом потоке, чтобы
быстро ответить на callback интеграции и не упереться в её таймаут.

Зависимости: flask, websockets, requests. Запуск:
    python bot.py
"""

import asyncio
import hmac
import json
import logging
import os
import re
import ssl
import tempfile
import threading
import time
from logging.handlers import RotatingFileHandler

from flask import Flask, request, jsonify

import admin
import db
import logsetup
import store
import texts as T
from config import load_cfg
from mm_client import MMClient, admin_client
from talk_api import TalkClient
from util import record_label

log = logging.getLogger("bot")

# Команды в ЛС (текстом). {nickname}/{ID} — «хвост» после подчёркивания.
RE_PANEL = re.compile(r"^arch(ie|i)ver_admin$", re.IGNORECASE)     # оба написания
RE_RESTART = re.compile(r"^restart_passed$", re.IGNORECASE)
RE_NEWADMIN = re.compile(r"^newadmin_(.+)$", re.IGNORECASE)
RE_REMEMBER = re.compile(r"^remember_(.+)$", re.IGNORECASE)
RE_DELETE = re.compile(r"^delete_(.+)$", re.IGNORECASE)

# Пределы выпадающих списков: окно Tag со списком в тысячи пунктов может не открыться, а
# тексты хранятся годами. Всё, что не поместилось, находится через «Поиск по названию».
SELECT_LIMIT = 1000      # записей в списке окна «Найти видеозапись» (самые новые)
RESULTS_LIMIT = 100      # записей в списке результатов поиска
FILE_KINDS = ("asr", "llm", "short")

cfg = load_cfg()
conn = db.connect(cfg.db_path)
mm = MMClient(cfg.mm, verify=cfg.mm.verify)       # основной бот: участники записей
# Админ-бот — отдельная учётка Tag для админской работы (панель, команды, сводки), чтобы она
# не смешивалась в личке админа с уведомлениями о его собственных записях. Если его токен
# (ADMIN_BOT_TOKEN) не задан, админские функции остаются в основном боте: amm — тот же mm.
amm = admin_client(cfg.mm, verify=cfg.mm.verify) if cfg.mm.admin_bot_token else mm
talk = TalkClient()
app = Flask(__name__)


# =========================================================================
#  КТО НАЖАЛ: пользователь, его почта, права, его ЛС
# =========================================================================

_users = {}              # user_id -> (когда спросили, профиль) — чтобы не дёргать Tag на каждый клик
_USER_TTL = 300
_dm_channels = {}        # (бот, user_id) -> id канала ЛС пользователя с этим ботом


def _user(user_id):
    """Профиль пользователя Tag (с коротким кэшем). {} — если Tag не ответил."""
    hit = _users.get(user_id)
    if hit and time.monotonic() - hit[0] < _USER_TTL:
        return hit[1]
    try:
        u = mm.user(user_id)
    except Exception as ex:
        log.warning("профиль %s не получен: %s", user_id, ex)
        return {}
    _users[user_id] = (time.monotonic(), u)
    return u


def _peer():
    """Откуда пришёл запрос (для логов): за nginx — адрес из X-Real-IP, иначе сам адрес."""
    return request.headers.get("X-Real-IP") or request.remote_addr


def _who(user_id):
    """Кто это — для логов: «@ник», а если Tag не ответил — id."""
    name = _user(user_id).get("username") if user_id else None
    return f"@{name}" if name else str(user_id or "?")


def _email(user_id):
    return _user(user_id).get("email")


def _admin_name(user_id):
    """Ник пользователя, если он администратор; иначе None."""
    name = _user(user_id).get("username")
    return name if name and db.is_admin(conn, name) else None


def _own_dm(client, user_id, channel_id):
    """Действие пришло из ЛС этого пользователя с ботом client. Так файлы и ответы уходят
    только самому нажавшему, даже если кто-то пришлёт боту запрос в обход Tag."""
    if not user_id or not channel_id:
        return False
    try:
        cid = _dm_channels.get((id(client), user_id))
        if cid is None:
            cid = _dm_channels[(id(client), user_id)] = client.direct_channel(user_id)
    except Exception as ex:
        log.warning("канал ЛС %s не определён: %s", user_id, ex)
        return False
    return cid == channel_id


def _may_read(user_id, key):
    """Можно ли отдать пользователю тексты записи: он её участник либо админ."""
    return bool(_admin_name(user_id)) or db.is_participant(conn, key, _email(user_id))


# =========================================================================
#  ДОСТАВКА ФАЙЛОВ (в фоне)
# =========================================================================

def _deliver(client, channel_id, root_id, key, action, user_id):
    """Положить нужный файл(ы) в ветку root_id от имени бота client (основного или админ-бота).
    Выполняется в отдельном потоке."""
    try:
        row = db.get(conn, key) if key else None
        if not row:
            client.post(channel_id, T.NOT_FOUND, root_id=root_id)
            return
        if not _may_read(user_id, key):
            client.post(channel_id, T.NOT_YOURS, root_id=root_id)
            return

        if action in FILE_KINDS or action == "all":
            if row.get("text_cleaned"):     # срок хранения истёк — «ещё не готов» было бы неправдой
                client.post(channel_id, T.TEXTS_EXPIRED, root_id=root_id)
                return
            caption = record_label(row)     # «ДД.ММ.ГГГГ - Название - X мин.»
            kinds = FILE_KINDS if action == "all" else (action,)
            paths = []
            for k in kinds:                 # без дублей: «улучшенная» без LLM — та же базовая
                p = store.existing_path(row, k)
                if p and p not in paths:
                    paths.append(p)
            if paths:
                client.post_files(channel_id, paths, message=caption, root_id=root_id)
                log.info("выдача: запись %s (%s) -> @%s, файлов: %d", key, action,
                         _user(user_id).get("username") or user_id, len(paths))
            else:
                client.post(channel_id, T.NOT_READY, root_id=root_id)
            return

        if action == "video":
            if row.get("deleted"):
                # ответ «уже удалено» — не чаще одного раза на ветку
                if not db.already_replied_deleted(conn, root_id):
                    client.post(channel_id, T.ALREADY_DELETED, root_id=root_id)
                    db.mark_replied_deleted(conn, root_id)
                return
            # уникальный временный файл: одновременные запросы одной записи не мешают
            fd, tmp = tempfile.mkstemp(prefix=f"{key}_", suffix=".mp4")
            os.close(fd)
            try:
                talk.download_video(key, tmp)
                client.post_file(channel_id, tmp, root_id=root_id)
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
    except Exception as ex:
        log.exception("доставка файла (%s, %s) не удалась: %s", key, action, ex)
        try:
            client.post(channel_id, T.DELIVER_FAIL, root_id=root_id)
        except Exception:
            pass


def _deliver_async(client, channel_id, root_id, key, action, user_id):
    threading.Thread(target=_deliver, args=(client, channel_id, root_id, key, action, user_id),
                     daemon=True).start()


# =========================================================================
#  ПОЛЬЗОВАТЕЛЮ: меню, список записей, модалка поиска
# =========================================================================

def _menu_for(channel_id, user_id):
    """Меню в ответ на любое сообщение в ЛС: сколько записей в архиве + кнопки."""
    u = _user(user_id)
    email = u.get("email")
    n = db.count_for_email(conn, email) if email else 0
    text = T.HELP_TEXT.format(name=u.get("first_name") or "коллега")
    text += T.HELP_COUNT.format(n=n) if n else T.HELP_EMPTY
    actions = [mm.button(T.BTN_FIND, "find"), mm.button(T.BTN_SEARCH, "search")] if n else []
    if amm is mm and _admin_name(user_id):   # отдельного админ-бота нет — панель здесь же
        actions.append(mm.button(T.BTN_ADMIN_PANEL, "admpanel"))
    mm.post(channel_id, message="", props={"attachments": [{"text": text, "actions": actions}]})


def _options(rows):
    """Пункты выпадающего списка записей: «ДД.ММ.ГГГГ - Название - X мин.»."""
    return [{"text": record_label(r)[:90], "value": r["recording_key"]} for r in rows]


def _type_field():
    """Поле окна «Выберите тип файла» (по умолчанию — «Всё»)."""
    return {"display_name": T.DIALOG_SELECT_TYPE, "name": "do", "type": "select", "default": "all",
            "options": [{"text": label, "value": val} for val, label in T.FILE_ACTIONS]}


def _file_kind(value):
    """Тип файла из формы; незнакомое значение -> «Всё»."""
    return value if value in dict(T.FILE_ACTIONS) else "all"


def _act_file(data, ctx):
    """Кнопка скачивания текста под уведомлением: файл(ы) в ветку под этим сообщением."""
    _deliver_async(mm, data["channel_id"], data.get("post_id"), ctx.get("key"), ctx["action"],
                   data.get("user_id"))


def _act_find(data, ctx):
    """«Найти видеозапись» / «Скачать запись»: окно с выпадающим списком ВСЕХ записей
    пользователя (сначала новые) и типом файла."""
    user_id, channel_id, post_id = data.get("user_id"), data["channel_id"], data.get("post_id")
    email = _email(user_id)
    total = db.count_for_email(conn, email) if email else 0
    if not total:
        mm.post(channel_id, T.NO_RECORDS, root_id=post_id)
        return
    rows = db.records_for_email(conn, email, limit=SELECT_LIMIT)
    record = {"display_name": T.DIALOG_SELECT_RECORD, "name": "recording", "type": "select",
              "options": _options(rows)}
    if total > len(rows):                    # список упёрся в предел окна — подскажем про поиск
        record["help_text"] = T.DIALOG_CAPPED_HELP.format(n=len(rows), total=total)
    mm.open_dialog(data.get("trigger_id"), {
        "callback_id": "find_video", "title": T.DIALOG_TITLE, "submit_label": T.DIALOG_SUBMIT,
        # state несёт, куда потом класть файл (в ту же ветку), и чей это диалог
        "state": mm.pack({"uid": user_id, "ch": channel_id, "root": post_id}),
        "elements": [record, _type_field()]})


def _dlg_find(data, st, sub):
    """Отправка окна «Видеозаписи»: файл(ы) выбранной записи — в ветку."""
    key = sub.get("recording")
    if not key:
        return {"errors": {"recording": T.DIALOG_NEED_RECORD}}
    mm.post(st["ch"], T.SOON, root_id=st.get("root"))
    _deliver_async(mm, st["ch"], st.get("root"), key, _file_kind(sub.get("do")), st["uid"])
    return None


def _act_search(data, ctx):
    """«Поиск по названию»: окно с полем для части названия и типом файла."""
    user_id, channel_id, post_id = data.get("user_id"), data["channel_id"], data.get("post_id")
    email = _email(user_id)
    if not email or not db.count_for_email(conn, email):
        mm.post(channel_id, T.NO_RECORDS, root_id=post_id)
        return
    mm.open_dialog(data.get("trigger_id"), {
        "callback_id": "search", "title": T.SEARCH_TITLE, "submit_label": T.SEARCH_SUBMIT,
        "state": mm.pack({"uid": user_id, "ch": channel_id, "root": post_id}),
        "elements": [{"display_name": T.SEARCH_FIELD, "name": "q", "type": "text", "min_length": 2,
                      "max_length": 100, "placeholder": T.SEARCH_PLACEHOLDER}, _type_field()]})


def _dlg_search(data, st, sub):
    """Отправка окна поиска. Нет совпадений — ошибка в самом окне; одно — сразу файл в ветку;
    несколько — сообщение с выпадающим списком найденного (выбор присылает файл)."""
    q, kind = (sub.get("q") or "").strip(), _file_kind(sub.get("do"))
    ch, root, uid = st["ch"], st.get("root"), st["uid"]
    email = _email(uid)
    n = db.count_for_email(conn, email, q) if email and q else 0
    if not n:
        return {"errors": {"q": T.DIALOG_NO_MATCH.format(q=q)}}
    rows = db.records_for_email(conn, email, limit=RESULTS_LIMIT, query=q)
    if n == 1:
        mm.post(ch, T.SOON, root_id=root)
        _deliver_async(mm, ch, root, rows[0]["recording_key"], kind, uid)
        return None
    text = T.SEARCH_RESULTS.format(q=q, n=n, kind=dict(T.FILE_ACTIONS)[kind])
    if n > len(rows):
        text += T.SEARCH_MORE.format(m=len(rows))
    options = [(o["text"], o["value"]) for o in _options(rows)]
    # отдельным сообщением (не в ветку): в ветку ПОД НИМ придут файлы выбранной записи
    mm.post(ch, message="", props={"attachments": [
        {"text": text, "actions": [mm.menu(T.MENU_PICK, "pick", options, do=kind)]}]})
    return None


def _act_pick(data, ctx):
    """Выбор записи в выпадающем списке результатов поиска: файл(ы) — в ветку под ним."""
    _deliver_async(mm, data["channel_id"], data.get("post_id"), ctx.get("selected_option"),
                   _file_kind(ctx.get("do")), data.get("user_id"))


# =========================================================================
#  АДМИНУ: действия панели (кнопки) и её модалки — от имени админ-бота (amm)
# =========================================================================

def _update(attachments):
    """Ответ на нажатие: заменить сообщение с кнопкой новым содержимым (на месте)."""
    return {"update": {"message": "", "props": {"attachments": attachments}}}


def _thread_root(post_id):
    """Корень треда, в котором лежит сообщение с нажатой кнопкой (для обычного сообщения —
    оно само). Карточки и списки панели живут в тредах, и ответы с файлами должны попадать
    в тот же тред."""
    if not post_id:
        return post_id
    try:
        return amm.get_post(post_id).get("root_id") or post_id
    except Exception as ex:
        log.warning("корень треда для %s не определён: %s", post_id, ex)
        return post_id


def _open(channel_id, view):
    """Длинный ответ панели: в ЛС — одна строка, а содержимое — в тред под ней, чтобы личка
    не превращалась в простыню. Пустой раздел — одной строкой, без треда."""
    head = amm.post(channel_id, view.head)
    if not view.empty:
        amm.post(channel_id, message="", props={"attachments": view.atts}, root_id=head["id"])


def _card(key):
    row = db.get(conn, key) if key else None
    return admin.card(conn, cfg, amm, row) if row else None


def _refresh_card(post_id, key):
    """Перерисовать карточку записи в уже отправленном сообщении (после действия из модалки)."""
    card = _card(key)
    if post_id and card:
        try:
            amm.patch_post(post_id, message="", props={"attachments": card})
        except Exception as ex:
            log.warning("карточка %s не обновлена: %s", key, ex)


def _adm_panel(data, ctx, by):
    """Главный экран панели — прямо в ЛС (на нём кнопки разделов); «Обновить» — на месте."""
    atts = admin.panel(conn, cfg, amm)
    if ctx.get("upd"):
        return _update(atts)
    amm.post(data["channel_id"], message="", props={"attachments": atts})
    return None


def _adm_card(data, ctx, by):
    key = ctx.get("selected_option") or ctx.get("key")      # выбор в списке либо кнопка
    card = _card(key)
    if not card:
        amm.post(data["channel_id"], T.ADM_NOT_FOUND.format(key=key), root_id=data["root"])
        return None
    if ctx.get("upd"):                                      # «Обновить» в самой карточке
        return _update(card)
    # запись выбрали в списке — карточку кладём в тот же тред, где этот список
    amm.post(data["channel_id"], message="", props={"attachments": card}, root_id=data["root"])
    return None


def _adm_file(data, ctx, by):
    key, kind = ctx.get("key"), ctx.get("kind")
    db.log_admin(conn, by, "download", key, dict(T.FILE_ACTIONS).get(kind, kind))
    _deliver_async(amm, data["channel_id"], data["root"], key, kind, data.get("user_id"))


def _adm_protect(data, ctx, by):
    fn = admin.protect if ctx["action"] == "admprotect" else admin.unprotect
    _, text = fn(conn, ctx.get("key"), by)
    amm.post(data["channel_id"], text, root_id=data["root"])
    card = _card(ctx.get("key"))
    return _update(card) if card else None


def _adm_restart(data, ctx, by):
    _, text = admin.restart(conn, by, ctx.get("key"))
    amm.post(data["channel_id"], text, root_id=data["root"])
    view = ctx.get("view")                   # откуда нажали — тот экран и перерисуем
    if view == "card":
        card = _card(ctx.get("key"))
        return _update(card) if card else None
    if view == "panel":
        return _update(admin.panel(conn, cfg, amm))
    if view == "holds":
        return _update(admin.holds_view(conn, amm).atts)
    return None                              # сводка демона — оставляем как есть


def _adm_find(data, ctx, by):
    amm.open_dialog(data.get("trigger_id"), {
        "callback_id": "admfind", "title": T.ADM_FIND_TITLE, "submit_label": T.ADM_FIND_SUBMIT,
        "state": amm.pack({"uid": data.get("user_id"), "ch": data["channel_id"]}),
        "elements": [
            {"display_name": T.ADM_FIND_Q, "name": "q", "type": "text", "optional": True,
             "max_length": 150, "help_text": T.ADM_FIND_Q_HELP},
            {"display_name": T.ADM_FIND_WHO, "name": "who", "type": "text", "optional": True,
             "max_length": 100, "help_text": T.ADM_FIND_WHO_HELP}]})


def _dlg_adm_find(data, st, sub, by):
    q, who = (sub.get("q") or "").strip(), (sub.get("who") or "").strip()
    if not q and not who:
        return {"errors": {"q": T.ADM_FIND_EMPTY}}
    rows = admin.find(conn, q or None, who or None)
    if not rows:
        return {"errors": {"q" if q else "who": T.ADM_FIND_NONE}}
    _open(st["ch"], admin.card_view(conn, cfg, amm, rows[0]) if len(rows) == 1
          else admin.results(amm, rows))
    return None


def _adm_delete(data, ctx, by):
    """«Удалить видео…»: сначала подтверждение в модалке (действие необратимо)."""
    key = ctx.get("key")
    row = db.get(conn, key)
    if not row or row.get("deleted"):        # удалять нечего — скажем, что с записью
        _, text = admin.delete_video(conn, cfg, talk, key, by)
        amm.post(data["channel_id"], text, root_id=data["root"])
        card = _card(key)
        return _update(card) if card else None
    intro = T.ADM_DELETE_INTRO.format(title=row.get("title") or key, label=record_label(row))
    if not row.get("transcribed"):
        intro += T.ADM_DELETE_INTRO_NO_TEXT
    if cfg.simulate_delete:
        intro += T.ADM_DELETE_INTRO_SIMULATED
    amm.open_dialog(data.get("trigger_id"), {
        "callback_id": "admdelete", "title": T.ADM_DELETE_TITLE,
        "submit_label": T.ADM_DELETE_SUBMIT, "introduction_text": intro,
        # post — сама карточка (её перерисуем), root — тред, куда писать ответ
        "state": amm.pack({"uid": data.get("user_id"), "ch": data["channel_id"], "key": key,
                          "post": data.get("post_id"), "root": data["root"]}),
        "elements": [{"display_name": T.ADM_DELETE_REASON, "name": "reason", "type": "text",
                      "optional": True, "max_length": 150}]})
    return None


def _delete_job(st, by, reason):
    """Удаление видео в Толке — в фоне: ответ в тред карточки + её перерисовка."""
    try:
        _, text = admin.delete_video(conn, cfg, talk, st["key"], by, reason)
    except Exception as ex:
        log.exception("удаление %s админом @%s не удалось: %s", st.get("key"), by, ex)
        text = T.ADM_DELETE_FAIL.format(key=st.get("key"), error=ex)
    try:
        amm.post(st["ch"], text, root_id=st.get("root") or st.get("post"))
    except Exception as ex:
        log.warning("ответ об удалении %s не отправлен: %s", st.get("key"), ex)
    _refresh_card(st.get("post"), st["key"])


def _dlg_adm_delete(data, st, sub, by):
    threading.Thread(target=_delete_job, args=(st, by, (sub.get("reason") or "").strip()),
                     daemon=True).start()
    return None


def _adm_section(view):
    """Обработчик кнопки раздела панели: строка в ЛС + содержимое раздела в треде под ней.
    view(by) — раздел глазами нажавшего админа."""
    def handler(data, ctx, by):
        _open(data["channel_id"], view(by))
    return handler


# ---- состав админов: решения с периодом охлаждения, блокировка, передача прав ----

def _tell_admins(text, skip=(), block_req=None):
    """ЛС всем админам (кроме skip). block_req — id решения: суперадмину под сообщением
    добавляется кнопка «Заблокировать»."""
    sup = db.superadmin(conn)
    for u in db.list_admins(conn):
        if u in skip:
            continue
        try:
            actions = ([amm.button(T.BTN_ADMIN_BLOCK, "admblock", style="danger", req=block_req)]
                       if block_req and u == sup else None)
            amm.send_dm(u, text, attachments=[{"text": text, "actions": actions}] if actions else None)
        except Exception as ex:
            log.warning("уведомление админу @%s не доставлено: %s", u, ex)


def _announce_outcome(req, result, reason=None, skip=()):
    """Разослать исход решения: всем админам — что произошло; тому, чьи права изменились, —
    личное сообщение."""
    target = req["target"]
    _tell_admins(admin.outcome_text(conn, req, result, reason), skip=tuple(skip) + (target,))
    try:
        if result == "applied":
            tpl = T.ADM_WELCOME if req["kind"] == "add" else T.ADM_GOODBYE
            amm.send_dm(target, tpl.format(by=req["requested_by"]))
        elif target not in skip and db.is_admin(conn, target):
            # решение о нём не состоялось — он админ и тоже должен об этом узнать
            amm.send_dm(target, admin.outcome_text(conn, req, result, reason))
    except Exception as ex:
        log.warning("уведомление @%s об исходе решения не доставлено: %s", target, ex)


def _after_change(change, by):
    """Рассылки после изменения состава админов — в фоне, чтобы не держать ответ на нажатие."""
    def job():
        try:
            if change.request:                   # новое решение ждёт — уведомляем всех админов
                _tell_admins(admin.request_notice(conn, cfg, change.request), skip=(by,),
                             block_req=change.request["id"])
            for req, result, reason in change.outcomes:
                _announce_outcome(req, result, reason, skip=(by,))
        except Exception as ex:
            log.warning("рассылка об изменении состава админов не удалась: %s", ex)
    threading.Thread(target=job, daemon=True).start()


def _refresh_admins(post_id, viewer):
    """Перерисовать раздел «Админы» в уже отправленном сообщении."""
    if post_id:
        try:
            amm.patch_post(post_id, message="",
                          props={"attachments": admin.admins_view(conn, amm, cfg, viewer).atts})
        except Exception as ex:
            log.warning("список админов не обновлён: %s", ex)


def _change_intro(by):
    """Вступление окна «добавить/убрать»: что произойдёт после отправки — зависит от роли."""
    if admin.is_super(conn, by):
        return T.ADM_CHANGE_INTRO_NOW
    return T.ADM_CHANGE_INTRO_WAIT.format(hours=admin.delay_hours(cfg),
                                          super=db.superadmin(conn) or "—")


def _adm_add_admin(data, ctx, by):
    amm.open_dialog(data.get("trigger_id"), {
        "callback_id": "admaddadmin", "title": T.ADM_ADD_TITLE, "submit_label": T.ADM_ADD_SUBMIT,
        "introduction_text": _change_intro(by),
        "state": amm.pack({"uid": data.get("user_id"), "ch": data["channel_id"],
                          "post": data.get("post_id"), "root": data["root"]}),
        "elements": [{"display_name": T.ADM_ADD_NICK, "name": "nick", "type": "text",
                      "max_length": 64}]})


def _adm_del_admin(data, ctx, by):
    sup = db.superadmin(conn)
    others = [a for a in db.list_admins(conn) if a != sup]      # суперадмина убрать нельзя
    if not others:
        amm.post(data["channel_id"], T.ADM_ADMIN_NO_OTHERS, root_id=data["root"])
        return
    amm.open_dialog(data.get("trigger_id"), {
        "callback_id": "admdeladmin", "title": T.ADM_REMOVE_TITLE,
        "submit_label": T.ADM_REMOVE_SUBMIT, "introduction_text": _change_intro(by),
        "state": amm.pack({"uid": data.get("user_id"), "ch": data["channel_id"],
                          "post": data.get("post_id"), "root": data["root"]}),
        "elements": [{"display_name": T.ADM_REMOVE_WHO, "name": "who", "type": "select",
                      "options": [{"text": f"@{a}", "value": a} for a in others]}]})


def _dlg_adm_admins(data, st, sub, by):
    """Отправка окон «Новый администратор» / «Убрать администратора»."""
    add = data.get("callback_id") == "admaddadmin"
    change = admin.change_admins(conn, cfg, amm, "add" if add else "remove",
                                 sub.get("nick") if add else sub.get("who"), by)
    if not change.ok:
        return {"errors": {"nick" if add else "who": change.text}}
    amm.post(st["ch"], change.text, root_id=st.get("root") or st.get("post"))
    _after_change(change, by)
    _refresh_admins(st.get("post"), by)
    return None


def _adm_close_request(data, ctx, by):
    """Суперадмин блокирует решение / подавший отзывает своё: кнопкой под уведомлением
    либо выбором в разделе «Админы»."""
    try:
        req_id = int(ctx.get("selected_option") or ctx.get("req"))
    except (TypeError, ValueError):
        return None
    fn = admin.block if ctx["action"] == "admblock" else admin.withdraw
    change = fn(conn, req_id, by)
    _after_change(change, by)
    if ctx.get("view") == "admins":              # выбрано в разделе «Админы» — ответ в его тред
        amm.post(data["channel_id"], change.text, root_id=data["root"])
        return _update(admin.admins_view(conn, amm, cfg, by).atts)
    if not change.ok:                            # кнопка под уведомлением: решение уже закрыто
        amm.post(data["channel_id"], change.text, root_id=data["root"])
        return None
    return _update([{"text": change.text}])      # уведомление заменяем итогом, кнопка исчезает


def _adm_transfer(data, ctx, by):
    """«Передать права суперадмина»: окно с выбором админа (только суперадмину)."""
    if not admin.is_super(conn, by):
        amm.post(data["channel_id"], T.ADM_SUPER_ONLY.format(super=db.superadmin(conn) or "—"),
                root_id=data["root"])
        return
    others = [a for a in db.list_admins(conn) if a != by]
    if not others:
        amm.post(data["channel_id"], T.ADM_TRANSFER_NOT_ADMIN, root_id=data["root"])
        return
    amm.open_dialog(data.get("trigger_id"), {
        "callback_id": "admtransfer", "title": T.ADM_TRANSFER_TITLE,
        "submit_label": T.ADM_TRANSFER_SUBMIT, "introduction_text": T.ADM_TRANSFER_INTRO,
        "state": amm.pack({"uid": data.get("user_id"), "ch": data["channel_id"],
                          "post": data.get("post_id"), "root": data["root"]}),
        "elements": [{"display_name": T.ADM_TRANSFER_WHO, "name": "who", "type": "select",
                      "options": [{"text": f"@{a}", "value": a} for a in others]}]})


def _dlg_adm_transfer(data, st, sub, by):
    ok, text = admin.transfer_superadmin(conn, sub.get("who"), by)
    if not ok:
        return {"errors": {"who": text}}
    amm.post(st["ch"], text, root_id=st.get("root") or st.get("post"))
    who = db.superadmin(conn)
    # кто суперадмин, должны знать все админы: именно он блокирует решения
    threading.Thread(target=_tell_admins, daemon=True,
                     args=(T.ADM_TRANSFER_NOTICE.format(who=who, by=by), (by,))).start()
    _refresh_admins(st.get("post"), by)
    return None


def _admin_requests_loop():
    """Фоновый поток: приводит в исполнение решения об админах, у которых истёк период
    охлаждения, и рассылает их исходы. Проверка раз в минуту; если бот не работал в момент
    срока — решение вступит в силу при запуске."""
    while True:
        try:
            for req, result, reason in admin.apply_due(conn):
                _announce_outcome(req, result, reason)
        except Exception as ex:
            log.warning("решения об админах: сбой: %s", ex)
        time.sleep(60)


# действие кнопки -> обработчик (data, ctx) -> ответ интеграции или None
USER_ACTIONS = {"find": _act_find, "search": _act_search, "pick": _act_pick}
USER_ACTIONS.update({a: _act_file for a in FILE_KINDS + ("all", "video")})
# действия панели -> обработчик (data, ctx, ник админа); права проверяются перед вызовом
ADMIN_ACTIONS = {
    "admpanel": _adm_panel, "admfind": _adm_find, "admcard": _adm_card, "admfile": _adm_file,
    "admprotect": _adm_protect, "admunprotect": _adm_protect, "admdelete": _adm_delete,
    "admrestart": _adm_restart,
    "admholds": _adm_section(lambda by: admin.holds_view(conn, amm)),
    "admupcoming": _adm_section(lambda by: admin.upcoming_view(conn, amm)),
    "admkept": _adm_section(lambda by: admin.kept_view(conn, amm)),
    "admadmins": _adm_section(lambda by: admin.admins_view(conn, amm, cfg, by)),
    "admlog": _adm_section(lambda by: admin.log_view(conn)),
    "admaddadmin": _adm_add_admin, "admdeladmin": _adm_del_admin,
    "admblock": _adm_close_request, "admwithdraw": _adm_close_request,
    "admtransfer": _adm_transfer,
}
USER_DIALOGS = {"find_video": _dlg_find, "search": _dlg_search}
ADMIN_DIALOGS = {"admfind": _dlg_adm_find, "admdelete": _dlg_adm_delete,
                 "admaddadmin": _dlg_adm_admins, "admdeladmin": _dlg_adm_admins,
                 "admtransfer": _dlg_adm_transfer}


# =========================================================================
#  HTTP: нажатия кнопок и модалки
# =========================================================================

def _authorized(expected):
    """Проверить токен в query (?t=...). Так отсекаем запросы не по нашему URL.
    Если секрет не задан в конфиге — пропускаем (но это НЕ рекомендуется в проде)."""
    if not expected:
        return True
    return hmac.compare_digest((request.args.get("t") or "").encode(), expected.encode())


@app.route("/action", methods=["POST"])
def action():
    """Callback интерактивных кнопок и выпадающих списков."""
    if not _authorized(mm.secret):
        log.warning("/action: запрос с %s с неверным секретом в адресе — отклонён "
                    "(BUTTON_SECRET у кнопки и у бота разный?)", _peer())
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):            # мусор вместо JSON-объекта — не наш запрос
        data = {}
    ctx = data.get("context") or {}
    act = str(ctx.get("action") or "") if isinstance(ctx, dict) else ""
    log.info("нажатие: %s от %s (с %s)", act or "?", _who(data.get("user_id")), _peer())
    client = None                 # бот, в чьей личке нажали (известен после проверок)
    try:
        if act not in USER_ACTIONS and act not in ADMIN_ACTIONS:
            return jsonify({})
        if not mm.signed(ctx):
            log.warning("/action %s: неверная подпись — отклонено", act)
            return jsonify({})
        # админские кнопки живут в личке с админ-ботом, остальные — с основным
        dm_bot = amm if act in ADMIN_ACTIONS else mm
        if not _own_dm(dm_bot, data.get("user_id"), data.get("channel_id")):
            log.warning("/action %s: запрос не из ЛС пользователя — отклонено", act)
            return jsonify({})
        client = dm_bot
        if act in ADMIN_ACTIONS:
            # кнопки панели бывают в тредах — отвечаем в тот же тред
            data["root"] = _thread_root(data.get("post_id"))
            by = _admin_name(data.get("user_id"))
            if not by:
                amm.post(data["channel_id"], T.ADMIN_ONLY, root_id=data["root"])
                return jsonify({})
            return jsonify(ADMIN_ACTIONS[act](data, ctx, by) or {})
        return jsonify(USER_ACTIONS[act](data, ctx) or {})
    except Exception:
        log.exception("/action %s не обработан", act)
        if client is not None:    # запрос проверен — скажем человеку, а не оставим ждать молча
            try:
                client.post(data["channel_id"], T.ACTION_FAIL,
                            root_id=data.get("root") or data.get("post_id"))
            except Exception as ex:
                log.warning("/action %s: ответ о сбое не отправлен: %s", act, ex)
    return jsonify({})            # Tag ждёт быстрый 200


@app.route("/dialog", methods=["POST"])
def dialog():
    """Отправка модалки. Ошибки заполнения возвращаем в саму модалку (errors)."""
    if not _authorized(mm.dialog_token):
        log.warning("/dialog: запрос с %s с неверным токеном в адресе — отклонён",
                    _peer())
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(force=True, silent=True)
    if not isinstance(data, dict):
        data = {}
    if data.get("cancelled"):
        return jsonify({})
    cb = data.get("callback_id")
    log.info("окно: %s от %s", cb or "?", _who(data.get("user_id")))
    st = mm.unpack(data.get("state"))
    # state подписан и привязан к пользователю: чужую или поддельную форму не принимаем
    if not st or st.get("uid") != data.get("user_id") or not st.get("ch"):
        return jsonify({"error": T.DIALOG_EXPIRED})
    sub = data.get("submission") or {}
    try:
        if cb in ADMIN_DIALOGS:
            by = _admin_name(st["uid"])
            if not by:
                return jsonify({"error": T.ADMIN_ONLY})
            return jsonify(ADMIN_DIALOGS[cb](data, st, sub, by) or {})
        if cb in USER_DIALOGS:
            return jsonify(USER_DIALOGS[cb](data, st, sub) or {})
    except Exception:
        log.exception("/dialog %s не обработан", cb)
        return jsonify({"error": T.DIALOG_FAIL})
    return jsonify({})


@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


# =========================================================================
#  WEBSOCKET: сообщение боту в ЛС -> команда админа либо меню
# =========================================================================

def _is_admin_command(text):
    return any(r.match(text) for r in (RE_PANEL, RE_RESTART, RE_NEWADMIN, RE_REMEMBER, RE_DELETE))


def _try_admin_command(text, user_id, channel_id):
    """Разобрать и выполнить админ-команду (отвечает админ-бот). Возвращает True, если это
    была команда."""
    if not _is_admin_command(text):
        return False
    by = _admin_name(user_id)
    if not by:
        amm.post(channel_id, T.ADMIN_ONLY)
        return True
    m_new, m_rem, m_del = RE_NEWADMIN.match(text), RE_REMEMBER.match(text), RE_DELETE.match(text)
    if RE_PANEL.match(text):
        amm.post(channel_id, message="", props={"attachments": admin.panel(conn, cfg, amm)})
    elif RE_RESTART.match(text):
        amm.post(channel_id, admin.restart(conn, by)[1])
    elif m_new:                                  # тот же путь, что кнопка «Добавить админа»
        change = admin.change_admins(conn, cfg, amm, "add", m_new.group(1), by)
        amm.post(channel_id, change.text)
        _after_change(change, by)
    elif m_rem:
        amm.post(channel_id, admin.protect(conn, m_rem.group(1).strip(), by)[1])
    else:
        amm.post(channel_id, admin.delete_video(conn, cfg, talk, m_del.group(1).strip(), by)[1])
    return True


def _on_user_bot_message(text, user_id, channel_id):
    """Сообщение ОСНОВНОМУ боту: меню. Админ-команды он выполняет, только пока отдельного
    админ-бота нет; иначе подсказывает админу, какому боту писать."""
    if amm is mm:
        if _try_admin_command(text, user_id, channel_id):
            return
    elif _is_admin_command(text):
        mm.post(channel_id, T.ADMIN_MOVED.format(bot=amm.username or "админ-бот")
                if _admin_name(user_id) else T.ADMIN_ONLY)
        return
    _menu_for(channel_id, user_id)


def _on_admin_bot_message(text, user_id, channel_id):
    """Сообщение АДМИН-БОТУ: команда, а на любой другой текст — панель. Только админам."""
    if _try_admin_command(text, user_id, channel_id):
        return
    if not _admin_name(user_id):
        amm.post(channel_id, T.ADMIN_ONLY)
        return
    amm.post(channel_id, message="", props={"attachments": admin.panel(conn, cfg, amm)})


async def _handle_event(raw, client, on_message):
    try:
        event = json.loads(raw)
    except (TypeError, ValueError):
        return
    if event.get("event") != "posted":
        return
    d = event.get("data", {})
    # общаемся ТОЛЬКО в ЛС (channel_type == "D")
    if d.get("channel_type") != "D":
        return
    try:
        post = json.loads(d["post"])
    except (KeyError, TypeError, ValueError):
        return
    if post.get("user_id") == client.bot_user_id:  # не отвечаем на свои же посты
        return
    if str(post.get("type") or "").startswith("system_"):
        return
    if str((post.get("props") or {}).get("from_bot") or "").lower() == "true":
        return                                     # ботам не отвечаем: два бота зациклились бы
    # Всё, что ходит в сеть (профиль для лога, сам обработчик — Tag/Talk через requests),
    # — в отдельном потоке: event loop остаётся свободным и отвечает на ping сервера.
    # Если бы он стоял, пока Tag отвечает медленно, MM закрыл бы соединение и сообщения
    # за время переподключения потерялись. Ждём поток здесь же — порядок сообщений
    # сохраняется.
    await asyncio.to_thread(_process_post, post, client, on_message)


def _process_post(post, client, on_message):
    """Личное сообщение боту (в потоке). Ошибка одного сообщения (напр. сбой сети к Tag)
    НЕ роняет WebSocket-сессию."""
    try:
        text = (post.get("message") or "").strip()
        log.info("сообщение боту @%s от %s: %s", client.username, _who(post.get("user_id")),
                 text[:200] + ("…" if len(text) > 200 else ""))
        on_message(text, post["user_id"], post["channel_id"])
    except Exception:
        log.exception("обработка сообщения от %s не удалась", post.get("user_id"))


_WS_RETRY_SEC = 5
# Пока Tag недоступен, повтор каждые 5 с; в лог — первая неудача и дальше раз в минуту.
_WS_LOG_EVERY = 12


def _is_hello(raw):
    """Событие hello: сервер принял аутентификацию — соединение рабочее."""
    try:
        return json.loads(raw).get("event") == "hello"
    except (TypeError, ValueError, AttributeError):
        return False


async def _ws_loop(client, on_message):
    """Слушать ЛС одного бота (у каждой учётки Tag — своё WebSocket-соединение).

    Связь держится вечно: при обрыве — повтор каждые 5 с. Рабочим соединение считается
    после события hello (сервер принял токен): так обрывы сразу после подключения
    (например, неверный токен) не превращаются в пару строк лога каждые 5 с."""
    ws_scheme = "wss" if client.scheme == "https" else "ws"
    url = f"{ws_scheme}://{client.host}:{client.port}/api/v4/websocket"
    if client.scheme == "https":
        sslctx = ssl.create_default_context()
        if not client.verify:
            sslctx.check_hostname = False
            sslctx.verify_mode = ssl.CERT_NONE
        connect_kwargs = {"ssl": sslctx}
    else:
        connect_kwargs = {}

    import websockets
    failures = 0
    while True:
        try:
            async with websockets.connect(url, **connect_kwargs) as ws:
                # аутентификация бота в сокете
                await ws.send(json.dumps({"seq": 1, "action": "authentication_challenge",
                                          "data": {"token": client.token}}))
                online = False
                async for raw in ws:
                    if not online and _is_hello(raw):
                        online = True
                        if failures:
                            log.info("WebSocket @%s: связь восстановлена (неудачных попыток: %d)",
                                     client.username, failures)
                        else:
                            log.info("WebSocket подключён (@%s): %s", client.username, url)
                        failures = 0
                    await _handle_event(raw, client, on_message)
            reason = "соединение закрыто сервером"
        except Exception as ex:
            reason = f"{type(ex).__name__}: {' '.join(str(ex).split())[:200]}"
        failures += 1
        if failures == 1 or failures % _WS_LOG_EVERY == 0:
            log.warning("WebSocket @%s: нет связи (%s) — переподключаюсь каждые %d с (попытка %d)",
                        client.username, reason, _WS_RETRY_SEC, failures)
        await asyncio.sleep(_WS_RETRY_SEC)


def _run_ws(client, on_message):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_ws_loop(client, on_message))
    except Exception:
        # сюда попадаем, только если сломалось само подключение (не сеть — её цикл переживает)
        log.exception("WebSocket @%s остановлен: личные сообщения этому боту не принимаются",
                      client.username)


# =========================================================================
#  ТОЧКА ВХОДА
# =========================================================================

def main():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(log_dir, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[RotatingFileHandler(os.path.join(log_dir, "bot.log"),
                                      maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8"),
                  logging.StreamHandler()])
    # werkzeug пишет строку каждого запроса (с ?t=<секрет>) — глушим access-лог,
    # чтобы BUTTON_SECRET не утекал в bot.log.
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    # Ошибки — одной строкой (трейсбек сворачивается); LOG_TRACEBACKS=1 — полные трейсбеки.
    if logsetup.condense_tracebacks():
        log.warning("LOG_TRACEBACKS=1 — в логах будут полные трейсбеки (режим отладки)")

    bots = [("бот", "BOT_TOKEN", mm)] + ([("админ-бот", "ADMIN_BOT_TOKEN", amm)] if amm is not mm else [])
    for name, var, client in bots:
        try:
            client.login()
        except Exception as ex:
            raise SystemExit(f"[!] {name}: не удалось подключиться к Tag ({client.api}): {ex}\n"
                             f"    Проверь URL/PORT/{var}/сеть (VPN) и запусти снова.")
        log.info("%s запущен: @%s (id=%s), Tag=%s", name, client.username, client.bot_user_id,
                 client.api)
    if amm is mm:
        log.warning("ADMIN_BOT_TOKEN не задан — отдельного админ-бота нет: админ-панель и "
                    "админ-команды работают в основном боте.")
    if not mm.secret:
        log.warning("BUTTON_SECRET не задан — эндпоинты /action и /dialog открыты без "
                    "проверки адреса. Задай BUTTON_SECRET в .env для защиты.")
    # суперадмин: хранится в БД; ADMIN_USERNAME назначает его при первом запуске и когда
    # значение в .env изменили (см. admin.bootstrap_superadmin)
    sup = admin.bootstrap_superadmin(conn, cfg)
    if sup:
        log.info("суперадмин: @%s; админов всего: %d", sup, len(db.list_admins(conn)))
    else:
        log.warning("ADMIN_USERNAME не задан — суперадмина нет, админ-панель и команды "
                    "(archiever_admin, restart_passed, newadmin/remember/delete) недоступны.")

    threading.Thread(target=_run_ws, args=(mm, _on_user_bot_message), daemon=True).start()
    if amm is not mm:
        threading.Thread(target=_run_ws, args=(amm, _on_admin_bot_message), daemon=True).start()
    threading.Thread(target=_admin_requests_loop, name="admin-requests", daemon=True).start()
    app.run(host="0.0.0.0", port=cfg.mm.button_port)


if __name__ == "__main__":
    main()
