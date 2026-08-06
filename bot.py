#!/usr/bin/env python3
"""
bot.py — ИНТЕРАКТИВНЫЙ БОТ Tag (Mattermost). Этап B.

Отдельный процесс (демон main.py только РАССЫЛАЕТ ЛС с кнопками; всё, что происходит
по нажатию, обрабатывает этот сервер).

Что умеет:
  1. ЛС + любое сообщение боту -> help-сообщение с кнопкой «Найти видеозапись».
     (Бот общается ТОЛЬКО в ЛС; сообщения из каналов игнорирует.)
  2. Кнопки уведомления «Видеозапись»/«Вся расшифровка»/«Короткая выжимка»
     -> соответствующий файл В ВЕТКУ. Если видео уже удалено — «Видеозапись была
     уже удалена😔» (1 раз на ветку).
  3. Кнопка «Найти видеозапись» -> модалка «Видеозаписи» с двумя select-полями
     (запись из БД пользователя + действие). После подтверждения: «Скоро пришлю
     файл….» и затем сам файл В ТУ ЖЕ ВЕТКУ.

Тяжёлые операции (скачивание/загрузка файлов, особенно видео) выполняются в фоновом
потоке, чтобы быстро ответить на callback интеграции и не упереться в её таймаут.

Зависимости: flask, websockets, requests. Запуск:
    python bot.py
"""

import asyncio
import json
import logging
import os
import re
import ssl
import tempfile
import threading
from datetime import datetime, timedelta, timezone

from flask import Flask, request, jsonify

import db
import statuses
from config import load_cfg
from mm_client import MMClient, FIND_ACTIONS
from talk_api import TalkClient

log = logging.getLogger("bot")

# Админ-команды в ЛС (текстом). {nickname}/{ID} — «хвост» после подчёркивания.
RE_NEWADMIN = re.compile(r"^newadmin_(.+)$", re.IGNORECASE)
RE_REMEMBER = re.compile(r"^remember_(.+)$", re.IGNORECASE)
RE_DELETE = re.compile(r"^delete_(.+)$", re.IGNORECASE)

HELP_TEXT = (
    "Привет, {name}! 👋\n\n"
    "Бот-архиватор удаляет записи видеоконференций Talk, записанные более года "
    "назад и предоставляет возможность скачивания полной текстовой расшифровки и "
    "сжатой выжимки, которые подготавливаются на следующий день после "
    "видеоконференции и будут храниться 5 лет после удаления видеозаписи"
)
ALREADY_DELETED = "Видеозапись была уже удалена😔"
SOON = "Скоро пришлю файл…."

cfg = load_cfg()
conn = db.connect(cfg.db_path)
mm = MMClient(cfg.mm, verify=cfg.mm.verify)
talk = TalkClient()
app = Flask(__name__)


# =========================================================================
#  ДОСТАВКА ФАЙЛОВ (в фоне)
# =========================================================================

def _fmt_option(row):
    """Подпись записи в select: «{название} - ДД.ММ.ГГГГ»."""
    d = row.get("created_date") or ""
    try:
        dt = datetime.fromisoformat(d.replace("Z", "+00:00")).astimezone().strftime("%d.%m.%Y")
    except ValueError:
        dt = d[:10]
    return f"{row.get('title') or 'без названия'} - {dt}"


def _deliver(channel_id, root_id, key, action):
    """Положить нужный файл в ветку root_id. Выполняется в отдельном потоке."""
    try:
        row = db.get(conn, key)
        if not row:
            mm.post(channel_id, "Запись не найдена в базе.", root_id=root_id)
            return

        if action in ("full", "short"):
            path = row["full_path"] if action == "full" else row["short_path"]
            if path and os.path.exists(path):
                mm.post_file(channel_id, path, root_id=root_id)
            else:
                mm.post(channel_id, "Файл ещё не готов.", root_id=root_id)
            return

        if action == "video":
            if row.get("deleted"):
                # ответ «уже удалено» — не чаще одного раза на ветку
                if not db.already_replied_deleted(conn, root_id):
                    mm.post(channel_id, ALREADY_DELETED, root_id=root_id)
                    db.mark_replied_deleted(conn, root_id)
                return
            # уникальный временный файл: одновременные запросы одной записи не мешают
            fd, tmp = tempfile.mkstemp(prefix=f"{key}_", suffix=".mp4")
            os.close(fd)
            try:
                talk.download_video(key, tmp)
                mm.post_file(channel_id, tmp, root_id=root_id)
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
    except Exception as ex:
        log.exception("доставка файла (%s, %s) не удалась: %s", key, action, ex)
        try:
            mm.post(channel_id, "Не удалось отправить файл, попробуй ещё раз.", root_id=root_id)
        except Exception:
            pass


def _deliver_async(channel_id, root_id, key, action):
    threading.Thread(target=_deliver, args=(channel_id, root_id, key, action),
                     daemon=True).start()


# =========================================================================
#  HTTP: нажатия кнопок и модалка
# =========================================================================

def _authorized():
    """Проверить общий секрет в query (?t=...). Так отсекаем запросы не по нашему URL.
    Если секрет не задан в конфиге — пропускаем (но это НЕ рекомендуется в проде)."""
    if not mm.secret:
        return True
    return request.args.get("t") == mm.secret


@app.route("/action", methods=["POST"])
def action():
    """Callback интерактивных кнопок (уведомление + «Найти видеозапись»)."""
    if not _authorized():
        return jsonify({"error": "unauthorized"}), 401
    try:
        data = request.get_json(force=True, silent=True) or {}
        ctx = data.get("context") or {}
        act = ctx.get("action")
        if act in ("video", "full", "short"):
            _deliver_async(data.get("channel_id"), data.get("post_id"), ctx.get("key"), act)
        elif act == "find":
            _open_find_dialog(data)
    except Exception as ex:
        log.warning("/action не обработан: %s", ex)
    return jsonify({})            # Tag ждёт быстрый 200; ошибки не показываем пользователю


def _open_find_dialog(data):
    """Открыть модалку «Видеозаписи» для пользователя, нажавшего «Найти видеозапись»."""
    user_id = data.get("user_id")
    channel_id = data.get("channel_id")
    post_id = data.get("post_id")
    trigger_id = data.get("trigger_id")
    try:
        email = mm.user(user_id).get("email")
    except Exception:
        email = None
    rows = db.list_for_email(conn, email) if email else []

    if not rows:
        mm.post(channel_id, "Пока нет доступных видеозаписей.", root_id=post_id)
        return

    dialog = {
        "callback_id": "find_video",
        "title": "Видеозаписи",
        "submit_label": "Готово",
        # state несёт, куда потом класть файл (в ту же ветку)
        "state": json.dumps({"channel_id": channel_id, "root_id": post_id}),
        "elements": [
            {"display_name": "Выберите видеозапись", "name": "recording", "type": "select",
             "options": [{"text": _fmt_option(r), "value": r["recording_key"]} for r in rows]},
            {"display_name": "Выберите действие", "name": "do", "type": "select",
             "options": [{"text": label, "value": val} for val, label in FIND_ACTIONS]},
        ],
    }
    mm.open_dialog(trigger_id, mm.dialog_url(), dialog)


@app.route("/dialog", methods=["POST"])
def dialog():
    """Отправка модалки «Найти видеозапись»."""
    if not _authorized():
        return jsonify({"error": "unauthorized"}), 401
    try:
        data = request.get_json(force=True, silent=True) or {}
        if not data.get("cancelled"):
            sub = data.get("submission") or {}
            try:
                state = json.loads(data.get("state") or "{}")
            except ValueError:
                state = {}
            channel_id = state.get("channel_id") or data.get("channel_id")
            root_id = state.get("root_id")
            mm.post(channel_id, SOON, root_id=root_id)
            _deliver_async(channel_id, root_id, sub.get("recording"), sub.get("do"))
    except Exception as ex:
        log.warning("/dialog не обработан: %s", ex)
    return jsonify({})


@app.route("/health", methods=["GET"])
def health():
    return "ok", 200


# =========================================================================
#  WEBSOCKET: любое сообщение боту в ЛС -> help-сообщение
# =========================================================================

def _help_for(channel_id, user_id):
    try:
        name = mm.user(user_id).get("first_name") or "коллега"
    except Exception:
        name = "коллега"
    mm.post(channel_id, message="",
            props={"attachments": mm.help_attachment(HELP_TEXT.format(name=name))})


# =========================================================================
#  АДМИН-КОМАНДЫ (текстом в ЛС)
# =========================================================================

def _username(user_id):
    try:
        return mm.user(user_id).get("username")
    except Exception:
        return None


def _fmt_ymd(iso):
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone().strftime("%d.%m.%Y")
    except (ValueError, AttributeError):
        return iso


def _cmd_newadmin(nickname, by_username, channel_id):
    target = nickname.strip().lstrip("@")
    try:
        mm.user_by_username(target)                 # убедимся, что пользователь есть в Tag
    except Exception:
        mm.post(channel_id, f"Пользователь @{target} не найден в Tag.")
        return
    db.add_admin(conn, target, by_username)
    mm.post(channel_id, f"Готово: @{target} теперь администратор (добавил @{by_username}).")


def _cmd_remember(key, channel_id):
    row = db.get(conn, key)
    if not row:
        mm.post(channel_id, f"Запись `{key}` не найдена в базе.")
        return
    if row.get("deleted"):
        mm.post(channel_id, f"Видео `{key}` уже удалено — защищать нечего.")
        return
    db.update(conn, key, protected=1)
    db.wake(conn, key)                              # демон переоценит запись
    mm.post(channel_id, f"Запись «{row.get('title') or key}» помечена как неудаляемая — "
                        f"видео сохранится в Толке навсегда.")


def _cmd_delete(key, channel_id):
    """Ручное удаление видео админом. Работает для ЛЮБОГО видео, включая защищённое
    (remember): ручное удаление имеет приоритет и снимает защиту."""
    row = db.get(conn, key)
    if not row:
        mm.post(channel_id, f"Запись `{key}` не найдена в базе.")
        return
    if row.get("deleted"):
        mm.post(channel_id, f"Видео `{key}` уже удалено.")
        return
    was_protected = bool(row.get("protected"))
    try:
        talk.delete_recording(key, force=cfg.force_delete)
    except Exception as ex:
        mm.post(channel_id, f"Не удалось удалить видео `{key}`: {ex}")
        return
    now = datetime.now(timezone.utc)
    text_delete_at = (now + timedelta(days=cfg.text_retention_days)).isoformat()
    db.update(conn, key, deleted=1, deleted_at=now.isoformat(),
              text_delete_at=text_delete_at, protected=0, status=statuses.DELETED)
    db.wake(conn, key)                              # демон подхватит: запланирует очистку текстов
    note = " (защита remember снята)" if was_protected else ""
    mm.post(channel_id, f"Видео `{key}` удалено вручную{note}. Тексты будут храниться до "
                        f"{_fmt_ymd(text_delete_at)}.")


def _try_admin_command(text, user_id, channel_id):
    """Разобрать и выполнить админ-команду. Возвращает True, если это была команда."""
    m_new, m_rem, m_del = (RE_NEWADMIN.match(text), RE_REMEMBER.match(text),
                           RE_DELETE.match(text))
    if not (m_new or m_rem or m_del):
        return False
    username = _username(user_id)
    if not username or not db.is_admin(conn, username):
        mm.post(channel_id, "Команда доступна только администраторам.")
        return True
    if m_new:
        _cmd_newadmin(m_new.group(1), username, channel_id)
    elif m_rem:
        _cmd_remember(m_rem.group(1).strip(), channel_id)
    else:
        _cmd_delete(m_del.group(1).strip(), channel_id)
    return True


async def _handle_event(raw):
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
    if post.get("user_id") == mm.bot_user_id:      # не отвечаем на свои же посты
        return
    if str(post.get("type") or "").startswith("system_"):
        return
    text = (post.get("message") or "").strip()
    # сначала пробуем админ-команду; если не она — обычное help-сообщение.
    # Оборачиваем: ошибка обработки одного сообщения (напр. сбой сети к Tag) НЕ должна
    # ронять всю WebSocket-сессию.
    try:
        if not _try_admin_command(text, post["user_id"], post["channel_id"]):
            _help_for(post["channel_id"], post["user_id"])
    except Exception as ex:
        log.warning("обработка сообщения от %s не удалась: %s", post.get("user_id"), ex)


async def _ws_loop():
    ws_scheme = "wss" if mm.scheme == "https" else "ws"
    url = f"{ws_scheme}://{mm.host}:{mm.port}/api/v4/websocket"
    if mm.scheme == "https":
        sslctx = ssl.create_default_context()
        if not mm.verify:
            sslctx.check_hostname = False
            sslctx.verify_mode = ssl.CERT_NONE
        connect_kwargs = {"ssl": sslctx}
    else:
        connect_kwargs = {}

    import websockets
    while True:
        try:
            async with websockets.connect(url, **connect_kwargs) as ws:
                # аутентификация бота в сокете
                await ws.send(json.dumps({"seq": 1, "action": "authentication_challenge",
                                          "data": {"token": cfg.mm.bot_token}}))
                log.info("WebSocket подключён: %s", url)
                async for raw in ws:
                    await _handle_event(raw)
        except Exception as ex:
            log.warning("WebSocket переподключение через 5с (%s: %s)",
                        type(ex).__name__, ex)
            await asyncio.sleep(5)


def _run_ws():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_ws_loop())


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
        handlers=[logging.FileHandler(os.path.join(log_dir, "bot.log")),
                  logging.StreamHandler()])
    # werkzeug пишет строку каждого запроса (с ?t=<секрет>) — глушим access-лог,
    # чтобы BUTTON_SECRET не утекал в bot.log.
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    try:
        me = mm.login()
    except Exception as ex:
        raise SystemExit(f"[!] Не удалось подключиться к Tag ({mm.api}): {ex}\n"
                         f"    Проверь URL/PORT/BOT_TOKEN/сеть (VPN) и запусти снова.")
    log.info("бот запущен: @%s (id=%s), Tag=%s", me.get("username"), mm.bot_user_id, mm.api)
    if not mm.secret:
        log.warning("BUTTON_SECRET не задан — эндпоинты /action и /dialog открыты без "
                    "проверки. Задай BUTTON_SECRET в .env для защиты.")
    # bootstrap-админ: первый администратор из ADMIN_USERNAME (может добавлять остальных)
    if cfg.admin_username:
        db.add_admin(conn, cfg.admin_username, "system")
        log.info("bootstrap-админ: @%s", cfg.admin_username)
    else:
        log.warning("ADMIN_USERNAME не задан — админ-команды (newadmin/remember/delete) "
                    "будут недоступны, пока не появится хотя бы один админ.")

    threading.Thread(target=_run_ws, daemon=True).start()
    app.run(host="0.0.0.0", port=cfg.mm.button_port)


if __name__ == "__main__":
    main()
