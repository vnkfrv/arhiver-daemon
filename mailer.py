"""
mailer.py — отправка уведомлений участникам на КОРПОРАТИВНУЮ ПОЧТУ по SMTP.

Механика взята из 04_feedback_bot/send_email.py, но получатель — параметр (шлём каждому
участнику на его email из таблицы participant), а конфиг читается через load_env (чтобы
работал и в контейнере, где .env-файла нет — переменные приходят из окружения).

Конфиг (env/.env):
    SEND_TO_MAIL       — 1/0, включает рассылку на почту (проверяет вызывающий код)
    SMTP_SERVER        — хост SMTP (напр. smtp.corp.tander.ru)
    SMTP_PORT          — порт (587 для starttls, 465 для ssl, 25 для none)
    SMTP_SECURITY      — starttls | ssl | none (по умолч. starttls)
    SMTP_SENDER        — адрес отправителя (поле From)
    SMTP_LOGIN         — логин для авторизации (по умолч. = SMTP_SENDER)
    SMTP_PASSWORD      — пароль (если пусто — без авторизации)
"""

import logging
import smtplib
import ssl
from email.mime.text import MIMEText

from talk_api import load_env

log = logging.getLogger("mailer")


def _config():
    """Собрать и провалидировать SMTP-конфиг из env. None — если настроено неполно/криво."""
    env = load_env()
    cfg = {
        "server": (env.get("SMTP_SERVER") or "").strip(),
        "port": (env.get("SMTP_PORT") or "").strip(),
        "security": (env.get("SMTP_SECURITY") or "starttls").strip().lower(),
        "sender": (env.get("SMTP_SENDER") or "").strip(),
        "login": (env.get("SMTP_LOGIN") or env.get("SMTP_SENDER") or "").strip(),
        "password": env.get("SMTP_PASSWORD") or "",
    }
    if not (cfg["server"] and cfg["port"] and cfg["sender"]):
        log.error("SMTP не настроен: заполни SMTP_SERVER/SMTP_PORT/SMTP_SENDER в .env")
        return None
    try:
        cfg["port"] = int(cfg["port"])
    except (TypeError, ValueError):
        log.error("SMTP_PORT должен быть числом, получено: %r", cfg["port"])
        return None
    if cfg["security"] not in ("starttls", "ssl", "none"):
        log.error("SMTP_SECURITY: ожидается starttls/ssl/none, получено %r", cfg["security"])
        return None
    return cfg


def _tls_context():
    # Корп-SMTP часто с внутренним сертификатом — не валимся на проверке хоста/цепочки.
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def send_email(subject, body, recipient):
    """Отправить письмо одному получателю. Возвращает True/False (успех отправки).
    Ничего не бросает наружу — только логирует; вызывающий решает, что делать с False."""
    cfg = _config()
    if cfg is None:
        return False
    if not recipient:
        log.warning("SMTP: пустой адрес получателя — пропуск")
        return False

    msg = MIMEText(body, _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = cfg["sender"]
    msg["To"] = recipient

    try:
        if cfg["security"] == "ssl":
            server = smtplib.SMTP_SSL(cfg["server"], cfg["port"], timeout=15,
                                      context=_tls_context())
        else:
            server = smtplib.SMTP(cfg["server"], cfg["port"], timeout=15)
        with server:
            if cfg["security"] == "starttls":
                server.starttls(context=_tls_context())
            if cfg["password"]:
                server.login(cfg["login"], cfg["password"])
            server.send_message(msg)
        log.info("SMTP: письмо отправлено %s -> %s", cfg["sender"], recipient)
        return True
    except Exception as ex:
        log.error("SMTP: не удалось отправить письмо на %s (%s:%s, %s): %s",
                  recipient, cfg["server"], cfg["port"], cfg["security"], ex)
        return False
