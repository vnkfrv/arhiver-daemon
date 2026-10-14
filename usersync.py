"""
usersync.py — наполнение таблицы users (участники конференций) при обработке записей.

Для единоразового old-records-уведомления нужен список пользователей с именами и
раздельными флагами (Tag/почта). Заводим их по мере обработки записей — из участников:
  • login (в users)  = username в Tag = тело корпоративной почты до «@»;
  • имя/фамилия       — из Тага (mm.user_by_username), надёжнее; фоллбэк — ФИО из Talk;
  • деактивированные (Talk disabled:true) — ПРОПУСКАЕМ, в БД не заводим.
"""

import logging

import db
from util import err as _err, username_from_email

log = logging.getLogger("usersync")


def sync_user(conn, participant, mm=None):
    """Завести/обновить пользователя из участника записи. participant — dict
    {name, email, login, disabled} из TalkClient.participants_with_emails.
    Возвращает username (login в users) или None, если пропущен."""
    email = participant.get("email")
    if not email:
        return None                              # без почты слать некуда — не заводим
    if participant.get("disabled"):
        return None                              # деактивированный аккаунт — пропуск
    username = username_from_email(email)
    if not username:
        return None
    if db.get_user(conn, username):
        return username                          # уже есть — не дёргаем API повторно

    first = last = None
    if mm is not None:                           # имя из Тага (приоритетно)
        try:
            mu = mm.user_by_username(username)
            if not mu.get("delete_at"):          # 0 = активен в Tag
                first = mu.get("first_name") or None
                last = mu.get("last_name") or None
        except Exception:
            pass
    if not (first or last):                      # фоллбэк — ФИО из Talk («Имя Фамилия»)
        parts = (participant.get("name") or "").split()
        first = parts[0] if parts else None
        last = " ".join(parts[1:]) if len(parts) > 1 else None

    db.upsert_user(conn, login=username, email=email, first_name=first, last_name=last)
    log.debug("user заведён: %s (%s %s)", username, first or "", last or "")
    return username


def sync_participants(conn, participants, mm=None):
    """Прогнать всех участников записи через sync_user (ошибки одного не рвут остальных)."""
    for p in participants or []:
        try:
            sync_user(conn, p, mm)
        except Exception as ex:
            # не заведён — не получит единоразовое уведомление о старых записях: это должно
            # быть видно в логе (попробуем снова на следующей записи с этим участником)
            log.warning("пользователь %s не заведён в базе: %s", p.get("email"), _err(ex))
