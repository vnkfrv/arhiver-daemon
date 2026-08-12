"""
notify.py — РАССЫЛКА УВЕДОМЛЕНИЙ участникам в Tag (Mattermost).

Формирует текст сообщения ровно по ТЗ и рассылает его участникам записи. Если передан
клиент `mm` (MMClient) — реально отправляет ЛС с кнопками «Видеозапись» / «Вся
расшифровка» / «Короткая выжимка»; если `mm=None` (SEND_TO_TAG=0) — только пишет в лог,
кому бы отправил (безопасный режим, никто ничего не получает).

Здесь же зафиксированы шаблон сообщения и правило username = тело почты до «@».
"""

import logging

from texts import MESSAGE_TEMPLATE, MAIL_SUBJECT, OLD_RECORDS_MESSAGE, MAIL_SUBJECT_OLD

log = logging.getLogger("notify")


def username_from_email(email):
    """username в Tag = тело корпоративной почты до «@»."""
    return (email or "").split("@", 1)[0]


def build_message(name, title, date_str, minutes, delete_dt_str):
    return MESSAGE_TEMPLATE.format(name=name, title=title, date=date_str,
                                   minutes=minutes, delete_dt=delete_dt_str)


def _mail_subject(title, date_str):
    return MAIL_SUBJECT.format(title=title, date=date_str)


def send_old_records_notifications(users, *, mm=None, send_mail=False):
    """
    Разослать ЕДИНОРАЗОВОЕ уведомление о старых записях. Для каждого пользователя шлём
    по каналам, где флаг ещё не выставлен: Tag (если mm задан) и/или почта (send_mail).

    users — list[dict] из db (login, email, first_name, notified_of_old_records_tag/_mail).
    Возвращает list[(login, tag_ok, mail_ok)], где *_ok = True (отправлено) / False (сбой) /
    None (канал не требовался или уже был уведомлён). Флаги в БД проставляет вызывающий.
    """
    results = []
    for u in users:
        login = u.get("login")
        name = u.get("first_name") or login
        text = OLD_RECORDS_MESSAGE.format(name=name)
        tag_ok = mail_ok = None
        # --- канал 1: Tag ---
        if mm is not None and not u.get("notified_of_old_records_tag"):
            try:
                mm.send_old_records_dm(login, text)
                tag_ok = True
            except Exception as ex:
                tag_ok = False
                log.warning("  [old-records] Tag: не удалось @%s: %s", login, ex)
        # --- канал 2: почта ---
        if send_mail and not u.get("notified_of_old_records_mail"):
            email = u.get("email")
            try:
                from mailer import send_email
                mail_ok = bool(send_email(MAIL_SUBJECT_OLD, text, email))
                if not mail_ok:
                    log.warning("  [old-records] почта: не отправлено на %s", email)
            except Exception as ex:
                mail_ok = False
                log.warning("  [old-records] почта: сбой на %s: %s", email, ex)
        results.append((login, tag_ok, mail_ok))
    return results


def send_archive_notifications(participants, *, title, date_str, minutes,
                               delete_dt_str, recording_key, mm=None, already=None,
                               send_mail=False):
    """
    Разослать уведомления участникам записи. ИДЕМПОТЕНТНО: тех, кто уже в `already`
    (usernames), пропускаем — чтобы при повторе тика не слать дубли тем, кому уже
    отправили на прошлой (частично упавшей) попытке.

    Каналы независимы: Tag (если mm задан) и/или почта (если send_mail=True). Участник
    считается уведомлённым только если ВСЕ включённые каналы отработали (иначе стадию
    повторим — но только для НЕуведомлённых, за счёт `already`).

    participants — [{"name":..., "email":...}]; mm — клиент Tag или None (лог-заглушка).
    Возвращает (all_ok, notified): all_ok=False, если кому-то не удалось отправить;
    notified — usernames, кому отправили в этот раз (по всем включённым каналам).
    """
    already = set(already or [])
    notified, all_ok = [], True
    subject = _mail_subject(title, date_str)
    for p in participants:
        email = p.get("email")
        if not email:
            log.warning("  [notify] у участника %r нет почты — пропуск", p.get("name"))
            continue
        username = username_from_email(email)
        if username in already:
            continue
        text = build_message(p.get("name") or username, title, date_str, minutes,
                             delete_dt_str)
        ok = True
        # --- канал 1: Tag ---
        try:
            if mm is None:
                log.info("  [notify:ЗАГЛУШКА Tag] -> @%s (%s) по записи %s\n%s",
                         username, email, recording_key, text)
            else:
                mm.send_archive_dm(username, text, recording_key)
        except Exception as ex:
            ok = False
            log.warning("  [notify] Tag: не удалось отправить @%s: %s", username, ex)
        # --- канал 2: почта (SMTP) ---
        if send_mail:
            try:
                from mailer import send_email
                if not send_email(subject, text, email):
                    ok = False
            except Exception as ex:
                ok = False
                log.warning("  [notify] почта: не удалось отправить на %s: %s", email, ex)
        if ok:
            notified.append(username)
        else:
            all_ok = False
    return all_ok, notified
