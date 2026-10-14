"""
notify.py — РАССЫЛКА УВЕДОМЛЕНИЙ участникам в Tag (Mattermost) и/или на почту.

Формирует текст сообщения ровно по ТЗ и рассылает его участникам записи. Каналы
независимы: Tag (ЛС с кнопками скачивания) и почта (SMTP). Если Tag выключен
(SEND_TO_TAG=0) — вместо ЛС пишем в лог, кому бы отправили (безопасный режим).

Кому доставить не удалось — возвращаем вызывающему: pipeline повторит по лестнице, а
остальным участникам в самом уведомлении называем тех, кто его не получит.

Правило «username = тело почты до @» — в util.username_from_email.
"""

import logging
from dataclasses import dataclass, field

from mm_client import UserUnavailable
from texts import (MESSAGE_TEMPLATE, MAIL_SUBJECT, OLD_RECORDS_MESSAGE, MAIL_SUBJECT_OLD,
                   UNREACHABLE_NOTE)
from util import err as _err, username_from_email

log = logging.getLogger("notify")

_NOTE_MAX_NAMES = 10      # сколько недоступных участников перечислять в уведомлении поимённо


def build_message(name, title, date_str, minutes, delete_dt_str):
    return MESSAGE_TEMPLATE.format(name=name, title=title, date=date_str,
                                   minutes=minutes, delete_dt=delete_dt_str)


def _mail_subject(title, date_str):
    return MAIL_SUBJECT.format(title=title, date=date_str)


def _note(labels):
    """Приписка к уведомлению: кому его отправить не удалось (или '' — если таких нет)."""
    if not labels:
        return ""
    who = ", ".join(labels[:_NOTE_MAX_NAMES])
    if len(labels) > _NOTE_MAX_NAMES:
        who += f" и ещё {len(labels) - _NOTE_MAX_NAMES}"
    return UNREACHABLE_NOTE.format(who=who)


def send_old_records_notifications(users, *, mm=None, tag_enabled=False, send_mail=False):
    """
    Разослать ЕДИНОРАЗОВОЕ уведомление о старых записях. Для каждого пользователя шлём
    по каналам, где флаг ещё не выставлен: Tag (tag_enabled) и/или почта (send_mail).

    users — list[dict] из db (login, email, first_name, notified_of_old_records_tag/_mail).
    Возвращает list[dict]: {"login", "tag", "mail", "error"}, где tag/mail = True
    (отправлено) / False (не вышло) / None (канал не требовался или уже был уведомлён), а
    error=True — не вышло из-за сбоя связи (а не потому, что пользователя нет в Tag).
    Флаги в БД проставляет вызывающий.
    """
    results = []
    for u in users:
        login = u.get("login")
        name = u.get("first_name") or login
        text = OLD_RECORDS_MESSAGE.format(name=name)
        tag_ok = mail_ok = None
        error = False
        # --- канал 1: Tag ---
        if tag_enabled and not u.get("notified_of_old_records_tag"):
            tag_ok = False
            if mm is None:                       # Tag включён, но клиент не поднят (сбой связи)
                error = True
            else:
                try:
                    mm.send_old_records_dm(login, text, email=u.get("email"))
                    tag_ok = True
                except UserUnavailable as ex:
                    log.warning("  [old-records] Tag: @%s — %s", login, ex)
                except Exception as ex:
                    error = True
                    log.warning("  [old-records] Tag: не удалось @%s: %s", login, _err(ex))
        # --- канал 2: почта ---
        if send_mail and not u.get("notified_of_old_records_mail"):
            email = u.get("email")
            try:
                from mailer import send_email
                mail_ok = bool(send_email(MAIL_SUBJECT_OLD, text, email))
            except Exception as ex:
                mail_ok = False
                log.warning("  [old-records] почта: сбой на %s: %s", email, _err(ex))
            if not mail_ok:
                error = True
                log.warning("  [old-records] почта: не отправлено на %s", email)
        results.append({"login": login, "tag": tag_ok, "mail": mail_ok, "error": error})
    return results


@dataclass
class NotifyResult:
    """Итог одного прохода рассылки по записи."""
    tag_sent: list = field(default_factory=list)    # usernames: в этот проход доставлено в Tag
    mail_sent: list = field(default_factory=list)   # usernames: в этот проход ушло письмо
    pending: list = field(default_factory=list)     # кому доставлено НЕ по всем каналам: описания
    unreachable: list = field(default_factory=list)  # кто не получил уведомление НИКАК (подписи)
    has_errors: bool = False     # среди неудач есть сбои связи (а не «нет в Tag/нет почты»)
    reached_any: bool = False    # хоть один участник реально получил уведомление

    @property
    def complete(self):
        """Всем, кому было куда слать, доставлено по всем включённым каналам."""
        return not self.pending


def send_archive_notifications(participants, *, title, date_str, minutes, delete_dt_str,
                               recording_key, mm=None, tag_enabled=False, send_mail=False,
                               tag_done=None, mail_done=None):
    """
    Разослать уведомления участникам записи. ИДЕМПОТЕНТНО по каждому каналу: кому уже
    доставлено в Tag (tag_done) или письмом (mail_done) — повторно не шлём, так что при
    повторе стадии дублей нет.

    Каналы: Tag — если tag_enabled (mm=None при этом значит «Tag сейчас недоступен» —
    сбой); почта — если send_mail. При tag_enabled=False ЛС заменяет строка в логе
    (заглушка, считается доставленной). Участник без почты уведомлён быть не может: стадию
    он не держит, но попадает в список неполучивших.

    Порядок важен: сначала выясняем, кого удастся достать в Tag, и только потом шлём —
    тогда в самом уведомлении можно назвать тех, кто его не получит.

    participants — [{"name":..., "email":...}]. Возвращает NotifyResult.
    """
    tag_done, mail_done = set(tag_done or []), set(mail_done or [])
    subject = _mail_subject(title, date_str)

    people, no_address, seen = [], [], set()
    for p in participants:
        email = p.get("email")
        if not email:
            log.warning("  [notify] у участника %r нет почты — уведомить некуда", p.get("name"))
            no_address.append(p.get("name") or "?")
            continue
        username = username_from_email(email)
        if username in seen:
            continue
        seen.add(username)
        people.append({"name": p.get("name") or username, "email": email, "username": username})

    def text_for(person, note):
        return build_message(person["name"], title, date_str, minutes, delete_dt_str) + note

    def label(person):
        return f"{person['name']} (@{person['username']})"

    fails = {}                  # username -> [(сбой связи?, причина)]

    def failed(person, is_error, reason):
        fails.setdefault(person["username"], []).append((is_error, reason))

    # --- шаг 1: кого можно достать в Tag (канал ЛС) ---
    channels = {}               # username -> channel_id
    if tag_enabled:
        for person in people:
            u = person["username"]
            if u in tag_done:
                continue
            if mm is None:
                failed(person, True, "Tag недоступен")
                continue
            try:
                channels[u] = mm.dm_channel(u, person["email"])
            except UserUnavailable as ex:
                failed(person, False, str(ex))
            except Exception as ex:
                failed(person, True, f"Tag: {_err(ex)}")

    # --- шаг 2: почта (SMTP) ---
    mail_sent = []
    if send_mail:
        note = _note(no_address)
        for person in people:
            if person["username"] in mail_done:
                continue
            try:
                from mailer import send_email
                sent = bool(send_email(subject, text_for(person, note), person["email"]))
            except Exception as ex:
                sent = False
                log.warning("  [notify] почта: не удалось отправить на %s: %s",
                            person["email"], _err(ex))
            if sent:
                mail_sent.append(person["username"])
            else:
                failed(person, True, "почта не отправлена")
    mailed = mail_done | set(mail_sent) if send_mail else set()

    # --- шаг 3: ЛС в Tag; в тексте называем тех, кто не получит уведомление никак ---
    lost = [p for p in people if p["username"] in fails and p["username"] not in channels
            and p["username"] not in tag_done and p["username"] not in mailed]
    note = _note(no_address + [label(p) for p in lost])
    tag_sent = []
    for person in people:
        u = person["username"]
        if u in tag_done:
            continue
        text = text_for(person, note)
        if not tag_enabled:
            log.info("  [notify:ЗАГЛУШКА Tag] -> @%s (%s) по записи %s\n%s",
                     u, person["email"], recording_key, text)
            tag_sent.append(u)
        elif u in channels:
            try:
                mm.post_archive(channels[u], text, recording_key)
                tag_sent.append(u)
                log.info("  [notify] Tag: уведомление об удалении записи %s отправлено @%s",
                         recording_key, u)
            except Exception as ex:
                failed(person, True, f"Tag: {_err(ex)}")
                log.warning("  [notify] Tag: не удалось отправить @%s: %s", u, _err(ex))

    # --- итог ---
    res = NotifyResult(tag_sent=tag_sent, mail_sent=mail_sent)
    in_tag = (tag_done | set(tag_sent)) if tag_enabled else set()
    res.reached_any = bool(in_tag or mailed)
    res.unreachable = list(no_address)
    for person in people:
        u = person["username"]
        tag_ok = (not tag_enabled) or u in in_tag
        mail_ok = (not send_mail) or u in mailed
        if tag_ok and mail_ok:
            continue
        reasons = fails.get(u) or [(True, "не доставлено")]
        res.has_errors = res.has_errors or any(is_error for is_error, _ in reasons)
        res.pending.append(f"@{u}: " + "; ".join(reason for _, reason in reasons))
        if u not in in_tag and u not in mailed:
            res.unreachable.append(label(person))
    if res.pending:
        log.warning("  [notify] не всем доставлено: %s", " | ".join(res.pending))
    return res
