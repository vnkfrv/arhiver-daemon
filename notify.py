"""
notify.py — РАССЫЛКА УВЕДОМЛЕНИЙ участникам в Tag (Mattermost).

Формирует текст сообщения ровно по ТЗ и рассылает его участникам записи. Если передан
клиент `mm` (MMClient) — реально отправляет ЛС с кнопками «Видеозапись» / «Вся
расшифровка» / «Короткая выжимка»; если `mm=None` (SEND_TO_TAG=0) — только пишет в лог,
кому бы отправил (безопасный режим, никто ничего не получает).

Здесь же зафиксированы шаблон сообщения и правило username = тело почты до «@».
"""

import logging

log = logging.getLogger("notify")

# Точный шаблон из ТЗ. Плейсхолдеры подставляем через .format(...).
MESSAGE_TEMPLATE = (
    "Привет, {name}! 👋\n\n"
    "Видеозапись конференции «{title}» от {date} длительностью {minutes} минут, "
    "участником которой ты был(а), находится в хранилище уже более года⌛️.\n\n"
    "В целях экономии дискового пространства этот видеофайл будет безвозвратно "
    "удалён {delete_dt}🗑️, поэтому если он может тебе понадобиться в дальнейшем — "
    "скачай его себе на устройство. Вместо видео у тебя останется доступ к полной "
    "текстовой расшифровке и сжатой выжимке, которые уже подготовлены и будут "
    "храниться ещё 5 лет. Их можно скачать в любой момент!"
)


def username_from_email(email):
    """username в Tag = тело корпоративной почты до «@»."""
    return (email or "").split("@", 1)[0]


def build_message(name, title, date_str, minutes, delete_dt_str):
    return MESSAGE_TEMPLATE.format(name=name, title=title, date=date_str,
                                   minutes=minutes, delete_dt=delete_dt_str)


def send_archive_notifications(participants, *, title, date_str, minutes,
                               delete_dt_str, recording_key, mm=None, already=None):
    """
    Разослать уведомления участникам записи. ИДЕМПОТЕНТНО: тех, кто уже в `already`
    (usernames), пропускаем — чтобы при повторе тика не слать дубли тем, кому ЛС
    уже ушло на прошлой (частично упавшей) попытке.

    participants — [{"name":..., "email":...}]; mm — клиент Tag или None (лог-заглушка).
    Возвращает (all_ok, notified): all_ok=False, если хоть одному НЕ удалось отправить
    (пайплайн тогда не пометит стадию завершённой и повторит только оставшихся);
    notified — usernames, кому отправили в этот раз.
    """
    already = set(already or [])
    notified, all_ok = [], True
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
        try:
            if mm is None:
                log.info("  [notify:ЗАГЛУШКА] -> @%s (%s) по записи %s\n%s",
                         username, email, recording_key, text)
            else:
                mm.send_archive_dm(username, text, recording_key)
            notified.append(username)
        except Exception as ex:
            all_ok = False
            log.warning("  [notify] не удалось отправить @%s: %s", username, ex)
    return all_ok, notified
