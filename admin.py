"""
admin.py — АДМИН-ПАНЕЛЬ бота (команда archiever_admin в ЛС, только для админов).

Что умеет админ:
  • сводка состояния архива (сколько записей в каком состоянии, фазы, окна);
  • поиск записи по ID, названию или участнику и её КАРТОЧКА: характеристики, этапы,
    ошибки, скачивание ЛЮБОЙ расшифровки, запрет/разрешение удаления, удаление видео;
  • записи «до решения админа»: список и перезапуск (то же, что команда restart_passed);
  • ближайшие удаления, неудаляемые записи, журнал действий;
  • состав админов: суперадмин меняет его сразу, решения обычных админов ждут период
    охлаждения — с уведомлением всех админов и правом суперадмина заблокировать.

Длинные ответы (разделы, результаты поиска, карточка) бот не вываливает в личку: туда
уходит одна строка, а содержимое — в тред под ней (см. View).

Модуль не знает про HTTP/WebSocket: функции получают зависимости явно (conn/cfg/mm/talk) и
возвращают вложения сообщения (attachments) либо результат операции (успех, текст ответа).
Транспорт и проверка прав — в bot.py.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

import config
import db
import resources
import statuses
import store
import texts as T
from mm_client import UserUnavailable
from util import err as _err, fmt_date, fmt_dt, parse_dt, record_label, username_from_email

log = logging.getLogger("admin")

_LIST_LIMIT = 15          # строк в списках (остановленные, удаления, неудаляемые, поиск)
_PEOPLE_LIMIT = 20        # участников в карточке поимённо
_KIND_NAMES = (("asr", "базовая"), ("llm", "улучшенная"), ("short", "выжимка"))


class View(NamedTuple):
    """Длинный ответ панели. В ЛС уходит одна строка head, а содержимое atts — в тред под
    ней, чтобы личка не превращалась в простыню. empty — раздел пуст: тред не нужен,
    достаточно строки head."""
    head: str
    atts: list
    empty: bool = False


def _http_status(ex):
    return getattr(getattr(ex, "response", None), "status_code", None)


def _ago(iso):
    """«3 мин назад» / «5 ч назад» / «2 дн. назад» для ISO-момента."""
    dt = parse_dt(iso)
    if not dt:
        return "—"
    sec = max(0, (datetime.now(timezone.utc) - dt).total_seconds())
    if sec < 3600:
        return f"{int(sec // 60)} мин назад"
    if sec < 86400:
        return f"{int(sec // 3600)} ч назад"
    return f"{int(sec // 86400)} дн. назад"


def _size(path):
    n = os.path.getsize(path)
    if n < 1024:
        return f"{n} Б"
    return f"{n / 1024:.0f} КБ" if n < 1024 * 1024 else f"{n / 1024 / 1024:.1f} МБ"


def deleted_by_text(by):
    """Кто удалил видео — словами (по полю deleted_by)."""
    if not by:
        return T.DELETED_BY_UNKNOWN
    if by.startswith("@"):
        return T.DELETED_BY_ADMIN.format(who=by)
    return {"auto": T.DELETED_BY_AUTO, "short": T.DELETED_BY_SHORT,
            "external": T.DELETED_BY_EXTERNAL}.get(by, by)


def file_kinds(row):
    """Какие тексты записи реально лежат на диске: {тип: путь} (asr/llm/short). «Улучшенную»
    считаем отдельным файлом, только если это не та же базовая (так бывает без LLM)."""
    paths = {k: store.existing_path(row, k) for k, _ in _KIND_NAMES}
    if paths["llm"] and paths["llm"] == paths["asr"]:
        paths["llm"] = None
    return {k: p for k, p in paths.items() if p}


# =========================================================================
#  ГЛАВНЫЙ ЭКРАН
# =========================================================================

def _window(w, now):
    (sh, sm), (eh, em) = w
    state = "открыто" if config.in_window(now, w) else "закрыто"
    return f"{sh:02d}:{sm:02d}–{eh:02d}:{em:02d} ({state})"


def _old_records_line(s, cfg):
    if s["old_records_flag"]:
        return (f"завершена {fmt_date(s['old_records_at'])}: уведомлено в Tag {s['users_tag']}, "
                f"почтой {s['users_mail']} из {s['users']}")
    if not (cfg.send_to_tag or cfg.send_to_mail):
        return "не начнётся: рассылка выключена (SEND_TO_TAG=0 и SEND_TO_MAIL=0)"
    return (f"ждёт обработки старых записей; пользователей к уведомлению: {s['users']}. "
            "До её завершения видео не удаляются")


def panel(conn, cfg, mm):
    """Главный экран админ-панели: сводка + кнопки разделов."""
    s = db.stats(conn)
    now = datetime.now().astimezone()
    nxt = f" (ближайшее {fmt_dt(s['next_delete_at'])})" if s["next_delete_at"] else ""
    sim = f"; сымитировано {s['delete_sim']}" if s["delete_sim"] else ""

    def f(title, value, short=True):
        return {"title": title, "value": str(value), "short": short}

    fields = [
        f("Всего записей", s["total"]),
        f("В архиве (тексты готовы)", s["saved"]),
        f("Ждут транскрибации", s["wait_asr"]),
        f("Ждут LLM / сохранения", s["wait_llm"]),
        f("⏸ Ждут решения админа (FAILED_…)", s["hold"]),
        f("Повторяются после сбоя", s["retrying"]),
        f("Видео удалено", f"{s['deleted']}{sim}"),
        f("Ждут удаления", f"{s['delete_pending']}{nxt}"),
        f("🛡 Запрет админа", s["protected"]),
        f("🔒 Запрет Толка (DELETE_PROTECTION)", s["kept_forever"]),
        f("Без аудио / слишком короткие", f"{s['no_audio']} / {s['too_short']}"),
        f("Без дорожек по спикерам", s["no_channels"]),
        f("Единоразовое уведомление о старых записях", _old_records_line(s, cfg), short=False),
        f("Режим", f"REAL_DELETE={int(cfg.real_delete)} · SEND_TO_TAG={int(cfg.send_to_tag)} · "
                   f"SEND_TO_MAIL={int(cfg.send_to_mail)} · окна: ASR {_window(cfg.asr_window, now)}, "
                   f"LLM {_window(cfg.llm_window, now)}", short=False),
    ]
    actions = [mm.button(T.BTN_ADMIN_FIND, "admfind", style="primary"),
               mm.button(T.BTN_ADMIN_HOLDS.format(n=s["hold"]), "admholds")]
    if s["hold"]:
        actions.append(mm.button(T.BTN_ADMIN_RESTART_ALL, "admrestart", view="panel"))
    waiting = len(db.admin_requests(conn))          # решения об админах в периоде охлаждения
    actions += [mm.button(T.BTN_ADMIN_UPCOMING, "admupcoming"),
                mm.button(T.BTN_ADMIN_KEPT, "admkept"),
                mm.button(T.BTN_ADMIN_ADMINS_PENDING.format(n=waiting) if waiting
                          else T.BTN_ADMIN_ADMINS, "admadmins"),
                mm.button(T.BTN_ADMIN_LOG, "admlog"),
                mm.button(T.BTN_ADMIN_MEMORY, "admmemory"),
                mm.button(T.BTN_ADMIN_REFRESH, "admpanel", upd=1)]
    return [{"title": "🛠 Админ-панель архиватора", "fields": fields, "actions": actions,
             "footer": f"Обновлено {now.strftime('%d.%m.%Y %H:%M')} · последнее изменение "
                       f"в базе: {_ago(s['last_update'])}"}]


# =========================================================================
#  КАРТОЧКА ЗАПИСИ
# =========================================================================

def _video_line(row, cfg):
    if row.get("deleted"):
        return f"удалено {fmt_dt(row.get('deleted_at'))} — {deleted_by_text(row.get('deleted_by'))}"
    if row.get("kept_forever"):
        return "🔒 сохранено бессрочно: Толк запрещает удалять это видео"
    if row.get("protected"):
        who = f" ({row['protected_by']})" if row.get("protected_by") else ""
        return f"🛡 в Толке, удаление запрещено админом{who}"
    if row.get("delete_simulated"):
        return "в Толке; удаление сымитировано (REAL_DELETE=0)"
    if row.get("status") == statuses.TOO_SHORT:
        # короткую запись удаляют при подхвате; раз она ещё в Толке — удалить не удалось
        if row.get("next_due_at"):
            return f"в Толке; удалить не удалось — повтор {fmt_dt(row['next_due_at'])}"
        return "в Толке; удалить не удалось, повторы исчерпаны — удали вручную кнопкой ниже"
    if row.get("needs_admin") and row.get("fail_stage") in ("messages", "delete"):
        return "в Толке; удаление приостановлено — запись ждёт решения админа"
    if row.get("delete_scheduled_at"):
        line = f"в Толке; удаление назначено на {fmt_dt(row.get('delete_scheduled_at'))}"
        if not row.get("messages_sent"):
            line += " (уведомление дошло ещё не до всех участников — идут повторы)"
        return line
    created = parse_dt(row.get("created_date"))
    if created:
        old = created + timedelta(days=cfg.delete_video_age_days)
        if old > datetime.now(timezone.utc):
            return f"в Толке; порог возраста для удаления — {fmt_date(old.isoformat())}"
    return "в Толке; уведомление участников и удаление — по графику"


def _texts_line(row, cfg, kinds):
    if row.get("text_cleaned"):
        return "удалены: срок хранения истёк"
    if not kinds:
        return "ещё не готовы"
    sizes = " · ".join(f"{name} {_size(kinds[k])}" for k, name in _KIND_NAMES if k in kinds)
    if row.get("deleted") and row.get("text_delete_at"):
        keep = f"хранятся до {fmt_date(row['text_delete_at'])}"
    elif cfg.text_retention_days is None:
        keep = "хранятся бессрочно"
    else:
        keep = f"хранятся {cfg.text_retention_days} дн. после удаления видео"
    return f"{sizes} — {keep}"


def _stages_line(row):
    def mark(flag):
        return "✅" if row.get(flag) else "—"
    parts = [f"транскрибация {mark('transcribed')}", f"LLM-правка {mark('improved')}",
             f"выжимка {mark('summarized')}", f"сохранение {mark('saved_to_db')}",
             f"уведомление участников {mark('messages_sent')}"]
    line = " · ".join(parts)
    if row.get("no_channels"):
        line += "\nБез дорожек по спикерам: распознана общая дорожка, реплики не привязаны к участникам."
    if row.get("no_audio"):
        line += "\nУ записи нет аудио — расшифровать нельзя, запись не обрабатывается."
    return line


def _problem_line(row):
    stage = statuses.STAGE_NAMES.get(row.get("fail_stage"), row.get("fail_stage") or "?")
    since = fmt_dt(row.get("error_since"))
    if row.get("needs_admin"):
        return (f"⏸ Остановлена на стадии «{stage}»: не удаётся с {since}, неудач подряд — "
                f"{row.get('fail_count') or 0}. Ошибка: `{row.get('last_error') or '—'}`")
    if row.get("fail_count"):
        return (f"Стадия «{stage}» не удаётся с {since} (неудач подряд — {row['fail_count']}); "
                f"следующий повтор не раньше {fmt_dt(row.get('next_due_at'))}. "
                f"Ошибка: `{row.get('last_error') or '—'}`")
    return None


def _people_line(row):
    people = row.get("participants") or []
    if not people:
        return "—"
    out = []
    for p in people[:_PEOPLE_LIMIT]:
        nick = f" (@{username_from_email(p['email'])})" if p.get("email") else ""
        out.append(f"{p.get('name') or '?'}{nick}")
    more = f" и ещё {len(people) - _PEOPLE_LIMIT}" if len(people) > _PEOPLE_LIMIT else ""
    return ", ".join(out) + more


def card(conn, cfg, mm, row):
    """Карточка записи: характеристики + кнопки (скачать тексты, запретить/разрешить
    удаление, удалить видео, перезапустить, обновить)."""
    key = row["recording_key"]
    kinds = {} if row.get("text_cleaned") else file_kinds(row)
    people = row.get("participants") or []

    def f(title, value, short=False):
        return {"title": title, "value": str(value), "short": short}

    fields = [
        f("ID", f"`{key}`"),
        f("Статус", f"`{row.get('status') or '—'}` — {statuses.label(row.get('status'))}"),
        f("Дата", fmt_dt(row.get("created_date")), True),
        f("Длительность", f"{round((row.get('duration_sec') or 0) / 60)} мин", True),
        f("Видео", _video_line(row, cfg)),
        f("Тексты", _texts_line(row, cfg, kinds)),
        f("Этапы", _stages_line(row)),
        f(f"Участники ({len(people)})", _people_line(row)),
    ]
    if row.get("unreachable"):
        fields.append(f("Не получили уведомление об удалении", ", ".join(row["unreachable"])))
    problem = _problem_line(row)
    if problem:
        fields.append(f("Проблема", problem))

    actions = [mm.button(label, "admfile", key=key, kind=val) for val, label in T.FILE_ACTIONS
               if val in kinds or (val == "all" and len(kinds) > 1)]
    if not row.get("deleted"):
        if row.get("protected"):
            actions.append(mm.button(T.BTN_ADMIN_UNPROTECT, "admunprotect", key=key))
        else:
            actions.append(mm.button(T.BTN_ADMIN_PROTECT, "admprotect", key=key))
        actions.append(mm.button(T.BTN_ADMIN_DELETE, "admdelete", style="danger", key=key))
    if row.get("needs_admin"):
        actions.append(mm.button(T.BTN_ADMIN_RESTART_ONE, "admrestart", key=key, view="card"))
    actions.append(mm.button(T.BTN_ADMIN_REFRESH, "admcard", key=key, upd=1))
    color = "#d24b4e" if row.get("needs_admin") else "#3d9970" if row.get("saved_to_db") else "#999999"
    return [{"title": f"📼 {row.get('title') or 'без названия'}", "fields": fields,
             "actions": actions, "color": color}]


def card_view(conn, cfg, mm, row):
    """Карточка записи как длинный ответ: строка в ЛС + сама карточка для треда."""
    return View(T.ADM_THREAD_CARD.format(title=row.get("title") or row["recording_key"]),
                card(conn, cfg, mm, row))


# =========================================================================
#  СПИСКИ
# =========================================================================

def _listing(mm, title, rows, line, buttons=(), empty="Пусто.", total=None):
    """Сообщение-список: строка на запись + выпадающий список «Открыть запись…»."""
    text = "\n".join(f"{i}. {line(r)}" for i, r in enumerate(rows, 1)) or empty
    if total and total > len(rows):
        text += f"\n… и ещё {total - len(rows)}"
    actions = []
    if rows:
        actions.append(mm.menu(T.MENU_ADMIN_OPEN, "admcard",
                               [(record_label(r)[:90], r["recording_key"]) for r in rows]))
    return [{"title": title, "text": text, "actions": actions + list(buttons)}]


def results(mm, rows):
    """Результаты поиска (несколько записей) — список с выбором записи."""
    atts = _listing(mm, f"🔎 Найдено записей: {len(rows)}"
                        + (" (показаны самые новые — уточни запрос)" if len(rows) >= _LIST_LIMIT else ""),
                    rows, lambda r: f"{record_label(r)} — `{r.get('status') or '—'}` — `{r['recording_key']}`")
    return View(T.ADM_THREAD_RESULTS.format(n=len(rows)), atts)


def find(conn, q=None, who=None):
    """Найти записи для админа: точный ID -> одна запись, иначе поиск по ID/названию/участнику."""
    exact = db.get(conn, q) if q else None
    if exact and not who:
        return [exact]
    return db.search_recordings(conn, q=q, who=who, limit=_LIST_LIMIT)


def holds_view(conn, mm):
    """Записи «до решения админа»: по стадиям, с ошибками, + кнопка перезапуска всех."""
    counts = db.hold_counts(conn)
    total = sum(counts.values())
    rows = db.holds(conn, limit=_LIST_LIMIT)
    by_stage = " · ".join(f"{statuses.failed(st)}: {n}" for st, n in counts.items())
    buttons = [mm.button(T.BTN_ADMIN_RESTART_ALL, "admrestart", style="primary", view="holds")] if total else []

    def line(r):
        return (f"{record_label(r)} — `{r.get('status')}`, "
                f"с {fmt_dt(r.get('error_since') or r.get('updated_at'))}: "
                f"`{(r.get('last_error') or '—')[:120]}`")

    title = f"⏸ Ждут решения админа: {total}" + (f" ({by_stage})" if by_stage else "")
    atts = _listing(mm, title, rows, line, buttons, empty=T.ADM_HOLDS_EMPTY, total=total)
    return View(T.ADM_THREAD_HOLDS.format(n=total) if total else T.ADM_HOLDS_EMPTY, atts, not total)


def upcoming_view(conn, mm):
    rows = db.upcoming_deletions(conn, limit=_LIST_LIMIT)
    total = db.stats(conn)["delete_pending"]
    atts = _listing(mm, f"🗓 Ближайшие удаления видео (всего ждут удаления: {total})", rows,
                    lambda r: f"**{fmt_dt(r.get('delete_scheduled_at'))}** — {record_label(r)}",
                    empty=T.ADM_UPCOMING_EMPTY, total=total)
    return View(T.ADM_THREAD_UPCOMING.format(n=total) if rows else T.ADM_UPCOMING_EMPTY, atts, not rows)


def kept_view(conn, mm):
    rows = db.list_kept(conn, limit=_LIST_LIMIT)

    def line(r):
        if r.get("protected"):
            why = "🛡 запрет админа" + (f" {r['protected_by']}" if r.get("protected_by") else "")
        else:
            why = "🔒 запрет Толка"
        return f"{record_label(r)} — {why}"

    atts = _listing(mm, "🛡 Неудаляемые видео (самые новые)", rows, line, empty=T.ADM_KEPT_EMPTY)
    return View(T.ADM_THREAD_KEPT if rows else T.ADM_KEPT_EMPTY, atts, not rows)


def admins_view(conn, mm, cfg, viewer):
    """Раздел «Админы» глазами viewer: состав, решения, ждущие вступления в силу, и кнопки по
    его роли — суперадмин блокирует решения и передаёт права, подавший отзывает своё."""
    rows = db.admins(conn)
    sup = db.superadmin(conn)
    lines = []
    for a in rows:
        if a["username"] == sup:
            lines.append(f"⭐ @{a['username']} — суперадмин")
        else:
            by = a.get("added_by")
            lines.append(f"• @{a['username']} — админ с {fmt_date(a.get('added_at'))}"
                         + (f", по решению @{by}" if by and by != "system" else ""))
    pending = db.admin_requests(conn)
    text = "\n".join(lines) or "—"
    if pending:
        text += "\n\n" + T.ADM_ADMINS_PENDING + "\n" + "\n".join(
            f"{i}. @{r['requested_by']} хочет {what(r)} — {fmt_dt(r['effective_at'])}"
            for i, r in enumerate(pending, 1))
    text += "\n\n" + T.ADM_ADMINS_RULES.format(hours=delay_hours(cfg), super=sup or "—")

    def options(reqs):
        return [(f"@{r['requested_by']}: {what(r)}"[:90], str(r["id"])) for r in reqs]

    actions = [mm.button(T.BTN_ADMIN_ADD, "admaddadmin"), mm.button(T.BTN_ADMIN_REMOVE, "admdeladmin")]
    mine = [r for r in pending if r["requested_by"] == viewer]
    if viewer == sup:
        actions.append(mm.button(T.BTN_ADMIN_TRANSFER, "admtransfer"))
        if pending:
            actions.append(mm.menu(T.MENU_ADMIN_BLOCK, "admblock", options(pending), view="admins"))
    elif mine:
        actions.append(mm.menu(T.MENU_ADMIN_WITHDRAW, "admwithdraw", options(mine), view="admins"))
    atts = [{"title": f"👥 Администраторы: {len(rows)}", "text": text, "actions": actions}]
    return View(T.ADM_THREAD_ADMINS.format(n=len(rows)), atts)


def memory_view(conn, cfg):
    """Диск и ОЗУ ВМ прямо сейчас (бот в своём контейнере меряет ту же ВМ) и последний снимок
    демона: пауза расшифровок, временные папки, когда он последний раз отметился."""
    pct, used, total, free = resources.disk(cfg)
    r = resources.ram()
    try:
        st = json.loads(db.get_meta(conn, "daemon_state") or "{}")
    except ValueError:
        st = {}
    limit = cfg.disk_max_used_pct
    if not limit:
        pause = "контроль выключен (`DISK_MAX_USED_PCT=0`)"
    elif st.get("asr_paused"):
        pause = f"⏸ новые расшифровки на паузе: занято от {limit}%"
    else:
        pause = f"идут; пауза — при {limit}% и больше"
    warn = "⚠️ " if limit and pct >= limit else ""
    ram = ("не удалось узнать" if not r else
           f"{'⚠️ ' if cfg.ram_alert_pct and r['used_pct'] >= cfg.ram_alert_pct else ''}"
           f"занято {resources.gb(r['total'] - r['available'])} из {resources.gb(r['total'])} "
           f"({r['used_pct']:.0f}%), доступно {resources.gb(r['available'])}; "
           f"swap {resources.gb(r['swap_used'])} из {resources.gb(r['swap_total'])}")
    seen = parse_dt(st.get("at"))
    stale = not seen or (datetime.now(timezone.utc) - seen).total_seconds() > 300
    daemon = ("⚠️ давно не отмечался — жив ли демон? (`docker compose ps`)" if stale else
              f"отметился {_ago(st.get('at'))}; временных папок расшифровки: {st.get('tmp_dirs', '—')} "
              f"(норма — не больше {2 * cfg.record_workers_asr})")
    fields = [
        {"title": "💽 SSD", "value": f"{warn}занято {resources.gb(used)} из {resources.gb(total)} "
                                     f"({pct:.0f}%), свободно {resources.gb(free)}", "short": False},
        {"title": "Расшифровки", "value": pause, "short": False},
        {"title": "🧠 ОЗУ ВМ", "value": ram + (f" · тревога с {cfg.ram_alert_pct}%"
                                              if cfg.ram_alert_pct else ""), "short": False},
        {"title": "Демон", "value": daemon, "short": False},
    ]
    atts = [{"title": "💾 Диск и память ВМ", "fields": fields,
             "footer": f"Сейчас: {datetime.now().astimezone().strftime('%d.%m.%Y %H:%M')}"}]
    return View(T.ADM_THREAD_MEMORY, atts)


def log_view(conn):
    lines = []
    for e in db.admin_log(conn, limit=20):
        what = T.LOG_ACTIONS.get(e["action"], e["action"])
        key = f" `{e['recording_key']}`" if e.get("recording_key") else ""
        details = f" — {e['details']}" if e.get("details") else ""
        lines.append(f"• {fmt_dt(e['at'])} — @{e['username']} {what}{key}{details}")
    atts = [{"title": "📜 Журнал действий админов (последние 20)",
             "text": "\n".join(lines) or T.ADM_LOG_EMPTY}]
    return View(T.ADM_THREAD_LOG if lines else T.ADM_LOG_EMPTY, atts, not lines)


# =========================================================================
#  ОПЕРАЦИИ (возвращают (успех, текст ответа))
# =========================================================================

def _forget_utilization_failure(row):
    """Поля, сбрасывающие сбой рассылки/удаления видео. После запрета, разрешения или
    ручного удаления эти стадии либо не нужны, либо начнутся заново — прежний отсчёт
    повторов (и «до решения админа» из-за них) теряет смысл."""
    if row.get("fail_stage") not in ("messages", "delete"):
        return {}
    fields = dict(fail_stage=None, fail_count=0, last_error=None, error_since=None)
    if row.get("needs_admin"):
        fields.update(needs_admin=0, hold_notified=0, status=statuses.derive(row))
    return fields


def protect(conn, key, by):
    """Запретить удаление видео («хранить в Толке навсегда»)."""
    row = db.get(conn, key)
    if not row:
        return False, T.ADM_NOT_FOUND.format(key=key)
    title = row.get("title") or key
    if row.get("deleted"):
        return False, T.ADM_PROTECT_DELETED.format(key=key)
    if row.get("protected"):
        return False, T.ADM_ALREADY_PROTECTED.format(title=title, by=row.get("protected_by") or "админ")
    db.update(conn, key, protected=1, protected_by=f"@{by}", **_forget_utilization_failure(row))
    db.wake(conn, key)                              # демон переоценит запись
    db.log_admin(conn, by, "protect", key)
    return True, T.ADM_PROTECTED.format(title=title)


def unprotect(conn, key, by):
    """Снять запрет админа. Прежнее уведомление участникам (если было) аннулируем: видео
    пойдёт по графику заново — с новым уведомлением и новым сроком, а не исчезнет сразу."""
    row = db.get(conn, key)
    if not row:
        return False, T.ADM_NOT_FOUND.format(key=key)
    title = row.get("title") or key
    if not row.get("protected"):
        return False, T.ADM_NOT_PROTECTED.format(title=title)
    db.update(conn, key, protected=0, protected_by=None, messages_sent=0,
              delete_scheduled_at=None, notified_usernames=[], notified_mail=[], unreachable=[],
              **_forget_utilization_failure(row))
    db.wake(conn, key)
    db.log_admin(conn, by, "unprotect", key)
    return True, T.ADM_UNPROTECTED.format(title=title)


def delete_video(conn, cfg, talk, key, by, reason=None):
    """Ручное удаление видео админом. Работает для ЛЮБОГО видео, включая защищённое
    (remember): ручное удаление имеет приоритет и снимает запрет. Если видео уже удалено —
    отвечаем, когда и кем."""
    row = db.get(conn, key)
    if not row:
        return False, T.ADM_NOT_FOUND.format(key=key)
    title = row.get("title") or key
    if row.get("deleted"):
        return False, T.ADM_ALREADY_DELETED.format(
            title=title, when=fmt_dt(row.get("deleted_at")), who=deleted_by_text(row.get("deleted_by")))
    deleted_by = f"@{by}"
    try:
        talk.delete_recording(key, force=cfg.force_delete)
    except Exception as ex:
        if _http_status(ex) != 404:
            return False, T.ADM_DELETE_FAIL.format(key=key, error=_err(ex))
        deleted_by = "external"                     # в Толке записи уже нет
    fields = _forget_utilization_failure(row)       # стояла на рассылке/удалении — больше незачем
    fields.update(db.deleted_fields(deleted_by, cfg.text_retention_days))
    fields.update(protected=0, protected_by=None, kept_forever=0)
    db.update(conn, key, **fields)
    db.wake(conn, key)                              # демон подхватит: запланирует очистку текстов
    db.log_admin(conn, by, "delete", key, reason or None)
    keep = (T.ADM_TEXTS_UNTIL.format(date=fmt_date(fields["text_delete_at"]))
            if fields.get("text_delete_at") else T.ADM_TEXTS_FOREVER)
    if deleted_by == "external":
        return True, T.ADM_DELETED_GONE.format(title=title, texts=keep)
    note = T.ADM_DELETE_NOTE_PROTECTED if row.get("protected") else ""
    return True, T.ADM_DELETED.format(title=title, note=note, texts=keep)


def restart(conn, by, key=None):
    """Вернуть в работу записи «до решения админа» (все или одну). Возвращает (число, текст)."""
    n = db.restart_holds(conn, key)
    if not n:
        return 0, T.ADM_RESTART_NONE
    db.log_admin(conn, by, "restart", key, f"записей: {n}")
    return n, T.ADM_RESTARTED.format(n=n)


# =========================================================================
#  СОСТАВ АДМИНОВ: суперадмин, период охлаждения, блокировка
# =========================================================================
#
# Суперадмин один. Он меняет состав админов сразу и без уведомления остальных, блокирует
# чужие решения и может передать свои права другому админу (сам остаётся обычным админом).
# Обычный админ состав сразу не меняет: его решение ждёт cfg.admin_change_delay_sec, все
# админы получают уведомление, суперадмин может заблокировать, подавший — отозвать.
# Убрать суперадмина нельзя.

class Change(NamedTuple):
    """Итог попытки изменить состав админов."""
    ok: bool                 # False — отказ, text объясняет почему
    text: str                # ответ тому, кто нажал
    request: dict = None     # заведённое решение с периодом охлаждения — о нём уведомить админов
    outcomes: tuple = ()     # решения, закрытые по ходу: (решение, исход, причина) — о них тоже


def bootstrap_superadmin(conn, cfg):
    """При старте бота определить суперадмина. Он хранится в БД (его можно передать через
    бота), а ADMIN_USERNAME из .env назначает его в двух случаях: при первом запуске и
    когда значение в .env ИЗМЕНИЛИ — это способ вернуть управление, если суперадмин
    недоступен. Возвращает ник суперадмина (или None)."""
    env_name, current = cfg.admin_username, db.superadmin(conn)
    if env_name and (not current or env_name != db.get_meta(conn, "superadmin_env")):
        db.add_admin(conn, env_name, "system")
        db.set_superadmin(conn, env_name)
        db.set_meta(conn, "superadmin_env", env_name)
        if current and current != env_name:
            db.log_admin(conn, env_name, "superadmin_env", details=f"прежний суперадмин — @{current}")
        return env_name
    return current


def is_super(conn, username):
    return bool(username) and username == db.superadmin(conn)


def delay_hours(cfg):
    """Период охлаждения в часах — для текстов («24», «0.5»)."""
    return f"{cfg.admin_change_delay_sec / 3600:g}"


def what(req):
    """Суть решения словами: «добавить администратора @x» / «убрать администратора @x»."""
    return (T.ADM_WHAT_ADD if req["kind"] == "add" else T.ADM_WHAT_REMOVE).format(target=req["target"])


def request_notice(conn, cfg, req):
    """Уведомление всем админам о решении, которое ждёт вступления в силу."""
    return T.ADM_REQ_NOTICE.format(by=req["requested_by"], what=what(req),
                                   when=fmt_dt(req["effective_at"]), hours=delay_hours(cfg),
                                   super=db.superadmin(conn) or "—")


def outcome_text(conn, req, result, reason=None):
    """Сообщение всем админам об исходе решения: вступило в силу / заблокировано / отозвано /
    отменено."""
    by, target = req["requested_by"], req["target"]
    if result == "applied":
        tpl = T.ADM_REQ_APPLIED_ADD if req["kind"] == "add" else T.ADM_REQ_APPLIED_REMOVE
        return tpl.format(target=target, by=by)
    if result == "blocked":
        return T.ADM_REQ_BLOCKED.format(super=req.get("resolved_by") or db.superadmin(conn),
                                        by=by, what=what(req))
    if result == "withdrawn":
        return T.ADM_REQ_WITHDRAWN.format(by=by, what=what(req))
    return T.ADM_REQ_CANCELLED.format(by=by, what=what(req), reason=reason or "—")


def _drop_admin(conn, target):
    """Убрать админа и отменить решения, которые он подал и которые ещё ждут: бывший админ
    не должен менять состав «из прошлого». Возвращает отменённые решения как исходы."""
    db.remove_admin(conn, target)
    outcomes = []
    for r in db.admin_requests(conn):
        if r["requested_by"] == target and db.resolve_admin_request(
                conn, r["id"], "cancelled", note=T.ADM_REASON_INITIATOR_GONE):
            db.log_admin(conn, target, "cancel", details=f"{what(r)}: {T.ADM_REASON_INITIATOR_GONE}")
            outcomes.append((r, "cancelled", T.ADM_REASON_INITIATOR_GONE))
    return outcomes


def change_admins(conn, cfg, mm, kind, who, by):
    """Добавить (kind="add") или убрать ("remove") админа who по воле by. Суперадмин — сразу
    и без уведомления остальных; обычный админ — заводит решение с периодом охлаждения
    (уведомления рассылает вызывающий по Change.request / Change.outcomes)."""
    who = (who or "").strip().lstrip("@").lower()    # ники в Tag — строчными и без «@»
    sup = db.superadmin(conn)
    if kind == "add":
        if not who:
            return Change(False, T.ADM_ADMIN_NO_USER.format(who=""))
        try:
            mm.find_user(username=who)              # убедимся, что такой пользователь есть в Tag
        except UserUnavailable:
            return Change(False, T.ADM_ADMIN_NO_USER.format(who=who))
        except Exception as ex:
            return Change(False, T.ADM_ADMIN_CHECK_FAIL.format(who=who, error=_err(ex)))
        if db.is_admin(conn, who):
            return Change(False, T.ADM_ADMIN_EXISTS.format(who=who))
    else:
        if who == sup:
            return Change(False, T.ADM_REMOVE_SUPER.format(who=who))
        if not db.is_admin(conn, who):
            return Change(False, T.ADM_ADMIN_NOT_ADMIN.format(who=who))
    same = next((r for r in db.admin_requests(conn) if r["kind"] == kind and r["target"] == who), None)

    if by == sup:                                   # суперадмин: сразу, остальных не уведомляем
        outcomes = []
        if kind == "add":
            db.add_admin(conn, who, by)
            text = T.ADM_ADMIN_ADDED.format(who=who, by=by)
        else:
            outcomes = _drop_admin(conn, who)
            text = T.ADM_ADMIN_REMOVED.format(who=who, by=by)
        db.log_admin(conn, by, "add_admin" if kind == "add" else "remove_admin", details=f"@{who}")
        # это же самое уже ждало по чьему-то решению — оно исполнено досрочно
        if same and db.resolve_admin_request(conn, same["id"], "applied", by=by):
            outcomes.insert(0, (same, "applied", None))
        return Change(True, text, outcomes=tuple(outcomes))

    if same:
        return Change(False, T.ADM_REQ_DUPLICATE.format(by=same["requested_by"],
                                                        when=fmt_dt(same["effective_at"])))
    effective = datetime.now(timezone.utc) + timedelta(seconds=cfg.admin_change_delay_sec)
    req = db.admin_request(conn, db.create_admin_request(conn, kind, who, by, effective.isoformat()))
    db.log_admin(conn, by, "request_add" if kind == "add" else "request_remove", details=f"@{who}")
    return Change(True, T.ADM_REQ_CREATED.format(what=what(req), when=fmt_dt(req["effective_at"]),
                                                 hours=delay_hours(cfg), super=sup or "—"),
                  request=req)


def _close(conn, req_id, status, by, log_action):
    """Закрыть ждущее решение (блокировка/отзыв); Change с отказом, если оно уже закрыто."""
    if not db.resolve_admin_request(conn, req_id, status, by=by):
        return Change(False, T.ADM_REQ_CLOSED)
    req = db.admin_request(conn, req_id)
    db.log_admin(conn, by, log_action, details=f"@{req['requested_by']}: {what(req)}")
    return Change(True, outcome_text(conn, req, status), outcomes=((req, status, None),))


def block(conn, req_id, by):
    """Суперадмин блокирует решение, пока оно не вступило в силу."""
    sup = db.superadmin(conn)
    if by != sup:
        return Change(False, T.ADM_SUPER_ONLY.format(super=sup or "—"))
    return _close(conn, req_id, "blocked", by, "block")


def withdraw(conn, req_id, by):
    """Подавший отзывает своё решение, пока оно не вступило в силу."""
    req = db.admin_request(conn, req_id)
    if not req:
        return Change(False, T.ADM_REQ_CLOSED)
    if req["requested_by"] != by:
        return Change(False, T.ADM_REQ_NOT_YOURS.format(by=req["requested_by"]))
    return _close(conn, req_id, "withdrawn", by, "withdraw")


def apply_due(conn, now=None):
    """Привести в исполнение решения, у которых истёк период охлаждения. Перед этим каждое
    перепроверяется: за время ожидания подавший мог перестать быть админом, а цель — стать
    суперадмином или уже получить/потерять права. Возвращает исходы (решение, исход,
    причина) — уведомления по ним рассылает вызывающий."""
    now_iso = (now or datetime.now(timezone.utc)).isoformat()
    sup = db.superadmin(conn)
    outcomes = []
    for r in db.admin_requests(conn, due_before=now_iso):
        target, by = r["target"], r["requested_by"]
        if not db.is_admin(conn, by):
            reason = T.ADM_REASON_INITIATOR_GONE
        elif r["kind"] == "add" and db.is_admin(conn, target):
            reason = T.ADM_REASON_ALREADY_ADMIN.format(target=target)
        elif r["kind"] == "remove" and target == sup:
            reason = T.ADM_REASON_IS_SUPER.format(target=target)
        elif r["kind"] == "remove" and not db.is_admin(conn, target):
            reason = T.ADM_REASON_NOT_ADMIN.format(target=target)
        else:
            reason = None
        if reason:
            if db.resolve_admin_request(conn, r["id"], "cancelled", note=reason):
                db.log_admin(conn, by, "cancel", details=f"{what(r)}: {reason}")
                outcomes.append((r, "cancelled", reason))
            continue
        if not db.resolve_admin_request(conn, r["id"], "applied"):
            continue                                # в эту же секунду заблокировали/отозвали
        outcomes.append((r, "applied", None))
        if r["kind"] == "add":
            db.add_admin(conn, target, by)
        else:
            outcomes += _drop_admin(conn, target)
        db.log_admin(conn, by, "add_admin" if r["kind"] == "add" else "remove_admin",
                     details=f"@{target} — решение вступило в силу")
    return outcomes


def transfer_superadmin(conn, target, by):
    """Суперадмин передаёт свои права другому админу и остаётся обычным админом."""
    sup = db.superadmin(conn)
    if by != sup:
        return False, T.ADM_SUPER_ONLY.format(super=sup or "—")
    target = (target or "").strip().lstrip("@").lower()
    if target == by or not db.is_admin(conn, target):
        return False, T.ADM_TRANSFER_NOT_ADMIN
    db.set_superadmin(conn, target)
    db.log_admin(conn, by, "transfer", details=f"@{target}")
    return True, T.ADM_TRANSFERRED.format(who=target)
