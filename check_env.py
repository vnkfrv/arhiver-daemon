"""check_env.py — проверить настройки архиватора и живые доступы: Толк, ASR, LLM, Tag.

Демон при старте только проверяет, что обязательные переменные заданы, и сам ничего не
запрашивает: неверный ключ ASR/LLM проявится лишь на записях (они уйдут в повторы, а через
сутки — в FAILED_…). Этот скрипт шлёт по одному маленькому запросу в каждую систему и
показывает, с какими настройками работает демон. Секреты не печатает. Ничего не меняет.

Запуск на ВМ из папки проекта:  docker compose exec -T talk-daemon python - < check_env.py
"""
import io
import os
import sqlite3
import time
import wave
from urllib.parse import urlsplit

from config import load_cfg

PLAN = {"REAL_DELETE": 0, "FORCE_DELETE": 0, "SEND_TO_TAG": 0, "SEND_TO_MAIL": 0}
problems = []


def short(ex):
    code = getattr(getattr(ex, "response", None), "status_code", None)
    return (f"HTTP {code}: " if code else "") + f"{type(ex).__name__}: {str(ex)[:200]}"


def check(name, fn, hints=None):
    t = time.monotonic()
    try:
        print(f"  OK    {name}: {fn()}  ({time.monotonic() - t:.1f} с)")
    except Exception as ex:
        code = getattr(getattr(ex, "response", None), "status_code", None)
        print(f"  FAIL  {name}: {short(ex)}")
        hint = (hints or {}).get(code) or (hints or {}).get("*")
        if hint:
            print(f"        → {hint}")
        problems.append(name)


print("=== 1. Настройки, с которыми работает демон")
try:
    cfg = load_cfg()
except SystemExit as ex:
    print(f"  FAIL  .env не проходит проверку: {ex}")
    raise SystemExit(1)
flags = {"REAL_DELETE": cfg.real_delete, "FORCE_DELETE": cfg.force_delete,
         "SEND_TO_TAG": cfg.send_to_tag, "SEND_TO_MAIL": cfg.send_to_mail}
for k, v in flags.items():
    bad = int(bool(v)) != PLAN[k]
    print(f"  {'⚠' if bad else ' '}     {k}={int(bool(v))}"
          + ("   ← ВНИМАНИЕ: по плану сейчас 0" if bad else ""))
    if bad:
        problems.append(k)
print(f"        воркеров ASR/LLM={cfg.record_workers_asr}/{cfg.record_workers_llm}, "
      f"ASR-окно={cfg.asr_window}, LLM-окно={cfg.llm_window}")
import resources
pct = resources.disk(cfg)[0]
tmp_dirs = resources.tmp_dirs()
over = bool(cfg.disk_max_used_pct) and pct >= cfg.disk_max_used_pct
print(f"  {'⚠' if over else ' '}     диск занят {pct:.0f}%, порог паузы расшифровок "
      f"DISK_MAX_USED_PCT={cfg.disk_max_used_pct or 'выкл'}; временных папок расшифровки: {tmp_dirs} "
      f"(норма — не больше 2 × воркеров ASR)" + ("   ← расшифровки на паузе" if over else ""))
if over:
    problems.append("диск")
ram = resources.ram()
ram_over = bool(ram and cfg.ram_alert_pct) and ram["used_pct"] >= cfg.ram_alert_pct
print(f"  {'⚠' if ram_over else ' '}     ОЗУ ВМ: " + (f"занято {ram['used_pct']:.0f}% из "
      f"{resources.gb(ram['total'])}, тревога суперадмину с RAM_ALERT_PCT={cfg.ram_alert_pct or 'выкл'}"
      if ram else "не удалось узнать"))
if ram_over:
    problems.append("ОЗУ")
print(f"        DELETE_VIDEO_AGE_DAYS={cfg.delete_video_age_days}, MIN_DURATION={cfg.min_duration} с, "
      f"TEXT_RETENTION_DAYS={cfg.text_retention_days}, удаление через {cfg.delete_delay_sec // 3600} ч "
      f"после уведомления")

print("=== 2. База")
print(f"        путь: {cfg.db_path}")
if not os.path.exists(cfg.db_path):
    print("  FAIL  базы по этому пути нет — проверь DB_PATH")
    problems.append("БД")
else:
    c = sqlite3.connect(f"file:{cfg.db_path}?mode=ro", uri=True, timeout=10)
    rows = c.execute("SELECT status, COUNT(*) FROM recordings GROUP BY status ORDER BY 2 DESC").fetchall()
    total = sum(n for _, n in rows)
    print(f"        записей: {total}  (" + ", ".join(f"{s} {n}" for s, n in rows) + ")")
    meta = dict(c.execute("SELECT key, value FROM meta").fetchall())
    print("        проверочная фраза: " + ("перенесена в старые файлы" if meta.get("md_marker_migrated") == "1"
          else "в старые файлы ещё НЕ перенесена — запусти fix_broken_md.py (см. DEPLOY.md)"))
    if total < 1000:
        print("  ⚠     записей подозрительно мало — не та база? (до чистки было ~152 тыс.) "
              "Останови демон и пришли этот вывод")
        problems.append("БД")

print("=== 3. Толк")
from talk_api import TalkClient
print(f"        адрес: {urlsplit(TalkClient().base).netloc}")


def talk_probe():
    r = next(TalkClient().list(limit=1, page_size=1))
    return f"первая запись {r.get('key')} от {(r.get('createdDate') or '')[:10]}"


check("список записей", talk_probe,
      {401: "неверный TALK_API_TOKEN", 403: "у токена нет прав на записи",
       "*": "проверь TALK_BASE_URL и доступ ВМ к Толку"})

print("=== 4. ASR (2 секунды тишины)")
from asr import get_asr
asr = get_asr()
print(f"        адрес: {urlsplit(asr.base).netloc}, модель: {asr.model}")


def asr_probe():
    path = "/tmp/check_env_silence.wav"
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(16000)
        w.writeframes(b"\0\0" * 32000)
    with open(path, "wb") as f:
        f.write(buf.getvalue())
    try:
        j = asr._call(path, cfg.language, timeout=120)
    finally:
        os.remove(path)
    return f"ответ получен, поля: {', '.join(sorted(j)) if isinstance(j, dict) else type(j).__name__}"


check("распознавание", asr_probe,
      {401: "неверный MAGNITGPT_API_KEY", 404: "неверный MAGNITGPT_BASE_URL или MAGNITGPT_MODEL",
       "*": "шлюз недоступен или сбоит"})

print("=== 5. LLM")
from llm import get_llm, LLMTruncated
llm = get_llm()
if not llm.available():
    print("        выключен (нет MAGNITGPT_LLM_API_KEY или MAGNITGPT_LLM_MODEL) — правка и выжимка "
          "пропускаются, записи сохраняются с базовой расшифровкой")
else:
    print(f"        адрес: {urlsplit(llm.base).netloc}, модель: {llm.model} — жду ответ, медленная "
          f"модель может думать пару минут…")

    def llm_probe():
        try:
            return "ответ: " + repr((llm.chat("Отвечай одним словом.", "Скажи «ок».",
                                              max_tokens=32, timeout=180) or "")[:40])
        except LLMTruncated:
            return "отвечает (ответ упёрся в лимит теста — это нормально)"

    check("чат", llm_probe,
          {401: "неверный MAGNITGPT_LLM_API_KEY", 404: "неверный MAGNITGPT_LLM_BASE_URL или MAGNITGPT_LLM_MODEL",
           500: "шлюз LLM сбоит (как раньше) — вопрос к его владельцам", "*": "шлюз LLM недоступен"})

print("=== 6. Tag")
if not cfg.mm.url:
    print("        URL Tag не задан — проверка пропущена")
else:
    from mm_client import MMClient, admin_client
    print(f"        адрес: {cfg.mm.url}, кнопки ведут на: {cfg.mm.bot_public_url or '—'}")

    def login_name(client):
        client.login()
        return "вошёл как @" + str(client.username)

    if cfg.mm.bot_token:
        check("основной бот", lambda: login_name(MMClient(cfg.mm, verify=cfg.mm.verify)),
              {401: "неверный токен основного бота"})
    if cfg.mm.admin_bot_token:
        check("админ-бот", lambda: login_name(admin_client(cfg.mm, verify=cfg.mm.verify)),
              {401: "неверный токен админ-бота"})

print()
print("ИТОГ: " + ("всё в порядке" if not problems else "проблемы: " + ", ".join(problems)))
