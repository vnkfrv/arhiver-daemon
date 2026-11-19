"""Проверка файлов расшифровок: перенос проверочной фразы в старые файлы и починка испорченных.

Новые файлы архиватор пишет атомарно и заканчивает проверочной фразой (store.MARKER). Файлы,
записанные раньше, фразы не имеют — их скрипт проверяет по старому признаку (текст и ровно
один перевод строки в конце: так архиватор дописывал файлы всегда):
  • целые — дописывает в конец проверочную фразу;
  • пустые, обрезанные, битые посреди буквы, без текста под шапкой, пропавшие — откладывает
    в <файл>.broken (пустые удаляет) и возвращает запись на переделку этой стадии: расшифровку —
    заново из Толка, правку и выжимку — заново через LLM;
  • пустые остатки упавших попыток ASR — удаляет.
После правки ставит в базе отметку: с этого момента демон считает файл без фразы неполным.
Без --apply ничего не меняет (только отчёт). Больше 300 записей к переделке — правка только
с --many (сначала стоит посмотреть отчёт).

Запуск при ОСТАНОВЛЕННОМ демоне (он не должен писать файлы одновременно со скриптом):
  docker compose run --rm --no-deps -T --name fix-md talk-daemon python - [--apply] < fix_broken_md.py
"""
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone

import db
import pipeline
import store
from config import load_cfg

APPLY = "--apply" in sys.argv
MANY = "--many" in sys.argv
LIMIT = pipeline.REDO_LIMIT
TS = re.compile(r"\[\d\d:\d\d:\d\d-(\d\d):(\d\d):(\d\d)\]")
SHOW = 30
stamp = f"{datetime.now():%Y%m%d-%H%M%S}"


def ends_early(path, duration):
    """Подозрение на незаметную обрезку (ровно по границе строки) у файла без фразы: дословная
    расшифровка кончается раньше половины записи. Бывает и честно (запись шла в тишине) —
    только в отчёт."""
    if not duration or duration < 600:
        return None
    with open(path, encoding="utf-8") as f:
        ends = TS.findall(f.read())
    if not ends:
        return "нет ни одного таймкода"
    h, m, s = map(int, ends[-1])
    last = h * 3600 + m * 60 + s
    return f"текст кончается на {last // 60} мин из {duration // 60}" if last < duration / 2 else None


cfg = load_cfg()
conn = db.connect(cfg.db_path)
fix, lost, leftovers, to_mark, suspicious = [], [], [], [], []
for r in conn.execute("SELECT * FROM recordings"):
    row, key = dict(r), r["recording_key"]
    rd = row["rec_dir"] or ""
    if not row["transcribed"]:
        p = store.asr_path(rd, key)      # остаток упавшей попытки ASR — её следующая попытка перезапишет
        if os.path.exists(p) and not os.path.getsize(p):
            leftovers.append(p)
        continue
    broken = pipeline.broken_outputs(row, strict=False)
    if broken:
        (lost if row["deleted"] or row["text_cleaned"] else fix).append((row, broken))
        continue
    for kind, flag, path in pipeline.OUTPUTS:
        p = path(rd, key)
        if row[flag] and store.problem(p, strict=True):      # целый, но ещё без фразы
            to_mark.append(p)
            if kind == "asr" and (why := ends_early(p, row["duration_sec"])):
                suspicious.append(f"{key} [{row['status']}]: {why}")

report = [f"=== к повторной обработке: {len(fix)}"]
report += [f"{row['recording_key']} [{row['status']}]: {pipeline.describe(b)}" for row, b in fix]
report.append(f"=== испорчены, но видео уже удалено — заново не расшифровать: {len(lost)}")
report += [f"{row['recording_key']} [{row['status']}]: {pipeline.describe(b)}" for row, b in lost]
report.append(f"=== пустые остатки упавших попыток ASR (просто удаляются): {len(leftovers)}")
report += leftovers
report.append(f"=== целые старые файлы — дописать проверочную фразу: {len(to_mark)}")
report.append(f"=== подозрительные (только список, ничего не меняю): {len(suspicious)}")
report += suspicious
path = os.path.join(os.path.dirname(os.path.abspath(cfg.db_path)), f"fix_md_report-{stamp}.txt")
with open(path, "w", encoding="utf-8") as f:
    f.write("\n".join(report) + "\n")

shown = 0
for line in report:                  # на экран — заголовки разделов и до SHOW строк в каждом
    if line.startswith("==="):
        print(line)
        shown = 0
    elif shown < SHOW:
        print("  " + line)
        shown += 1
    elif shown == SHOW:
        print("  … остальное в отчёте")
        shown += 1
migrated = pipeline.strict_files(conn)
print("полный отчёт:", path)
print("проверочная фраза уже перенесена в старые файлы" if migrated else
      "проверочная фраза в старые файлы ещё не перенесена")

if not APPLY:
    print("ПРОБНЫЙ ПРОГОН, ничего не изменено (для правки: --apply)")
    sys.exit()
try:
    seen = datetime.fromisoformat(json.loads(db.get_meta(conn, "daemon_state") or "{}").get("at", ""))
    alive = (datetime.now(timezone.utc) - seen).total_seconds() < 90   # отмечается раз в минуту
except (ValueError, TypeError):
    alive = False
if alive:
    print("СТОП: демон работает (отмечался меньше полутора минут назад) — он пишет те же файлы. "
          "Останови его (docker compose stop talk-daemon); если только что остановил — подожди "
          "минуту и запусти снова.")
    sys.exit(1)
if len(fix) > LIMIT and not MANY:
    print(f"СТОП: к правке {len(fix)} записей (больше {LIMIT}) — сначала пришли отчёт на проверку; "
          f"применить всё равно: --apply --many")
    sys.exit(1)

backup = os.path.join(os.path.dirname(os.path.abspath(cfg.db_path)), f"recordings.backup-{stamp}.db")
with sqlite3.connect(backup) as b:
    conn.backup(b)                   # копия вместе со свежими изменениями из -wal
print("копия базы до правки:", backup)

for row, broken in fix:
    pipeline.set_aside(row, broken)
    db.update(conn, row["recording_key"], **pipeline.redo_fields(row, broken))
for p in leftovers:
    os.remove(p)
errors = []
for p in to_mark:
    # файл кончается «текст\n» (проверено выше) — дописываем «\nфраза\n»: ровно как пишет _write
    with open(p, "a", encoding="utf-8") as f:
        f.write("\n" + store.MARKER + "\n")
    if store.problem(p, strict=True):
        errors.append(p)
os.sync()
if errors:
    print(f"НЕ УДАЛОСЬ дописать фразу в {len(errors)} файлов (отметку не ставлю, пришли вывод):")
    print("\n".join("  " + p for p in errors[:SHOW]))
    sys.exit(1)
db.set_meta(conn, store.MARKER_MIGRATED, "1")
print(f"ПРИМЕНЕНО: на переделку — {len(fix)}, пустых остатков удалено — {len(leftovers)}, "
      f"фраза дописана в {len(to_mark)} файлов. Отметка поставлена: дальше файл без фразы "
      f"считается неполным.")
