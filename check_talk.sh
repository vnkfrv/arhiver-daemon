#!/usr/bin/env bash
# check_talk.sh — проверить запись прямо в API Толка (из папки проекта: на ВМ или на Mac).
#
#   ./check_talk.sh --list [N]    # N самых старых записей из Толка (ключ, дата, длительность)
#   ./check_talk.sh --keys        # (на ВМ) примеры ключей из базы: «без аудио», «без дорожек»…
#   ./check_talk.sh <ключ>        # что Толк отдаёт по записи: детали, аудиоархив, видео (и есть ли
#                                 # в нём звук), свою транскрипцию и краткий пересказ / протокол
#
# ffmpeg берётся из контейнера демона, если он запущен, иначе — локальный (ffmpeg в PATH или
# из .venv проекта).
#
# Адрес и токен Толка берутся из .env; токен на экран и в список процессов не попадает.
# Скачанное складывается в data/check_<ключ>/ — после проверки эту папку можно удалить.
set -u
cd "$(dirname "$0")"

env_get() {   # значение ключа из .env без инлайн-комментария и пробелов по краям
    grep -E "^$1=" .env | tail -1 | cut -d= -f2- | sed -E 's/[[:space:]]+#.*$//; s/^[[:space:]]+//; s/[[:space:]]+$//'
}
BASE="$(env_get TALK_BASE_URL)"; BASE="${BASE%/}"
[ -n "$BASE" ] || { echo "[!] в .env нет TALK_BASE_URL"; exit 1; }
INSECURE=""; [ "$(env_get TALK_VERIFY_SSL)" = "0" ] && INSECURE="-k"
HDR="$(mktemp)"; chmod 600 "$HDR"; trap 'rm -f "$HDR"' EXIT
printf 'X-Auth-Token: %s\n' "$(env_get TALK_API_TOKEN)" > "$HDR"

if [ "${1:-}" = "--list" ]; then
    curl -sS $INSECURE -H @"$HDR" "$BASE/api/Domain/recordings/v2?top=${2:-10}&orderMode=byTimeOldFirst" \
        | python3 -c "import json,sys
d = json.load(sys.stdin)
for e in d.get('entities') or []:
    print(f\"  {e.get('key')}  {(e.get('createdDate') or '')[:10]}  {round((e.get('duration') or 0)/60)} мин  «{(e.get('title') or '')[:50]}»\")"
    exit 0
fi

if [ "${1:-}" = "--keys" ]; then
    docker compose exec -T talk-daemon python - <<'EOF'
import sqlite3
c = sqlite3.connect("data/recordings.db")
for title, where in (("без аудио", "no_audio=1"), ("без дорожек по спикерам", "no_channels=1"),
                     ("обычные (расшифрованы)", "transcribed=1 AND no_channels=0")):
    print(f"--- {title}:")
    for key, name, err in c.execute(f"SELECT recording_key, title, last_error FROM recordings "
                                    f"WHERE {where} ORDER BY created_date DESC LIMIT 3"):
        print(f"  {key}  «{(name or '')[:40]}»  {(err or '')[:90]}")
EOF
    exit 0
fi

KEY="${1:?укажи ключ записи: ./check_talk.sh <ключ>  (примеры: ./check_talk.sh --list)}"
OUT="data/check_$KEY"; mkdir -p "$OUT"

req() {   # req <путь API> <файл> -> «код тип размер»
    curl -sS $INSECURE -H @"$HDR" --max-time 3600 -o "$2" \
         -w '%{http_code} %{content_type} %{size_download}' "$BASE$1" 2>&1
}
show_json() {   # краткая выжимка из JSON-ответа (python-фрагмент получает объект в d)
    python3 -c "import json,sys
try: d = json.load(open(sys.argv[1], encoding='utf-8'))
except Exception as ex: print('    не JSON:', open(sys.argv[1], 'rb').read(300)); sys.exit()
$2" "$1"
}

echo "=== 1. Детали записи"
echo "    $(req "/api/Domain/recordings/$KEY" "$OUT/detail.json")"
show_json "$OUT/detail.json" "
for k in ('title', 'createdDate', 'duration', 'size', 'participantsCount', 'canBeRemoved', 'immutable'):
    print(f'    {k}: {d.get(k)}')"

echo "=== 2. Аудиоархив (как его берёт демон)"
echo "    $(req "/api/recordings/$KEY/audio" "$OUT/audio.zip")"
python3 - "$OUT/audio.zip" <<'EOF'
import json, sys, zipfile
p = sys.argv[1]
try:
    z = zipfile.ZipFile(p)
except zipfile.BadZipFile:
    print("    не архив, ответ Толка:", open(p, "rb").read(300).decode("utf-8", "replace"))
    sys.exit()
names = z.namelist()
info = json.loads(z.read("info.json")) if "info.json" in names else {}
chans = info.get("channels") or []
files = [f.get("path") for ch in chans for f in ch.get("files", [])]
print(f"    файлов в архиве: {len(names)}; дорожек по спикерам в info.json: {len(chans)} "
      f"(из них в архиве: {sum(f in names for f in files)})")
print(f"    общие дорожки: {[i.get('audio') for i in info.get('items') or []]}; audio.m4a: {'audio.m4a' in names}")
print("    первые файлы:", names[:8])
EOF

echo "=== 3. Видео (без качества; запасной вариант — /900p)"
VID="$OUT/video.mp4"
R="$(req "/api/Recordings/$KEY/file" "$VID")"; echo "    /file      -> $R"
case "$R" in 200*) ;; *) R="$(req "/api/Recordings/$KEY/file/900p" "$VID")"; echo "    /file/900p -> $R";; esac
case "$R" in
    200*) if docker compose ps --status running talk-daemon 2>/dev/null | grep -q talk-daemon; then
              PROBE="$(docker compose exec -T talk-daemon ffmpeg -hide_banner -i "/app/$VID" 2>&1)"
          else
              FF="$(command -v ffmpeg || .venv/bin/python -c 'import imageio_ffmpeg; print(imageio_ffmpeg.get_ffmpeg_exe())' 2>/dev/null)"
              PROBE="$("${FF:-ffmpeg}" -hide_banner -i "$VID" 2>&1)"
          fi
          PROBE="$(echo "$PROBE" | grep -E "Duration|Stream")"
          echo "    дорожки в видео (ffmpeg):"; echo "$PROBE" | sed 's/^/      /'
          if echo "$PROBE" | grep -q "Audio:"; then echo "    => звук в видео ЕСТЬ — демон распознает его"
          else echo "    => звуковой дорожки в видео НЕТ — это «запись без аудио»"; fi;;
    *) echo "    видео не скачалось, ответ:"; head -c 300 "$VID"; echo;;
esac

echo "=== 4. Транскрипция Толка"
echo "    $(req "/api/recordings/$KEY/transcript" "$OUT/transcript.json")"
show_json "$OUT/transcript.json" "
tracks = d.get('tracks') or []
print('    status:', d.get('status'), '|', d.get('statusMessage') or '')
print('    дорожек:', len(tracks))
for t in tracks[:3]:
    sp = (t.get('speaker') or {}).get('userInfo') or t.get('speaker') or {}
    print('     ', sp.get('firstname') or sp.get('anonymousName') or '?', sp.get('surname') or '',
          '| кусков:', len(t.get('chunks') or []), '|', json.dumps((t.get('chunks') or [None])[0], ensure_ascii=False)[:200])"

for TYPE in shortSummary protocol; do
    echo "=== 5. Пересказ Толка: $TYPE"
    echo "    $(req "/api/recordings/$KEY/summary/$TYPE" "$OUT/$TYPE.json")"
    show_json "$OUT/$TYPE.json" "
chunks = d.get('chunks') or []
print('    status:', d.get('status'), '| кусков:', len(chunks), '| скрыт:', d.get('hidden'))
print('   ', ' '.join((c.get('text') or '') for c in chunks)[:600])"
done

echo
echo "Ответы целиком — в $OUT/ (после проверки: rm -rf $OUT)"
