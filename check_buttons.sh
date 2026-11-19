#!/usr/bin/env bash
# check_buttons.sh — проверка цепочки кнопок: Tag -> ВМ:443 -> nginx -> бот:8081.
# Запускать на ВМ из папки проекта:  ./check_buttons.sh [секунд_ожидания_нажатия, по умолч. 60]
#
# Часть 1 — проверки на самой ВМ (без Tag): бот, nginx, проксирование 443 -> 8081, секрет.
# Часть 2 — ловушка: скрипт слушает порт 443 и логи; ты в это время жмёшь кнопку в Tag.
# Секрет кнопок и токены на экран не выводятся. Для tcpdump нужен sudo (спросит пароль).
set -u
cd "$(dirname "$0")"
WAIT="${1:-60}"
ok()   { printf '  \033[32mOK\033[0m   %s\n' "$*"; }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAILS=$((FAILS+1)); }
info() { printf '       %s\n' "$*"; }
FAILS=0

echo "=== 1. Контейнеры"
for s in talk-daemon talk-bot talk-nginx; do
    st="$(docker compose ps --format '{{.Service}} {{.State}} {{.Health}}' 2>/dev/null | awk -v s="$s" '$1==s {print $2, $3}')"
    case "$st" in running*) ok "$s: $st";; *) bad "$s: ${st:-не запущен}";; esac
done

echo "=== 2. Что бот считает своим адресом для кнопок"
CFG="$(docker compose exec -T talk-bot python -c "
from config import load_cfg; c = load_cfg().mm
print(c.bot_public_url); print(c.button_port); print('1' if c.button_secret else '0')" 2>/dev/null)"
PUBLIC="$(echo "$CFG" | sed -n 1p)"; BPORT="$(echo "$CFG" | sed -n 2p)"; HASSEC="$(echo "$CFG" | sed -n 3p)"
info "BOT_PUBLIC_URL = ${PUBLIC:-?}   BUTTON_PORT = ${BPORT:-?}   секрет: $([ "$HASSEC" = 1 ] && echo задан || echo НЕ задан)"
case "$PUBLIC" in https://*) ok "кнопки ведут на HTTPS (через nginx)";; *) bad "BOT_PUBLIC_URL должен быть https://<IP ВМ> (сейчас: ${PUBLIC:-пусто})";; esac
VMIP="$(echo "$PUBLIC" | sed -E 's#^https?://([^:/]+).*#\1#')"
[ -n "$VMIP" ] || VMIP="$(hostname -I | awk '{print $1}')"
info "адрес ВМ для проверок: $VMIP"

echo "=== 3. Бот отвечает напрямую (порт ${BPORT:-8081} только на 127.0.0.1)"
R="$(curl -s -m 5 "http://127.0.0.1:${BPORT:-8081}/health")"
[ "$R" = "ok" ] && ok "http://127.0.0.1:${BPORT:-8081}/health -> ok" || bad "бот напрямую не ответил (${R:-нет ответа})"

echo "=== 4. Порт 443 слушается на ВМ"
if sudo ss -ltnp 2>/dev/null | grep -q ':443 '; then
    ok "порт 443 открыт: $(sudo ss -ltnp | grep ':443 ' | awk '{print $4}' | tr '\n' ' ')"
else bad "на ВМ никто не слушает 443"; fi

echo "=== 5. nginx: сертификат и куда он проксирует"
if [ -s nginx/certs/bot.crt ] && [ -s nginx/certs/bot.key ]; then
    ok "сертификат есть: $(openssl x509 -in nginx/certs/bot.crt -noout -subject -enddate 2>/dev/null | tr '\n' ' ')"
else bad "нет nginx/certs/bot.crt или bot.key"; fi
PP="$(docker compose exec -T talk-nginx nginx -T 2>/dev/null | grep -E 'proxy_pass|listen' | sort -u | sed 's/^ *//' | tr '\n' ' ')"
info "конфиг nginx: $PP"
echo "$PP" | grep -q "proxy_pass http://talk-bot:${BPORT:-8081}" && ok "nginx проксирует на talk-bot:${BPORT:-8081}" \
    || bad "nginx проксирует не на talk-bot:${BPORT:-8081} (BUTTON_PORT и nginx/talk-bot.conf должны совпадать)"

echo "=== 6. 443 -> nginx -> бот (по адресу ВМ, как ходит Tag)"
R="$(curl -sk -m 5 "https://$VMIP/health")"
[ "$R" = "ok" ] && ok "https://$VMIP/health -> ok (nginx довёл запрос до бота)" || bad "https://$VMIP/health не ответил (${R:-нет ответа})"
CODE="$(curl -sk -m 5 -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{}' "https://$VMIP/action?t=wrong")"
[ "$CODE" = "401" ] && ok "POST /action с чужим секретом -> 401 (отказ дал сам бот — проксирование /action работает)" \
    || bad "POST /action с чужим секретом -> $CODE (ожидали 401 от бота)"

echo "=== 7. Нажатие с настоящим секретом (как его пошлёт Tag)"
R="$(docker compose exec -T talk-bot python -c "
import os, ssl, urllib.parse, urllib.request
u = 'https://$VMIP/action?t=' + urllib.parse.quote(os.environ.get('BUTTON_SECRET', ''), safe='')
try:
    r = urllib.request.urlopen(urllib.request.Request(u, data=b'{}', headers={'Content-Type': 'application/json'}),
                               context=ssl._create_unverified_context(), timeout=10)
    print(r.status, r.read().decode()[:80])
except Exception as ex:
    print('ошибка', getattr(ex, 'code', ''), ex)" 2>&1)"
case "$R" in "200 "*) ok "POST /action с секретом бота -> $R";; *) bad "POST /action с секретом бота -> $R";; esac

echo
if [ "$FAILS" -gt 0 ]; then
    echo "На самой ВМ есть проблемы ($FAILS) — сначала исправь их; Tag тут ни при чём."
else
    echo "На ВМ всё в порядке: 443 -> nginx -> бот:${BPORT:-8081} работает."
fi

echo
echo "=== 8. Ловушка: жду нажатия кнопки в Tag ${WAIT} с"
echo "   Сейчас в Tag отправь админ-боту archiever_admin (СВЕЖАЯ панель) и нажми «🔄 Обновить» 2–3 раза."
SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
CAP="$(mktemp)"
sudo timeout "$WAIT" tcpdump -ni any -l \
    "dst host $VMIP and dst port 443 and not src host $VMIP and tcp[tcpflags] & tcp-syn != 0" > "$CAP" 2>/dev/null
echo
echo "--- Попытки подключиться к ВМ:443 (не с самой ВМ):"
if [ -s "$CAP" ]; then
    sed -nE 's/.* IP ([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)\.[0-9]+ > .*/\1/p' "$CAP" | sort | uniq -c \
        | sed 's/^ */   попыток: /; s/ \([0-9.]*\)$/  с адреса \1/'
    SYN=1
else
    echo "   НИ ОДНОЙ"; SYN=0
fi
NGX="$(docker compose logs --since "$SINCE" talk-nginx 2>/dev/null | grep -E '"POST /action"|"POST /dialog"|SSL|ssl|handshake' )"
BOT="$(docker compose logs --since "$SINCE" talk-bot 2>/dev/null | grep -E 'нажатие:|окно:|неверн|не обработан')"
echo "--- nginx за это время:"; echo "${NGX:-   ничего}" | sed 's/^/   /'
echo "--- бот за это время:";   echo "${BOT:-   ничего}" | sed 's/^/   /'
rm -f "$CAP"

echo
echo "=== Вывод"
if [ "$SYN" = 0 ]; then
    echo "  Tag НЕ подключается к ВМ по 443. Цепочка рвётся ДО ВМ:"
    echo "   • в Tag не разрешён адрес ВМ: Системная консоль → Среда → Разработчик →"
    echo "     «Allow untrusted internal connections to» → добавить $VMIP;"
    echo "   • или сетевой доступ открыт не с того сервера Tag (нужны все его узлы -> $VMIP:443);"
    echo "   • точную причину пишет лог сервера Tag в момент нажатия (address forbidden / i/o timeout)."
elif ! echo "$NGX" | grep -q 'POST /action'; then
    echo "  Tag подключается (см. адреса выше), но до nginx запрос не дошёл — скорее всего,"
    echo "  Tag не принял самоподписанный сертификат: в Tag нужна «Enable insecure outgoing"
    echo "  connections» (или сертификат от корпоративного УЦ). В логе Tag будет «x509: …»."
elif echo "$NGX" | grep -q 'POST /action" 200'; then
    echo "  Нажатие дошло до бота и обработано (200). Если в Tag всё равно ошибка — пришли этот вывод."
else
    echo "  Нажатие дошло до nginx, но ответ не 200 (см. строки выше): 401 — секрет, 502/504 — бот."
fi
