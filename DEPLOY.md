# Деплой на корпоративный SSH-хост (Docker)

Два сервиса на одном образе: **talk-daemon** (архивация) + **talk-bot** (Tag), перед ботом —
**talk-nginx** (HTTPS на 443 для кнопок).
Общие тома: `data/` (SQLite-БД), `Transcripts/` (расшифровки), `logs/`.

## Предусловия на хосте
- Docker + Docker Compose v2 (`docker compose version`). На **Debian 13** хватит пакетов
  дистрибутива: `sudo apt install docker.io docker-compose` (Engine 26.1 и Compose 2.26 — он же
  работает как `docker compose`); подойдёт и Docker CE из репозитория Docker. Своего
  пользователя — в группу `docker`: `sudo usermod -aG docker $USER` и перелогиниться.
- Docker должен суметь скачать базовый образ `python:3.11-slim-trixie`: Docker Hub открыт либо
  в `/etc/docker/daemon.json` задано корпоративное зеркало реестра (`registry-mirrors`).
- Хост в корп-сети — шлюз `llmlite…` (ASR/LLM) и `talk.magnit.ru` доступны **без VPN**.
- Заполненный `.env` (секреты; в git НЕ коммитим).

## Шаг 1. Код на хост

**Вариант А — через кодрепо (git):**
```bash
# локально (Mac), из папки проекта:
git add -A && git commit -m "talk archiver: deploy"
git push origin <ветка>
```
```bash
# на хосте:
ssh <user>@<host>
git clone <repo-url> talk-archiver      # или: cd talk-archiver && git pull
cd talk-archiver                        # корень репозитория — это и есть папка проекта
```

**Вариант Б — напрямую rsync (без репо):**
```bash
rsync -av --exclude .venv --exclude __pycache__ --exclude Transcripts --exclude data \
      --exclude logs --exclude '*.db*' --exclude '*.mp4' --exclude .env --exclude .git \
      ./ <user>@<host>:~/talk-archiver/
```

## Шаг 2. `.env` на хосте
```bash
cp .env.example .env
nano .env    # заполнить реальными значениями
```
Обязательно: `TALK_BASE_URL`/`TALK_API_TOKEN`; `MAGNITGPT_*` (ASR) и `MAGNITGPT_LLM_*` (LLM);
флаги `REAL_DELETE`/`SEND_TO_TAG`/`SEND_TO_MAIL`; окна `TIME_START_ASR/END_ASR`, `TIME_START_LLM/END_LLM`;
`TZ` (если окна — по Москве); `RECORD_WORKERS_ASR`/`RECORD_WORKERS_LLM`;
`MIN_DURATION`; Tag-блок (`URL`/`PORT`/`BOT_TOKEN`/`BOT_PUBLIC_URL`/`BUTTON_PORT`/
`BUTTON_SECRET`); `ADMIN_USERNAME`.
**На первый прогон — `REAL_DELETE=0`, `SEND_TO_TAG=0`, `SEND_TO_MAIL=0`** (наполнить архивы,
никому не слать, ничего не удалять).

**Два бота в Tag.** Нужны две учётки-бота: основная (`BOT_TOKEN`) и админская (`ADMIN_BOT_TOKEN`).
Вторую заводят так же, как первую (в Tag: «Интеграции» → «Аккаунты ботов»), её токен кладут в
`ADMIN_BOT_TOKEN`. Отдельный порт и сервис не нужны: обоих ботов обслуживает `talk-bot`, кнопки
обоих ведут на тот же `BOT_PUBLIC_URL`.

Если `.env` остался от прежней версии: `MODE`, `ASR_MAX_ATTEMPTS` и `EMERGENCY` больше не
используются (вместо них — флаги выше, лестница `RETRY_DELAYS_MIN` и суточная сводка админам
`ADMIN_REPORT_TIME`), их можно удалить. Каких ключей из `.env.example` в твоём `.env` нет — покажет
(только имена, без значений):
```bash
comm -13 <(grep -oE '^[A-Z_]+=' .env | sort -u) <(grep -oE '^[A-Z_]+=' .env.example | sort -u)
```

## Шаг 3. Сборка и запуск
```bash
cd ~/talk-archiver
docker compose build      # образ; pip из корп-зеркала repo.corp.tander.ru
docker compose up -d       # поднять оба сервиса
```

## Шаг 4. Проверка
```bash
docker compose ps
docker compose logs -f talk-daemon      # архивация: подхват, транскрибация, стадии
docker compose logs -f talk-bot         # бот: подключение к Tag
# либо файлы (общий том logs/):
tail -f logs/daemon.log
tail -f logs/rec_0.log                  # лог отдельного воркера
```

## Шаг 5. Обновление
```bash
git pull                  # или повторить rsync
docker compose build
docker compose up -d       # пересоздаст контейнеры новым образом (тома/БД сохранятся)
```
После правки одного лишь `.env` (без нового кода) контейнеры пересоздай явно:
`docker compose up -d --force-recreate` — переменные из `.env` читаются только при создании контейнера.

## Управление
```bash
docker compose stop        # остановить
docker compose start       # запустить
docker compose down        # удалить контейнеры (тома data/Transcripts/logs остаются)
docker compose exec talk-daemon python main.py --once   # разовый проход вручную
```

## Важное
- **Первый прод-прогон: `SEND_TO_TAG=0`.** Иначе по всей «горе» (десятки тысяч записей старше
  года) разом уйдут тысячи ЛС. Наполнил архивы/убедился в расшифровках → `SEND_TO_TAG=1` и
  `docker compose up -d`.
- **Кнопки и nginx.** Нажатия кнопок и отправку окон сервер Tag шлёт на `BOT_PUBLIC_URL`. Их
  принимает `talk-nginx` по HTTPS на 443 и передаёт боту; сам бот на ВМ слушает только
  `127.0.0.1:8081`. Что нужно:
  1. Сертификат на имя ВМ из `BOT_PUBLIC_URL`, которому доверяет Tag (лучше выпущенный корпоративным
     УЦ): на ВМ положить `nginx/certs/bot.crt` (сертификат + цепочка) и `nginx/certs/bot.key`.
     Заявку на сертификат можно сделать так:
     `openssl req -new -newkey rsa:2048 -nodes -keyout nginx/certs/bot.key -out bot.csr -subj "/CN=<имя ВМ>" -addext "subjectAltName=DNS:<имя ВМ>,IP:<IP ВМ>"`
     — `bot.csr` отдать в УЦ, полученный сертификат сохранить как `nginx/certs/bot.crt`.
  2. В `.env`: `BOT_PUBLIC_URL=https://<имя ВМ>` (без порта), `BUTTON_PORT=8081`.
  3. Сетевой доступ от серверов приложения Tag к ВМ по TCP 443.
  4. В Tag адрес ВМ добавлен в `AllowedUntrustedInternalConnections` (Tag не ходит на внутренние
     адреса без разрешения).
  Проверка с ВМ: `curl -k https://127.0.0.1/health` → `ok`; журнал запросов — `docker compose logs talk-nginx`
  (секрет из адреса туда не пишется). После смены `BOT_PUBLIC_URL` старые кнопки ведут на старый
  адрес — проверяй на новом сообщении (`archiever_admin`).
- **Общая БД.** Оба контейнера монтируют `./data` → один SQLite-файл (WAL, локальный диск — ок).
  Не клади `data/` на сетевую ФС.
- **VPN не нужен** — хост уже в корп-сети.
- **Бэкап:** `./data/recordings.db` (витрина/планировщик), `./Transcripts/` (тексты).
- **Тома — вместе.** База и `Transcripts` должны переезжать парой. Если тексты не смонтированы,
  демон видео не удалит: стадия упадёт с «расшифровки нет на диске» и попадёт в сводку админам.
- **Скорость:** поднимай `RECORD_WORKERS_LLM`, если шлюз тянет (следи за 429/5xx в логах);
  `RECORD_WORKERS_ASR` — по ОЗУ и диску ВМ (каждый ASR-воркер держит скачанные файлы и ffmpeg).
- **Диск и ОЗУ:** при заполненности диска от `DISK_MAX_USED_PCT` (по умолчанию 90%) новые
  расшифровки встают на паузу; при ОЗУ от `RAM_ALERT_PCT` (80%) — тревога. Обе — суперадмину.
  Состояние сейчас — кнопка «💾 Память» в админ-панели или `check_env.py`.
- **Проверочная фраза в старых файлах** (один раз после обновления, демон остановлен):
  `docker compose run --rm --no-deps -T --name fix-md talk-daemon python - < fix_broken_md.py`
  (отчёт), затем то же с `--apply`.
- **Перезапуск демона** — `docker compose up -d --force-recreate talk-daemon` (не `restart`: тот
  не перечитывает `.env`).
- **Автозапуск после перезагрузки ВМ** — контейнеры поднимает Docker, если он сам включён в
  автозагрузку: `systemctl is-enabled docker` должно ответить `enabled` (иначе
  `sudo systemctl enable docker`). Контейнер, остановленный вручную (`docker compose stop`), после
  перезагрузки останется остановленным — так работает `restart: unless-stopped`.
- **Копии базы** — каждое утро в `data/backups/recordings-ГГГГ-ММ-ДД.db` (7 последних, с проверкой
  целостности). Восстановление: `docker compose stop talk-daemon talk-bot`, затем
  `cp data/backups/<копия>.db data/recordings.db && rm -f data/recordings.db-wal data/recordings.db-shm`
  (копия уже содержит всё нужное), затем `docker compose up -d`. Что изменилось после копии,
  демон наверстает сам: записи, обработанные позже, пройдут стадии заново (файлы перезапишутся
  целиком), новые записи подхватятся снова, а видео, удалённые за это время, он увидит как уже
  удалённые в Толке.
- **Сводка админам:** раз в сутки (`ADMIN_REPORT_TIME`, по умолчанию 9:00) демон пишет админам в
  Tag, сколько записей за сутки встало в ожидание админа (статусы `FAILED_…`) и на каких стадиях;
  если таких нет — молчит. Нужны `ADMIN_USERNAME` и Tag-блок в `.env`; уходит даже при `SEND_TO_TAG=0`. Кнопки в
  сводке обрабатывает `talk-bot`.
- **Часовой пояс:** рабочие окна и время сводки считаются по часам контейнера, а контейнер пояс
  у ВМ НЕ берёт: пока `TZ` не задан, это UTC (9:00 = 12:00 по Москве), как бы ни была настроена
  сама ВМ. Окна задуманы по Москве — добавь в `.env` строку `TZ=Europe/Moscow` (база часовых
  поясов в образе есть) и пересоздай контейнеры: `docker compose up -d`.
- **Логи Docker** (`docker compose logs`) ротируются: не больше 3 файлов по 50 МБ на сервис
  (`x-logging` в `docker-compose.yml`). Полные логи — в `logs/` (своя ротация).
- **Файрвол.** Опубликованный порт (443 у nginx) Docker открывает в обход nftables/ufw на ВМ —
  закрывают его сетевые правила компании; бот дополнительно защищён секретом в адресе и подписями.
- **Админ-панель:** команда `archiever_admin` в ЛС боту (см. README).
