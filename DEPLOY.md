# Деплой на корпоративный SSH-хост (Docker)

Два сервиса на одном образе: **talk-daemon** (архивация) + **talk-bot** (Tag).
Общие тома: `data/` (SQLite-БД), `Transcripts/` (расшифровки), `logs/`.

## Предусловия на хосте
- Docker + Docker Compose (`docker compose version`).
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
cd talk-archiver/05_step_1
```

**Вариант Б — напрямую rsync (без репо):**
```bash
rsync -av --exclude .venv --exclude __pycache__ --exclude Transcripts --exclude data \
      --exclude logs --exclude '*.db*' --exclude '*.mp4' --exclude .env --exclude .git \
      ./ <user>@<host>:~/talk-archiver/05_step_1/
```

## Шаг 2. `.env` на хосте
```bash
cp .env.example .env
nano .env    # заполнить реальными значениями
```
Обязательно: `TALK_BASE_URL`/`TALK_API_TOKEN`; `MAGNITGPT_*` (ASR) и `MAGNITGPT_LLM_*` (LLM);
`MODE=prod`; окна `TIME_START_ASR/END_ASR`, `TIME_START_LLM/END_LLM`; `RECORD_WORKERS`;
`MIN_DURATION`; `EMERGENCY`; Tag-блок (`URL`/`PORT`/`BOT_TOKEN`/`BOT_PUBLIC_URL`/`BUTTON_PORT`/
`BUTTON_SECRET`); `ADMIN_USERNAME`.
**На первый прогон — `SEND_TO_TAG=0`** (наполнить архивы, никому не слать).

## Шаг 3. Сборка и запуск
```bash
cd 05_step_1
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
- **Порт бота.** Левое в `ports: "8081:8081"` — порт ХОСТА. Если 8081 занят (напр. feedback-бот) —
  поменяй на свободный, напр. `8086:8081`, и укажи `BOT_PUBLIC_URL=http://<host>:8086` в `.env`.
  Tag должен достучаться до `BOT_PUBLIC_URL`.
- **Общая БД.** Оба контейнера монтируют `./data` → один SQLite-файл (WAL, локальный диск — ок).
  Не клади `data/` на сетевую ФС.
- **VPN не нужен** — хост уже в корп-сети.
- **Бэкап:** `./data/recordings.db` (витрина/планировщик), `./Transcripts/` (тексты).
- **Скорость:** поднимай `RECORD_WORKERS`, если шлюз тянет (следи за 429/5xx в логах).
- **Алерты:** при недоступности шлюза дольше `EMERGENCY` часов бот напишет админам в Tag
  (нужен поднятый `talk-bot`, `ADMIN_USERNAME` и Tag-блок в `.env`; уходит даже при `SEND_TO_TAG=0`).
