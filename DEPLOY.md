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
`MIN_DURATION`; Tag-блок (`URL`/`PORT`/`BOT_TOKEN`/`BOT_PUBLIC_URL`/`BUTTON_PORT`/
`BUTTON_SECRET`); `ADMIN_USERNAME`.
**На первый прогон — `SEND_TO_TAG=0`** (наполнить архивы, никому не слать).

**Два бота в Tag.** Нужны две учётки-бота: основная (`BOT_TOKEN`) и админская (`ADMIN_BOT_TOKEN`).
Вторую заводят так же, как первую (в Tag: «Интеграции» → «Аккаунты ботов»), её токен кладут в
`ADMIN_BOT_TOKEN`. Отдельный порт и сервис не нужны: обоих ботов обслуживает `talk-bot`, кнопки
обоих ведут на тот же `BOT_PUBLIC_URL`.

Если `.env` остался от прежней версии: `ASR_MAX_ATTEMPTS` и `EMERGENCY` больше не используются
(вместо них — лестница `RETRY_DELAYS_MIN` и суточная сводка админам `ADMIN_REPORT_TIME`), их можно
удалить.

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
- **Тома — вместе.** База и `Transcripts` должны переезжать парой. Если тексты не смонтированы,
  демон видео не удалит: стадия упадёт с «расшифровки нет на диске» и попадёт в сводку админам.
- **Скорость:** поднимай `RECORD_WORKERS`, если шлюз тянет (следи за 429/5xx в логах).
- **Сводка админам:** раз в сутки (`ADMIN_REPORT_TIME`, по умолчанию 9:00) демон пишет админам в
  Tag, сколько записей за сутки встало в ожидание админа (статусы `FAILED_…`) и на каких стадиях;
  если таких нет — молчит. Нужны `ADMIN_USERNAME` и Tag-блок в `.env`; уходит даже при `SEND_TO_TAG=0`. Кнопки в
  сводке обрабатывает `talk-bot`.
- **Часовой пояс:** рабочие окна и время сводки считаются по часам контейнера. В `docker-compose.yml`
  пояс не задан — значит, это UTC (9:00 = 12:00 по Москве). Нужен другой — задай `TZ` в `.env`
  (например `TZ=Europe/Moscow`); учти, что вместе со сводкой сдвинутся и окна.
- **Админ-панель:** команда `archiever_admin` в ЛС боту (см. README).
