#!/usr/bin/env bash
# run.sh — запуск ДЕМОНА архивации на хосте (без контейнера).
# Демон сам себя планирует: работает в ОБЯЗАТЕЛЬНЫХ рабочих окнах (TIME_START_ASR/LLM),
# внешний cron не нужен. Бота Tag запускать отдельно:  ./run.sh bot
#
#   ./run.sh            # демон (бесконечно)
#   ./run.sh --once     # один проход и выход
#   ./run.sh bot        # интерактивный бот Tag (bot.py)
set -euo pipefail
cd "$(dirname "$0")"

# ffmpeg/ffprobe должны быть в PATH (под сервисами/cron PATH бывает урезан).
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

if [ "${1:-}" = "bot" ]; then
    shift
    exec .venv/bin/python bot.py "$@"
fi
exec .venv/bin/python main.py "$@"
