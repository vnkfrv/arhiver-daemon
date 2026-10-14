# Python 3.11 на Debian 13 (trixie). Сейчас это и есть python:3.11-slim, но явный суффикс не даст
# образу молча «переехать» на следующий Debian при очередной пересборке. В образе уже есть tzdata
# (TZ из .env работает), ca-certificates и LANG=C.UTF-8.
FROM python:3.11-slim-trixie

# логи Python не буферизуются -> видны в docker logs сразу
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Хост закрыт от pypi.org И от deb.debian.org -> всё ставим из внутреннего зеркала PyPI.
# На машине с интернетом (Mac) переопределить:
#   --build-arg PIP_INDEX_URL=https://pypi.org/simple --build-arg PIP_TRUSTED_HOST=pypi.org
ARG PIP_INDEX_URL=https://repo.corp.tander.ru/repository/pypi/simple
ARG PIP_TRUSTED_HOST=repo.corp.tander.ru

# ffmpeg нужен ДЕМОНУ (asr.py режет аудио на куски). Ставим НЕ через apt, а статическим
# бинарём из пакета imageio-ffmpeg (тянется из корп-зеркала PyPI) и линкуем его в PATH,
# чтобы asr.py вызывал `ffmpeg` как обычную программу. `ffmpeg -version` в конце —
# санити-чек, что бинарь распаковался и запускается.
COPY requirements.txt .
RUN pip install --no-cache-dir \
    --index-url "$PIP_INDEX_URL" \
    --trusted-host "$PIP_TRUSTED_HOST" \
    -r requirements.txt \
    && ln -sf "$(python -c 'import imageio_ffmpeg; print(imageio_ffmpeg.get_ffmpeg_exe())')" /usr/local/bin/ffmpeg \
    && ffmpeg -version

COPY . .

# порт HTTP-сервера кнопок/модалок бота (BUTTON_PORT внутри контейнера)
EXPOSE 8081

# По умолчанию — ДЕМОН. Бот запускается тем же образом с командой `python bot.py`
# (см. docker-compose.yml).
CMD ["python", "main.py"]
