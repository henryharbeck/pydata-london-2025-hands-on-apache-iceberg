FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv
ENV UV_SYSTEM_PYTHON=1

WORKDIR /app

RUN --mount=src=requirements.txt,target=/tmp/requirements.txt uv pip install -r /tmp/requirements.txt

RUN useradd -m app_user
USER app_user
COPY --link marimo.toml /home/app_user/.config/marimo/marimo.toml
