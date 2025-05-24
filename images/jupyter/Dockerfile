FROM python:3.12-slim


RUN apt-get update -y && \
    apt-get install -y \
    jq  \
    curl

COPY --from=ghcr.io/astral-sh/uv:0.4.20 /uv /bin/uv
ENV UV_SYSTEM_PYTHON=1 \
    UV_LINK_MODE=copy

WORKDIR /app

RUN --mount=src=requirements.txt,target=/tmp/requirements.txt \
    --mount=type=cache,target=/root/.cache/uv \
    uv pip install -r /tmp/requirements.txt

RUN useradd -m app_user
USER app_user