FROM python:3.12.5-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY ./requirements.txt /tmp

RUN apt-get -q update \
    && apt-get install -yq --no-install-recommends build-essential curl libpq5 \
        libpq-dev \
    && pip install -U pip \
    && pip install -r /tmp/requirements.txt \
    && apt-get purge -y build-essential libpq-dev \
    && apt-get autoremove -y --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY  . /app
WORKDIR /app
