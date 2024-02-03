FROM python:3.10-slim as builder

ARG EXPOSED_PORT
EXPOSE $EXPOSED_PORT
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

FROM python:3.10-slim as prod

WORKDIR /app

COPY --from=builder /usr/local /usr/local
COPY src ./src
COPY  main.py .

ENTRYPOINT [ "gunicorn", "main:app", "--worker-class", "uvicorn.workers.UvicornWorker ", "--bind", "0.0.0.0:8000" ]

FROM python:3.10-slim as dev

WORKDIR /app

COPY --from=builder /usr/local /usr/local

ENTRYPOINT [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload" ]
