FROM python:3.10-slim as builder

ENV PATH /app/.venv/bin/:$PATH
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY poetry.lock pyproject.toml ./

RUN python -m pip install --no-cache-dir poetry==1.7.1 \
    && poetry config virtualenvs.in-project true \
    && poetry install --no-interaction --with=dev --no-ansi


FROM python:3.10-slim as prod

ENV PATH /app/.venv/bin/:$PATH
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY --from=builder /app /app
COPY src ./src
COPY  main.py .

ENTRYPOINT [ "gunicorn", "main:app", "--worker-class", "uvicorn.workers.UvicornWorker ", "--bind", "0.0.0.0:8000" ]


FROM python:3.10-slim as dev

ENV PATH /app/.venv/bin/:$PATH
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY --from=builder /app /app

ENTRYPOINT [ "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload" ]


FROM python:3.10-slim as tests

ENV PATH /app/.venv/bin/:$PATH
WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY --from=builder /app /app
COPY tests tests

ENTRYPOINT [ "pytest", "-rP", "./tests/functional/src" ]
