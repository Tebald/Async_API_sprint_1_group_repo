# Проектная работа 5 спринта

## Запуск prod версии приложения

`make prod-up` - запускает все сервисы.

После того, как все сервисы стартовали, API доступен по адресу http://127.0.0.1/api/
Спецификация http://127.0.0.1/api/openapi

## Запуск dev версии приложения

`make dev-up`

## Запуск тестов в контейнере

`make tests-up`


## Локальный запуск приложения

Для локального запуска необходимо в файле `.env` закомментировать `ES_HOST` и `REDIS_HOST` переменные. 