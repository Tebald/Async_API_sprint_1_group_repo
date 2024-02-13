#!/bin/bash

echo "Ожидание доступности Elasticsearch и Redis..."
python ./tests/functional/utils/wait_for_services.py

if [ $? -eq 0 ]; then
    echo "Сервисы доступны. Запуск тестов..."
    pytest -rP ./tests/functional/src
else
    echo "Ошибка при ожидании сервисов. Проверьте состояние сервисов и повторите попытку."
    exit 1
fi
