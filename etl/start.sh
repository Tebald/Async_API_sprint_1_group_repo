#!/bin/sh

./create_index.sh
echo "Индекс создали"

while true; do
    python main.py
    sleep "${INTERVAL:-120}"
done
