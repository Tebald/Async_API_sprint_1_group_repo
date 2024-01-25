#!/bin/sh

cd index_jsons_dir || true

for file in *.json; do
  file_no_extension="${file%.json}"
  http_code=$(curl -s -o /dev/null -I -w "%{http_code}" "http://${ES_HOST}:${ES_PORT}/${file_no_extension}")

  if ! [ "$http_code" -eq "200" ]; then
    curl -XPUT -sS http://"$ES_HOST":"$ES_PORT"/"$file_no_extension" -H 'Content-Type: application/json' -d "@$file_no_extension.json"
    echo
  fi
done
