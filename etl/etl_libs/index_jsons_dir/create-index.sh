#bin/bash

curl -XPUT http://127.0.0.1:9200/movies -H 'Content-Type: application/json' -d "@movies.json"
echo
curl -XPUT http://127.0.0.1:9200/persons -H 'Content-Type: application/json' -d "@persons.json"
echo
curl -XPUT http://127.0.0.1:9200/genres -H 'Content-Type: application/json' -d "@genres.json"
echo
