from elasticsearch import Elasticsearch
import os
import json
import logging


def create_index(client, index_name, document):
    client.index(index=index_name, body=document)


def index_files_in_directory(directory_path, es_host):
    client = Elasticsearch(es_host)
    for filename in os.listdir(directory_path):
        if not filename.endswith(".json"):
            continue

        file_path = os.path.join(directory_path, filename)
        with open(file_path, 'r') as file:
            document = json.load(file)

        index_name = filename.partition('.')[0]
        create_index(client, index_name, document)
        logging.info(f"Создан индекс {index_name}")
