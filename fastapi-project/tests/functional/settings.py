"""
Module to store all test settings in one place.
"""
import json
import os
from json import JSONDecodeError

from pydantic import BaseSettings, Field

current_dir = os.path.dirname(os.path.abspath(__file__))


def retrieve_state(file_path: str) -> dict:
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            return json.load(file)
    except (FileNotFoundError, JSONDecodeError) as exc:
        raise Exception(f'Can not open file {file_path}') from exc


class TestBaseSettings(BaseSettings):
    es_host: str = Field('127.0.0.1', env='ES_HOST')
    es_port: int = Field(9200, env='ES_PORT')
    redis_host: str = Field('127.0.0.1', env='REDIS_HOST')
    redis_port: int = Field(6379, env='REDIS_PORT')

    service_host: str = Field('0.0.0.0', env='API_HOST')
    service_port: int = Field(8000, env='API_PORT')


class TestMoviesSettings(TestBaseSettings):
    es_index: str = Field('movies', env='ES_TEST_INDEX_MOVIES')
    file_path = os.path.join(current_dir, 'testdata', 'movies.json')
    es_index_mapping: dict = retrieve_state(file_path)


class TestGenresSettings(TestBaseSettings):
    es_index: str = Field('genres', env='ES_TEST_INDEX_GENRES')
    file_path = os.path.join(current_dir, 'testdata', 'genres.json')
    es_index_mapping: dict = retrieve_state(file_path)


class TestPersonsSettings(TestBaseSettings):
    es_index: str = Field('persons', env='ES_TEST_INDEX_PERSONS')
    file_path = os.path.join(current_dir, 'testdata', 'persons.json')
    es_index_mapping: dict = retrieve_state(file_path)


test_base_settings = TestBaseSettings()
movies_test_settings = TestMoviesSettings()
genres_test_settings = TestGenresSettings()
persons_test_settings = TestPersonsSettings()
