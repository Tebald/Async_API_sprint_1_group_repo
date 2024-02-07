import json
from json import JSONDecodeError

from pydantic import BaseSettings, Field


def retrieve_state(file_path: str) -> dict:
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except (FileNotFoundError, JSONDecodeError):
        return dict()


class TestBaseSettings(BaseSettings):
    es_host: str = Field('127.0.0.1', env='ES_HOST')
    es_port: int = Field(9200, env='ES_PORT')
    redis_host: str = Field('127.0.0.1', env='REDIS_HOST')
    redis_port: int = Field(6379, env='REDIS_PORT')

    service_url: str = Field('http://0.0.0.0:8000', env='SERVICE_HOST')

    class Config:
        env_file = '../../.env'


class TestMoviesSettings(TestBaseSettings):
    es_index: str = Field('movies', env='ES_TEST_INDEX_MOVIES')
    # es_id_field: str = ...
    es_index_mapping: dict = retrieve_state('./testdata/movies.json')


class TestGenresSettings(TestBaseSettings):
    es_index: str = Field('genres', env='ES_TEST_INDEX_GENRES')
    es_index_mapping: dict = retrieve_state('./testdata/genres.json')


class TestPersonsSettings(TestBaseSettings):
    es_index: str = Field('persons', env='ES_TEST_INDEX_PERSONS')
    es_index_mapping: dict = retrieve_state('./testdata/persons.json')


test_base_settings = TestBaseSettings()
movies_test_settings = TestMoviesSettings()
genres_test_settings = TestGenresSettings()
persons_test_settings = TestPersonsSettings()
