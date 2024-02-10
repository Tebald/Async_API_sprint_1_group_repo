import asyncio
from typing import Any

import aiohttp
import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from redis.asyncio import Redis

from tests.functional import test_index_settings
from tests.functional.settings import test_base_settings


@pytest_asyncio.fixture(scope='session')
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(name='client_session', scope='session')
async def client_session() -> aiohttp.ClientSession:
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest_asyncio.fixture(name='es_client', scope='session')
async def es_client() -> AsyncElasticsearch:
    host = f'{test_base_settings.es_host}:{test_base_settings.es_port}'
    es_client = AsyncElasticsearch(hosts=host, verify_certs=False)
    yield es_client
    await es_client.close()


@pytest_asyncio.fixture(name='redis_client', scope='session')
async def redis_client() -> Redis:
    redis_client = Redis(host=test_base_settings.redis_host, port=test_base_settings.redis_port)
    yield redis_client
    await redis_client.close()


@pytest_asyncio.fixture(name='redis_clear', autouse=True)
async def redis_clear(redis_client):
    await redis_client.flushdb(asynchronous=True)


@pytest_asyncio.fixture(name='es_write_data')
def es_write_data(es_client):
    async def inner(data: list[dict], settings: test_index_settings):
        if await es_client.indices.exists(index=settings.es_index):
            await es_client.indices.delete(index=settings.es_index)

        await es_client.indices.create(index=settings.es_index, body=settings.es_index_mapping)

        _, errors = await async_bulk(client=es_client, actions=data)

        await es_client.indices.refresh(index=settings.es_index)

        assert not errors, 'Error during Elasticsearch data push.'

    return inner


@pytest_asyncio.fixture(name='es_delete_data')
def es_delete_data(es_client):
    async def inner(settings: test_index_settings):
        if await es_client.indices.exists(index=settings.es_index):
            await es_client.indices.delete(index=settings.es_index)

        await es_client.indices.create(index=settings.es_index,
                                       body=settings.es_index_mapping)

        await es_client.indices.refresh(index=settings.es_index)

    return inner


@pytest_asyncio.fixture(name='api_make_get_request')
def api_make_get_request(client_session: aiohttp.ClientSession):

    async def inner(query_data: dict, endpoint: str) -> tuple[int, Any]:
        """
        :param query_data: {'query': 'The Star', 'page_number': 1, 'page_size': 50}
        :param endpoint: '/api/v1/films/search/'
        :return:
        """
        url = test_base_settings.service_url + endpoint
        async with client_session.get(url, params=query_data) as response:
            body = await response.json()
            status = response.status
        return status, body

    return inner


@pytest_asyncio.fixture(name='prepare_films_data_factory')
def prepare_films_data_factory(request):
    def _prepare_data_func(name):
        if name == "es_single_film":
            return request.getfixturevalue("es_single_film")()
        elif name == "es_films_search_data":
            return request.getfixturevalue("es_films_search_data")()
        else:
            raise ValueError(f"Unknown fixture: {name}")

    return _prepare_data_func


@pytest_asyncio.fixture(name='prepare_genres_data_factory')
def prepare_genres_data_factory(request):
    def _prepare_data_func(name):
        if name == "es_list_genres":
            return request.getfixturevalue("es_list_genres")()
        elif name == "es_single_genre":
            return request.getfixturevalue("es_single_genre")()
        else:
            raise ValueError(f"Unknown fixture: {name}")

    return _prepare_data_func
