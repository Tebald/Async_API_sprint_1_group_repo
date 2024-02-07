import asyncio
import aiohttp

import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from tests.functional import test_index_settings
from tests.functional.settings import test_base_settings


@pytest_asyncio.fixture(scope='session')
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(name='client_session', scope='session')
async def client_session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest_asyncio.fixture(name='es_client', scope='session')
async def es_client():
    host = f'{test_base_settings.es_host}:{test_base_settings.es_port}'
    es_client = AsyncElasticsearch(hosts=host, verify_certs=False)
    yield es_client
    await es_client.close()


@pytest_asyncio.fixture(name='es_write_data')
def es_write_data():

    async def inner(data: list[dict], settings: test_index_settings):

        host = f'{test_base_settings.es_host}:{test_base_settings.es_port}'
        es_client = AsyncElasticsearch(hosts=host, verify_certs=False)
        try:
            if await es_client.indices.exists(index=settings.es_index):
                await es_client.indices.delete(index=settings.es_index)
                await asyncio.sleep(1)

            await es_client.indices.create(index=settings.es_index, body=settings.es_index_mapping)

            await es_client.indices.refresh(index=settings.es_index)

            updated, errors = await async_bulk(client=es_client, actions=data)

            await es_client.indices.refresh(index=settings.es_index)
        finally:
            await es_client.close()

        if errors:
            raise Exception('Error during Elasticsearch data push.')
    return inner


@pytest_asyncio.fixture(name='es_delete_data')
def es_delete_data():
    async def inner(settings: test_index_settings):
        host = f'{test_base_settings.es_host}:{test_base_settings.es_port}'
        es_client = AsyncElasticsearch(hosts=host, verify_certs=False)

        async with es_client as es:
            try:
                if await es.indices.exists(index=settings.es_index):
                    await es.indices.delete(index=settings.es_index)
                    await asyncio.sleep(1)

                await es_client.indices.create(index=settings.es_index,
                                               body=settings.es_index_mapping)

                await es_client.indices.refresh(index=settings.es_index)
            finally:
                await es_client.close()

    return inner


@pytest_asyncio.fixture(name='api_make_get_request')
def api_make_get_request(client_session):

    async def inner(query_data: dict, endpoint: str):
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
