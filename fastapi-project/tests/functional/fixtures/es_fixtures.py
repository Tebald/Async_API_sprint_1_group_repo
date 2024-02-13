import backoff
import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from tests.functional import test_index_settings
from tests.functional.settings import test_base_settings


@pytest_asyncio.fixture(name='es_client', scope='session')
async def es_client() -> AsyncElasticsearch:
    host = f'{test_base_settings.es_host}:{test_base_settings.es_port}'

    @backoff.on_exception(backoff.expo, Exception, max_time=30, jitter=backoff.random_jitter)
    async def get_es_client():
        return AsyncElasticsearch(hosts=[host], verify_certs=False)

    es_client = await get_es_client()
    yield es_client
    await es_client.close()


@pytest_asyncio.fixture(name='es_write_data')
def es_write_data(es_client):
    @backoff.on_exception(backoff.expo, Exception, max_time=30, jitter=backoff.random_jitter)
    async def inner(data: list[dict], settings: test_index_settings):
        """Accepts only data with in the correct Bulk form."""
        if await es_client.indices.exists(index=settings.es_index):
            await es_client.indices.delete(index=settings.es_index)

        await es_client.indices.create(index=settings.es_index, body=settings.es_index_mapping)

        _, errors = await async_bulk(client=es_client, actions=data)

        await es_client.indices.refresh(index=settings.es_index)

        assert not errors, 'Error during Elasticsearch data push.'

    return inner


@pytest_asyncio.fixture(name='es_delete_data')
def es_delete_data(es_client):
    @backoff.on_exception(backoff.expo, Exception, max_time=30, jitter=backoff.random_jitter)
    async def inner(settings: test_index_settings):
        if await es_client.indices.exists(index=settings.es_index):
            await es_client.indices.delete(index=settings.es_index)

        await es_client.indices.create(index=settings.es_index,
                                       body=settings.es_index_mapping)

        await es_client.indices.refresh(index=settings.es_index)

    return inner


@pytest_asyncio.fixture(name='es_delete_record')
@backoff.on_exception(backoff.expo, Exception, max_time=30, jitter=backoff.random_jitter)
def es_delete_record(es_client: AsyncElasticsearch):
    """Delete a single record from the Elasticsearch by its id and ES index name."""

    async def inner(index: str, object_id: str):
        if await es_client.indices.exists(index=index):
            await es_client.delete(index=index, id=object_id)
            await es_client.indices.refresh(index=index)

    return inner

