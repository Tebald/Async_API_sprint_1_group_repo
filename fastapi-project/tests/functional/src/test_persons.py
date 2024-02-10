import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_bulk
from redis.asyncio.client import Redis

from tests.functional.settings import persons_test_settings, movies_test_settings
from tests.functional.testdata.elasticsearch_input import es_films_for_person_films
from tests.functional.utils.generate import (
    generate_person_data,
    generate_person_data_without_films,
    bulk_query_from_data,
    generate_person_data_1,
)


@pytest_asyncio.fixture(name='es_delete_record')
def es_delete_record(es_client):
    async def inner(index: str, object_id):
        if await es_client.indices.exists(index=index):
            await es_client.delete(index=index, id=object_id)

        await es_client.indices.refresh(index=index)

    return inner


@pytest_asyncio.fixture(name='es_write_data_with_class_scope_saving', scope='class')
async def es_write_data_with_class_scope_saving(es_client: AsyncElasticsearch):
    es_index = persons_test_settings.es_index
    if await es_client.indices.exists(index=es_index):
        await es_client.indices.delete(index=es_index)
    await es_client.indices.create(index=es_index, body=persons_test_settings.es_index_mapping)

    async def inner(data: list[dict]):
        _, errors = await async_bulk(client=es_client, actions=data)
        await es_client.indices.refresh(index=es_index)
        assert not errors, 'Error during Elasticsearch data push.'

    yield inner
    await es_client.indices.delete(index=es_index)


@pytest_asyncio.fixture(name='es_write_data_with_class_scope_saving2', scope='class')
async def es_write_data_with_class_scope_saving2(es_client: AsyncElasticsearch):
    es_index = movies_test_settings.es_index
    if await es_client.indices.exists(index=es_index):
        await es_client.indices.delete(index=es_index)
    await es_client.indices.create(index=es_index, body=movies_test_settings.es_index_mapping)

    async def inner(data: list[dict]):
        _, errors = await async_bulk(client=es_client, actions=data)
        await es_client.indices.refresh(index=es_index)
        assert not errors, 'Error during Elasticsearch data push.'

    yield inner
    await es_client.indices.delete(index=es_index)


@pytest_asyncio.fixture(name='create_es_index', scope='class')
async def create_test_index(es_client: AsyncElasticsearch):
    es_index = persons_test_settings.es_index
    if await es_client.indices.exists(index=es_index):
        await es_client.indices.delete(index=es_index)
    await es_client.indices.create(index=es_index, body=movies_test_settings.es_index_mapping)
    yield
    await es_client.indices.delete(index=es_index)


@pytest_asyncio.fixture(name='hello2', scope='class')
async def create_test_data():
    print(123)
    yield
    print(321)


# @pytest.mark.usefixtures("create_es_index", 'hello2')
class TestSinglePerson:
    es_index = persons_test_settings.es_index

    @pytest.mark.parametrize(
        'single_person_data', [generate_person_data(), generate_person_data_without_films(), generate_person_data_1()]
    )
    @pytest.mark.asyncio
    async def test_person_details(
        self,
        single_person_data: dict,
        es_write_data_with_class_scope_saving,
        es_write_data_with_class_scope_saving2,
        api_make_get_request,
        es_films_for_person_films,
    ):
        bulk_data_p = bulk_query_from_data(self.es_index, single_person_data)
        await es_write_data_with_class_scope_saving(bulk_data_p)

        bulk_data_m = await es_films_for_person_films()
        await es_write_data_with_class_scope_saving2(bulk_data_m)

        endpoint = f'/api/v1/persons/{single_person_data["id"]}'
        status, body = await api_make_get_request(query_data={}, endpoint=endpoint)

        assert status == 200
        assert body.get('uuid') == single_person_data.get('id')
        assert body.get('full_name') == single_person_data.get('full_name')
        assert body.get('films') == single_person_data.get('films')

    @pytest.mark.parametrize(
        'person_id, expected_status',
        [
            ('5ad2a0ae-14b4-4204-9516-a83fba77e6e8', 200),
            ('7847c001-1040-4b4c-b846-51376517ff08', 200),
            ('0ff685cc-5f78-4926-9949-c6216d13863b', 404),
            ('00000000-0000-0000-0000-000000000000', 422),
        ],
    )
    @pytest.mark.asyncio
    async def test_person_error_codes(self, person_id, expected_status, api_make_get_request):
        endpoint = f'/api/v1/persons/{person_id}'
        status, body = await api_make_get_request(query_data={}, endpoint=endpoint)
        assert status == expected_status

    @pytest.mark.parametrize(
        'single_person_data, expected_films_count',
        [(generate_person_data(), 1), (generate_person_data_without_films(), 0), (generate_person_data_1(), 2)],
    )
    @pytest.mark.asyncio
    async def test_person_films(
        self,
        single_person_data: dict,
        expected_films_count: int,
        es_films_for_person_films,
        api_make_get_request,
    ):
        endpoint = f'/api/v1/persons/{single_person_data["id"]}/film'
        status, body = await api_make_get_request(query_data={}, endpoint=endpoint)

        if body == {'detail': 'Not Found'}:
            assert status == 404
            assert expected_films_count == 0
        else:
            assert status == 200
            assert expected_films_count == len(body)

    @pytest.mark.parametrize(
        'single_person_data',
        [generate_person_data(), generate_person_data_without_films(), generate_person_data_1()],
    )
    @pytest.mark.asyncio
    async def test_person_cache(
        self,
        single_person_data: dict,
        api_make_get_request,
        redis_client: Redis,
        es_delete_record,
    ):
        person_id = single_person_data['id']

        endpoint = f'/api/v1/persons/{person_id}'

        es_status, es_body = await api_make_get_request(query_data={}, endpoint=endpoint)
        assert es_status == 200

        assert await redis_client.exists(person_id) == 1

        await es_delete_record(index=self.es_index, object_id=person_id)

        cache_status, cache_body = await api_make_get_request(query_data={}, endpoint=endpoint)
        assert cache_status == 200

        assert es_body == cache_body
