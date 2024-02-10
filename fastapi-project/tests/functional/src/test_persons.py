import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch._async.helpers import async_bulk

from tests.functional.settings import persons_test_settings
from tests.functional.utils.generate import (generate_person_data, generate_person_data_without_films,
                                             bulk_query_from_data, )


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
    await es_client.indices.delete(index=persons_test_settings.es_index)


class TestSinglePerson:
    @pytest.mark.parametrize('single_person_data', [generate_person_data(), generate_person_data_without_films()])
    @pytest.mark.asyncio
    async def test_person_details(self, single_person_data: dict, es_write_data_with_class_scope_saving, api_make_get_request):
        bulk_data = bulk_query_from_data(single_person_data)
        await es_write_data_with_class_scope_saving(bulk_data)

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
