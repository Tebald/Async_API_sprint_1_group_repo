import pytest

from tests.functional.settings import persons_test_settings
from tests.functional.testdata.elasticsearch_input import es_single_person_data


@pytest.mark.parametrize('person_id, expected_status', [
    ('5ad2a0ae-14b4-4204-9516-a83fba77e6e8', 200),
    ('0ff685cc-5f78-4926-9949-c6216d13863b', 404),
    ('00000000-0000-0000-0000-000000000000', 422)
])
@pytest.mark.asyncio
async def test_person_details(
        es_single_person_data,
        es_write_data,
        api_make_get_request,
        person_id,
        expected_status
):
    es_data = await es_single_person_data()
    await es_write_data(data=es_data, settings=persons_test_settings)

    endpoint = f'/api/v1/persons/{person_id}'
    status, body = await api_make_get_request(query_data={}, endpoint=endpoint)

    assert status == expected_status
