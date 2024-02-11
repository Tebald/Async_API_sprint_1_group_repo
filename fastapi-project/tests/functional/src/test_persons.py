from typing import cast

import pytest
from redis.asyncio.client import Redis

from tests.functional.conftest import api_make_get_request, es_delete_record
from tests.functional.settings import persons_test_settings
from tests.functional.testdata.elasticsearch_input import data_to_bulk, es_person_with_four_films, es_person_with_two_films, es_person_without_films


@pytest.mark.asyncio
async def test_no_persons_data(api_make_get_request, es_delete_data):
    await es_delete_data(persons_test_settings)
    status, body = await api_make_get_request({}, '/api/v1/persons/')
    assert status == 404


@pytest.fixture(params=['es_person_with_four_films', 'es_person_with_two_films', 'es_person_without_films'])
def data(request, es_person_with_four_films, es_person_with_two_films, es_person_without_films):
    return {
        'es_person_with_four_films': es_person_with_four_films,
        'es_person_with_two_films': es_person_with_two_films,
        'es_person_without_films': es_person_without_films,
    }


@pytest.mark.asyncio
async def test_person_details(data, es_write_data, api_make_get_request, data_to_bulk):
    """
    Endpoint /api/v1/persons/{person_id}.
    Get uuid of person.
    Return related information.
    """
    for obj in data.values():
        person = await obj()
        person_bulk = data_to_bulk(person)
        await es_write_data(data=person_bulk, settings=persons_test_settings)

        endpoint = '/api/v1/persons/' + person['id']
        status, body = await api_make_get_request(query_data={}, endpoint=endpoint)

        assert status == 200
        assert body.get('uuid') == person.get('id')
        assert body.get('full_name') == person.get('full_name')
        assert body.get('films') == person.get('films')


@pytest.mark.parametrize(
    'person,films_count',
    [
        ('es_person_with_four_films', 4),
        ('es_person_with_two_films', 2),
        ('es_person_without_films', 0),
    ],
)
@pytest.mark.asyncio
async def test_person_films(person: dict, films_count: int, api_make_get_request):
    """Endpoint {person_id}/film."""
    person = cast(dict, person)
    p_id = person.get('id')
    endpoint = f'/api/v1/persons/{p_id}/film'
    status, body = await api_make_get_request(query_data={}, endpoint=endpoint)

    if body == {'detail': 'Not Found'}:
        assert status == 404
        assert films_count == 0
    else:
        assert status == 200
        assert films_count == len(body)


@pytest.mark.parametrize('person', ['es_person_with_two_films'])
@pytest.mark.asyncio
async def test_person_cache(
    person: dict,
    api_make_get_request,
    redis_client: Redis,
    es_delete_record,
):
    endpoint = f'/api/v1/persons/{person["id"]}'

    es_status, es_body = await api_make_get_request(query_data={}, endpoint=endpoint)

    assert es_status == 200
    assert await redis_client.exists(person['id']) == 1

    await es_delete_record(index='persons', object_id=person['id'])

    cache_status, cache_body = await api_make_get_request(query_data={}, endpoint=endpoint)
    assert cache_status == 200

    assert es_body == cache_body
