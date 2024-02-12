import pytest
import pytest_asyncio
from redis.asyncio.client import Redis

from tests.functional.conftest import api_make_get_request, es_delete_record
from tests.functional.settings import persons_test_settings
from tests.functional.testdata.elasticsearch_input import (  # type: ignore # isort: skip
    es_person_with_four_films,
    es_person_with_two_films,
    es_person_without_films,
)
from tests.functional.testdata.elasticsearch_input import from_dict_to_bulk


@pytest_asyncio.fixture(params=['es_person_with_four_films', 'es_person_with_two_films', 'es_person_without_films'])
def es_persons_bulks(request):
    """Fixture, containing all single person fixtures, used in tests."""
    fixture = request.param
    return request.getfixturevalue(fixture)


@pytest.mark.asyncio
async def test_no_persons_data(api_make_get_request, es_delete_data):
    await es_delete_data(persons_test_settings)
    status, body = await api_make_get_request({}, '/api/v1/persons/')
    assert status == 404


@pytest.mark.asyncio
async def test_person_details(es_persons_bulks, es_write_data, api_make_get_request):
    """
    Test for endpoint /api/v1/persons/{person_id}

    Asserted fields:
    uuid: An ID of person.
    full_name: A fullname of person.
    films: A list of films, related to person.

    Check that endpoint returns correct person by:
    0. Set test data by es_write_data.
    1. Get person data.
    2. Assert result fields with input.
    """
    person_bulk = es_persons_bulks
    person = person_bulk[0]['_source']

    await es_write_data(data=person_bulk, settings=persons_test_settings)

    endpoint = '/api/v1/persons/' + person['id']
    status, body = await api_make_get_request(query_data={}, endpoint=endpoint)

    assert status == 200
    assert body.get('uuid') == person.get('id')
    assert body.get('full_name') == person.get('full_name')
    assert body.get('films') == person.get('films')


@pytest.mark.asyncio
async def test_person_films(es_persons_bulks, es_write_data, api_make_get_request):
    """
    Test for endpoint /api/v1/persons/{person_id}/film

    Check that endpoint returns correct list of films for person by:
    0. Set test data by es_write_data.
    1. Get films.
    2. Get length of films list for input person.
    3. Get length of api answer.
    3. Assert both lengths.
    """
    person_bulk = es_persons_bulks
    person = person_bulk[0]['_source']
    await es_write_data(data=person_bulk, settings=persons_test_settings)

    endpoint = f'/api/v1/persons/{person["id"]}/film'
    status, body = await api_make_get_request(query_data={}, endpoint=endpoint)

    if body == {'detail': 'Not Found'}:
        assert len(person['films']) == 0
    else:
        assert status == 200
        assert len(person['films']) == len(body)


@pytest.mark.asyncio
async def test_person_cache(
    es_persons_bulks,
    redis_client: Redis,
    api_make_get_request,
    es_delete_record,
    from_dict_to_bulk,
    es_write_data,
):
    """
    Test for endpoint /api/v1/persons/{person_id}

    Check Redis caching by:
    0. Set test data by es_write_data.
    1. Get person.
    2. Remove it from ES.
    3. Get person again.
    4. Assert datas.

    If person not found in cache - cache did not work.
    """
    person_bulk = es_persons_bulks
    person = person_bulk[0]['_source']

    await es_write_data(data=person_bulk, settings=persons_test_settings)

    endpoint = f'/api/v1/persons/{person["id"]}'

    es_status, es_body = await api_make_get_request(query_data={}, endpoint=endpoint)

    assert es_status == 200
    assert await redis_client.exists(person['id']) == 1

    await es_delete_record(index='persons', object_id=person['id'])

    cache_status, cache_body = await api_make_get_request(query_data={}, endpoint=endpoint)
    assert cache_status == 200

    assert es_body == cache_body
