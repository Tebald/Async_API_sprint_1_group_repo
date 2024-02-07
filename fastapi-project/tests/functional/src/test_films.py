import uuid

import pytest

from tests.functional import movies_test_settings
from tests.functional.conftest import es_write_data, api_make_get_request
from tests.functional.testdata.elasticsearch_input import es_films_search_data


@pytest.mark.parametrize("query_data,expected_status", [
    ({"page_number": 1, "page_size": 50}, 200),
    ({"page_number": 0, "page_size": 50}, 422),
    ({"page_number": 1, "page_size": 0}, 422),
    ({"page_number": 1, "page_size": 50, "sort": "rating"}, 422)
])
@pytest.mark.asyncio
async def test_list_films_invalidation(
        es_write_data,
        es_films_search_data,
        api_make_get_request,
        query_data,
        expected_status):

    data = await es_films_search_data()
    await es_write_data(data=data, settings=movies_test_settings)

    status, _ = await api_make_get_request(query_data, '/api/v1/films/')
    assert status == expected_status


@pytest.mark.asyncio
async def test_list_films_no_filter(
        es_write_data,
        es_films_search_data,
        api_make_get_request):

    data = await es_films_search_data()
    await es_write_data(data=data, settings=movies_test_settings)

    query_data = {
        "page_size": 50
    }

    status, body = await api_make_get_request(query_data, '/api/v1/films/')
    assert len(body.get('items', [])) == query_data['page_size']


@pytest.mark.asyncio
async def test_film_details_valid_uuid(
        es_write_data,
        es_films_search_data,
        api_make_get_request):

    data = await es_films_search_data()
    await es_write_data(data=data, settings=movies_test_settings)

    valid_uuid = data[0]['_id']
    # TODO: поменять uuid на film_id
    status, body = await api_make_get_request({"uuid": valid_uuid}, '/api/v1/films/{film_id}')
    assert status == 200
    assert body['uuid'] == valid_uuid

@pytest.mark.asyncio
async def test_film_details_invalid_uuid(api_make_get_request):
    invalid_uuid = str(uuid.uuid4())
    status, _ = await api_make_get_request({"uuid": invalid_uuid}, '/api/v1/films/{film_id}')
    assert status == 404
