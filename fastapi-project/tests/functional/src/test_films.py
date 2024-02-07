import uuid

import pytest

from tests.functional import movies_test_settings
from tests.functional.conftest import es_write_data, api_make_get_request
from tests.functional.testdata.elasticsearch_input import es_films_search_data, es_single_film


@pytest.mark.parametrize("query_data,expected_status", [
    ({"page_number": 1, "page_size": 50}, 200),
    ({"page_number": 0, "page_size": 50}, 422),
    ({"page_number": 1, "page_size": 0}, 422),
    ({"page_number": 1, "page_size": 50, "sort": "rating"}, 422)
])
@pytest.mark.asyncio
async def test_list_films_validation(
        es_write_data,
        es_films_search_data,
        api_make_get_request,
        query_data,
        expected_status):

    data = await es_films_search_data()
    await es_write_data(data=data, settings=movies_test_settings)

    status, _ = await api_make_get_request(query_data, '/api/v1/films/')
    assert status == expected_status


@pytest.mark.parametrize("film_id,expected_status", [
    ('95b7ddb4-1f59-4a2f-982d-65d733934b53', 200),
    ('95b7ddb4-1f59-4a2f-982d-65d733934b22', 404),
    ('00000000-0000-0000-0000-000000000000', 422)
])
@pytest.mark.asyncio
async def test_film_detail(
        es_write_data,
        es_single_film,
        api_make_get_request,
        film_id,
        expected_status
):

    data = await es_single_film()
    await es_write_data(data=data, settings=movies_test_settings)

    # TODO: поменять uuid на film_id
    status, body = await api_make_get_request({"uuid": film_id}, '/api/v1/films/{film_id}')
    assert status == expected_status


@pytest.mark.parametrize("query,expected_status,films_count", [
    ({}, 200, 60),
    ({'genre': '812e88bd-7db1-4827-967e-53c946a602b3'}, 200, 30),
    ({'genre': '812e88bd-7db1-4827-967e-53c946a602b3'}, 422)
])
@pytest.mark.asyncio
async def test_list_films(
        es_write_data,
        es_films_search_data,
        api_make_get_request):

    data = await es_films_search_data()
    await es_write_data(data=data, settings=movies_test_settings)

    query_data = {
        "page_size": 50
    }

    status, body = await api_make_get_request(query_data, '/api/v1/films/')
    assert status == 200
    assert len(body.get('items', [])) == query_data['page_size']
    assert body.get('total', 0) == len(data)
