import pytest

from tests.functional import movies_test_settings
from tests.functional.conftest import es_write_data, api_make_get_request, es_delete_data
from tests.functional.testdata.elasticsearch_input import es_films_search_data, es_single_film


@pytest.mark.asyncio
async def test_no_films_data(api_make_get_request, es_delete_data):
    await es_delete_data(movies_test_settings)
    status, body = await api_make_get_request({}, '/api/v1/films/')
    assert status == 404


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
    ({}, 200, 50),
    ({'genre': '812e88bd-7db1-4827-967e-53c946a602b3'}, 200, 30),
    ({'genre': '812e88bd-7db1-4827-967e-53c946a60222'}, 404, 0)
])
@pytest.mark.asyncio
async def test_list_films(
        es_write_data,
        es_films_search_data,
        api_make_get_request,
        query,
        expected_status,
        films_count
):

    data = await es_films_search_data()
    await es_write_data(data=data, settings=movies_test_settings)

    status, body = await api_make_get_request(query, '/api/v1/films/')
    assert status == expected_status
    assert len(body.get('items', [])) == films_count


@pytest.mark.asyncio
async def test_list_films_genre_filtering(
        es_write_data,
        es_films_search_data,
        api_make_get_request,
):

    data = await es_films_search_data()
    await es_write_data(data=data, settings=movies_test_settings)

    genre_id, genre_name = data[0]['_source']['genre'][0]['id'], data[0]['_source']['genre'][0]['name']
    query = {'genre': genre_id}

    status, body = await api_make_get_request(query, '/api/v1/films/')
    assert all([True for film in body.get('items', {}) if genre_name in film.get('title')])
