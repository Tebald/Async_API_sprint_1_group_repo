"""
Group of tests for checking /api/v1/films/ endpoint.
"""
from http import HTTPStatus

import pytest
from tests.functional import movies_test_settings
from tests.functional.testdata.elasticsearch_input import es_films_search_data, es_single_film  # noqa: F401


@pytest.mark.asyncio
async def test_no_films_data(api_make_get_request, es_delete_data):
    await es_delete_data(movies_test_settings)
    status, body = await api_make_get_request({}, '/api/v1/films/')
    assert status == HTTPStatus.NOT_FOUND


@pytest.mark.parametrize('query_data, expected_status', [
    ({"page_number": 1, "page_size": 50}, HTTPStatus.OK),
    ({"page_number": 0, "page_size": 50}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({"page_number": 1, "page_size": 0}, HTTPStatus.UNPROCESSABLE_ENTITY),
    ({"page_number": 1, "page_size": 50, "sort": "rating"}, HTTPStatus.UNPROCESSABLE_ENTITY)
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


@pytest.mark.parametrize('film_id, expected_status', [
    ('95b7ddb4-1f59-4a2f-982d-65d733934b53', HTTPStatus.OK),
    ('95b7ddb4-1f59-4a2f-982d-65d733934b22', HTTPStatus.NOT_FOUND),
    ('00000000-0000-0000-0000-000000000000', HTTPStatus.UNPROCESSABLE_ENTITY)
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
    endpoint = f'/api/v1/films/{film_id}'

    status, body = await api_make_get_request(query_data={}, endpoint=endpoint)

    assert status == expected_status


@pytest.mark.parametrize('query, expected_status, films_count', [
    ({}, HTTPStatus.OK, 50),
    ({'genre': '812e88bd-7db1-4827-967e-53c946a602b3'}, HTTPStatus.OK, 30),
    ({'genre': '812e88bd-7db1-4827-967e-53c946a60222'}, HTTPStatus.NOT_FOUND, 0)
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

    assert all([True for film in body.get('items', {}) if genre_name in film.get('title')]) == 1


@pytest.mark.parametrize("endpoint, query, cache_key, prepare_data_func_name", [
    (
            '/api/v1/films',
            {},
            "FilmsService{'sort': '-imdb_rating', 'filters': [None], 'page_number': 1, 'size': 50}",
            'es_films_search_data'
    ),

    (
            '/api/v1/films/95b7ddb4-1f59-4a2f-982d-65d733934b53',
            {},
            '95b7ddb4-1f59-4a2f-982d-65d733934b53',
            'es_single_film'
    )
])
@pytest.mark.asyncio
async def test_films_cache(
        redis_client,
        es_delete_data,
        es_write_data,
        api_make_get_request,
        prepare_films_data_factory,
        prepare_data_func_name,
        cache_key,
        endpoint,
        query
):
    prepare_data_func = prepare_films_data_factory(prepare_data_func_name)
    data = await prepare_data_func
    await es_write_data(data=data, settings=movies_test_settings)

    from_es_status, from_es_body = await api_make_get_request(query, endpoint)

    assert from_es_status == HTTPStatus.OK
    assert await redis_client.exists(cache_key) == 1

    await es_delete_data(movies_test_settings)

    from_cache_status, from_cache_body = await api_make_get_request(query, endpoint)

    assert from_cache_status == HTTPStatus.OK
    if endpoint == '/api/v1/films':
        assert from_es_body.get('items') == from_cache_body.get('items')
    else:
        assert from_es_body == from_cache_body
