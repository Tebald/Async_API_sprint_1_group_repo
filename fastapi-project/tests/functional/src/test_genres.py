import pytest

from tests.functional import genres_test_settings
from tests.functional.testdata.elasticsearch_input import es_list_genres, es_single_genre


@pytest.mark.asyncio
async def test_no_genres_data(api_make_get_request, es_delete_data):
    await es_delete_data(genres_test_settings)
    status, body = await api_make_get_request({}, '/api/v1/genres/')
    assert status == 404


@pytest.mark.asyncio
async def test_list_genres(
        es_write_data,
        es_list_genres,
        api_make_get_request):

    data = await es_list_genres()
    await es_write_data(data=data, settings=genres_test_settings)

    status, body = await api_make_get_request({}, '/api/v1/genres/')
    assert status == 200
    assert len(body) == len(data)

#
@pytest.mark.parametrize("genre_id,expected_status", [
    ('f2998290-8ea4-48ae-a3a0-1ea43becfa9b', 200),
    ('95b7ddb4-1f59-4a2f-982d-65d733934b22', 404),
    ('00000000-0000-0000-0000-000000000000', 422)
])
@pytest.mark.asyncio
async def test_genre_detail(
        es_write_data,
        es_single_genre,
        api_make_get_request,
        genre_id,
        expected_status
):

    data = await es_single_genre()
    await es_write_data(data=data, settings=genres_test_settings)

    status, body = await api_make_get_request({"uuid": genre_id}, '/api/v1/genres/{genre_id}')
    assert status == expected_status


@pytest.mark.parametrize("endpoint,query,cache_key,prepare_data_func_name,", [
    ('/api/v1/genres', {}, '{}', 'es_list_genres'),

    ('/api/v1/genres/{genre_id}', {'uuid': 'f2998290-8ea4-48ae-a3a0-1ea43becfa9b'},
     'f2998290-8ea4-48ae-a3a0-1ea43becfa9b', 'es_single_genre')
])
@pytest.mark.asyncio
async def test_genre_cache(
        redis_client,
        es_delete_data,
        es_write_data,
        api_make_get_request,
        prepare_genres_data_factory,
        prepare_data_func_name,
        cache_key,
        endpoint,
        query
):
    prepare_data_func = prepare_genres_data_factory(prepare_data_func_name)
    data = await prepare_data_func
    await es_write_data(data=data, settings=genres_test_settings)

    from_es_status, from_es_body = await api_make_get_request(query, endpoint)
    assert from_es_status == 200

    assert await redis_client.exists(cache_key) == 1

    await es_delete_data(genres_test_settings)

    from_cache_status, from_cache_body = await api_make_get_request(query, endpoint)
    assert from_cache_status == 200

    assert from_es_body == from_cache_body
