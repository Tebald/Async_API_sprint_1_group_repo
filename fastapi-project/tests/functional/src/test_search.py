import pytest
from tests.functional.conftest import (es_write_data, api_make_get_request)
from tests.functional.testdata.elasticsearch_input import (es_films_search_data,
                                                           es_persons_search_data)
from tests.functional.settings import (movies_test_settings,
                                       persons_test_settings)


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'query': 'The Star', 'page_number': 1, 'page_size': 50},
                {'status': 200, 'length': 50}
        ),
        (
                {'query': 'Mashed potato', 'page_number': 1, 'page_size': 50},
                {'status': 404, 'length': 0}
        ),
        (
                {'query': 'The Star', 'page_number': 0, 'page_size': 50},
                {'status': 422, 'length': 0}
        ),
        (
                {'query': 'The Star', 'page_number': 1, 'page_size': 0},
                {'status': 422, 'length': 0}
        ),
        (
                {'query': 'The Star', 'page_number': 1, 'page_size': 101},
                {'status': 422, 'length': 0}
        ),
(
                {'query': 'The Star', 'page_number': 'a', 'page_size': 50},
                {'status': 422, 'length': 0}
        ),
        (
                {'query': 'The Star', 'page_number': 1, 'page_size': 'b'},
                {'status': 422, 'length': 0}
        ),
        # The following two parameters do not work properly, since
        # 'str' type transformed to 'int' somewhere inside aiohttp.ClientSession().
        # (
        #         {'query': 'The Star', 'page_number': '1', 'page_size': 50},
        #         {'status': 422, 'length': 0}
        # ),
        # (
        #         {'query': 'The Star', 'page_number': 1, 'page_size': '50'},
        #         {'status': 422, 'length': 0}
        # ),
    ]
)
@pytest.mark.asyncio
async def test_films_search(
        es_write_data,
        es_films_search_data,
        api_make_get_request,
        query_data: dict,
        expected_answer: dict):

    es_data = await es_films_search_data()

    await es_write_data(data=es_data, settings=movies_test_settings)

    status, body = await api_make_get_request(query_data=query_data, endpoint='/api/v1/films/search/')

    assert status == expected_answer['status']
    assert len(body.get('items', [])) == expected_answer['length']


@pytest.mark.parametrize(
    'query_data, expected_answer',
    [
        (
                {'query': 'Nash Bridges', 'page_number': 1, 'page_size': 50},
                {'status': 200, 'length': 50}
        ),
        (
                {'query': 'Don Johnson', 'page_number': 1, 'page_size': 50},
                {'status': 404, 'length': 0}
        ),
        (
                {'query': 'Nash Bridges', 'page_number': 0, 'page_size': 50},
                {'status': 422, 'length': 0}
        ),
        (
                {'query': 'Nash Bridges', 'page_number': 1, 'page_size': 0},
                {'status': 422, 'length': 0}
        ),
        (
                {'query': 'Nash Bridges', 'page_number': 1, 'page_size': 101},
                {'status': 422, 'length': 0}
        ),
(
                {'query': 'Nash Bridges', 'page_number': 'a', 'page_size': 50},
                {'status': 422, 'length': 0}
        ),
        (
                {'query': 'Nash Bridges', 'page_number': 1, 'page_size': 'b'},
                {'status': 422, 'length': 0}
        ),
    ]
)
@pytest.mark.asyncio
async def test_persons_search(
        es_write_data,
        es_persons_search_data,
        api_make_get_request,
        query_data: dict,
        expected_answer: dict):

    es_data = await es_persons_search_data()

    await es_write_data(data=es_data, settings=persons_test_settings)

    status, body = await api_make_get_request(query_data=query_data, endpoint='/api/v1/persons/search/')

    assert status == expected_answer['status']
    assert len(body.get('items', [])) == expected_answer['length']
