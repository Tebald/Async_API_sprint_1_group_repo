import pytest_asyncio
import aiohttp
import backoff

from tests.functional.settings import test_base_settings


@pytest_asyncio.fixture(name='client_session', scope='session')
async def client_session() -> aiohttp.ClientSession:
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest_asyncio.fixture(name='api_make_get_request')
def api_make_get_request(client_session):
    @backoff.on_exception(backoff.expo, Exception, max_time=30, jitter=backoff.random_jitter)
    async def inner(query_data: dict, endpoint: str):
        """
        :param query_data: {'query': 'The Star', 'page_number': 1, 'page_size': 50}
        :param endpoint: '/api/v1/films/search/'
        :return:
        """
        url = f'http://{test_base_settings.service_host}:{test_base_settings.service_port}'
        url += endpoint
        async with client_session.get(url, params=query_data) as response:
            body = await response.json()
            status = response.status
        return status, body

    return inner