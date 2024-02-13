import pytest_asyncio
from redis.asyncio import Redis
import backoff

from tests.functional.settings import test_base_settings


@pytest_asyncio.fixture(name='redis_client', scope='session')
async def redis_client() -> Redis:
    @backoff.on_exception(backoff.expo, Exception, max_time=30, jitter=backoff.random_jitter)
    async def get_redis_client():
        return Redis(host=test_base_settings.redis_host, port=test_base_settings.redis_port, decode_responses=True)

    redis_client = await get_redis_client()
    yield redis_client
    await redis_client.close()


@pytest_asyncio.fixture(name='redis_clear', autouse=True)
@backoff.on_exception(backoff.expo, Exception, max_time=30, jitter=backoff.random_jitter)
async def redis_clear(redis_client):
    await redis_client.flushdb(asynchronous=True)