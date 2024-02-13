import asyncio

import backoff
from elasticsearch import AsyncElasticsearch
from redis.asyncio import Redis

from tests.functional.settings import test_base_settings


@backoff.on_exception(backoff.expo, Exception, max_time=300, max_tries=10)
async def wait_for_redis() -> None:
    print("Pinging Redis...")
    client = Redis(host=test_base_settings.redis_host, port=test_base_settings.redis_port)
    try:
        await client.ping()
        print("Redis is up!")
    finally:
        await client.close()


@backoff.on_exception(backoff.expo, Exception, max_time=300, max_tries=10)
async def wait_for_elasticsearch() -> None:
    print("Pinging Elastic...")
    client = AsyncElasticsearch([{'host': test_base_settings.es_host, 'port': test_base_settings.es_port}])
    try:
        await client.indices.exists(index='ping')
        print("Elasticsearch is up!")
    finally:
        await client.close()


async def main():
    await asyncio.gather(
        wait_for_redis(),
        wait_for_elasticsearch(),
    )

if __name__ == '__main__':
    print("Waiting for Redis to start...")
    print("Waiting for Elastic to start...")
    asyncio.run(main())
