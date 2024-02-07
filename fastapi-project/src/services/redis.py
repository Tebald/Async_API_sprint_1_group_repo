import logging
from functools import lru_cache
from typing import Optional

from fastapi import Depends
from orjson import orjson
from pydantic.json import pydantic_encoder
from redis.asyncio import Redis

from src.db._redis import get_redis
from src.schemas import Schema


class RedisService:
    """
    A class to combine all the Redis operations in the one place.
    Stores functions to get/put data in and out of cache.
    """
    CACHE_EXPIRE_IN_SECONDS: int = 60 * 5

    def __init__(self, redis: Redis):
        self.redis = redis

    async def get(self, object_id: str, model: type[Schema]) -> Optional[Schema]:
        """
        Get object from cache using command get https://redis.io/commands/get/.
        :param model: Model to parse.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: FilmSchema | GenreSchema | PersonSchema
        """
        data = await self.redis.get(object_id)
        if not data:
            return None

        object_data = model.parse_raw(data)
        logging.info('Retrieved object from cache: %s', object_id)
        return object_data

    async def get_many(self, record_key: str, model: Schema) -> Optional[list[Schema]]:
        """
        Get multiple objects from cache using command get https://redis.io/commands/get/.
        :param record_key: A string record key, containing the record.
        :param model: A model, used for parsing objects.
        :return: A list of Schema.
        """
        data = await self.redis.get(record_key)
        if not data:
            return None

        result = [model(**entity) for entity in orjson.loads(data)]
        logging.info('Retrieved objects from cache: key=%s', record_key)
        return result

    async def put(self, entity: Schema) -> None:
        """
        Save object info using set https://redis.io/commands/set/.
        Pydantic allows to serialize model to json.
        :param entity: FilmSchema | GenreSchema | PersonSchema
        :return:
        """
        try:
            await self.redis.set(entity.uuid, entity.json(), self.CACHE_EXPIRE_IN_SECONDS)
            logging.info('Saved object into cache: id=%s', entity.uuid)
        except TypeError:
            logging.error('Cannot cache object: %s to cache. Cannot convert uuid to string.', entity.__class__)
        except AttributeError:
            logging.error("Cannot cache object: %s. No attribute 'uuid'.", entity.__class__)

    async def put_many(self, record_key: str, entities: list[Schema]) -> None:
        """
        Save multiple objects to cache using set https://redis.io/commands/set/.
        :param record_key: A string key to store record.
        :param entities: A list of objects to store.
        :return: -
        """
        value_str = orjson.dumps(entities, default=pydantic_encoder)
        await self.redis.set(record_key, value_str, self.CACHE_EXPIRE_IN_SECONDS)
        logging.info('Saved object into cache: key=%s', record_key)


@lru_cache()
def get_redis_service(
    redis: Redis = Depends(get_redis),
) -> RedisService:
    """Provider of RedisService.

    :param redis: An async Redis exemplar which represents async connection to Redis.
    :return: A Singleton of RedisService.
    """
    return RedisService(redis)
