import logging
from functools import lru_cache
from typing import Optional

from fastapi import Depends
from redis.asyncio import Redis

from src.db._redis import get_redis
from src.models import ElasticModel
from src.schemas import Schema
from src.services.elastic import ElasticService, get_elastic_service


class BaseService:
    """
    Class for business logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """

    index: str
    redis_model: Schema
    search_field: str
    CACHE_EXPIRE_IN_SECONDS: int = 60 * 5
    DEFAULT_SIZE = 100

    def __init__(self, redis: Redis, elastic_service: ElasticService):
        self.redis = redis
        self.elastic_service: ElasticService = elastic_service

    async def get_all(self, **kwargs) -> Optional[list[ElasticModel]]:
        if kwargs.get('page_number'):
            kwargs['from_'] = (kwargs.pop('page_number') - 1) * kwargs.get('size')
        if kwargs.get('search_query'):
            kwargs['body'] = self._build_search_body(kwargs.pop('search_query'))
        if not kwargs.get('size'):
            kwargs['size'] = self.DEFAULT_SIZE
        return await self.elastic_service.search(index=self.index, **kwargs)

    async def get_one(self, object_id: str) -> Optional[ElasticModel]:
        """
        Returns object Film/Person/Genre.
        It is optional since the object can be absent in the elastic/cache.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film | Genre | Person
        """
        entity = await self._object_from_cache(object_id=object_id)
        if not entity:
            entity = await self.elastic_service.get(index=self.index, object_id=object_id)
            if entity:
                await self._put_object_to_cache(entity)
        return entity

    async def get_by_ids(self, object_ids: list[str]):
        return await self.elastic_service.mget(index=self.index, object_ids=object_ids)

    async def _object_from_cache(self, object_id: str) -> Optional[ElasticModel]:
        """
        Getting object info from cache using command get https://redis.io/commands/get/.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film | Genre | Person
        """
        data = await self.redis.get(object_id)
        if not data:
            return None
        object_data = self.redis_model.parse_raw(data)
        logging.info('Retrieved object info from cache: %s', object_data)
        return object_data

    async def _put_object_to_cache(self, entity: ElasticModel):
        """
        Save object info using set https://redis.io/commands/set/.
        Pydantic allows to serialize model to json.
        :param entity: Film | Genre | Person
        :return:
        """
        try:
            logging.info('Saving object into cache: %s', entity.uuid)
            await self.redis.set(entity.uuid, entity.json(), self.CACHE_EXPIRE_IN_SECONDS)
        except TypeError:
            logging.error('Cannot cache object: %s to cache. Cannot convert uuid to string.', entity.__class__)
        except AttributeError:
            logging.error("Cannot cache object: %s. No attribute 'uuid'.", entity.__class__)

    def _build_search_body(self, search_query: str) -> dict:
        return {
            'query': {
                'multi_match': {
                    'query': search_query,
                    'fields': [self.search_field],
                    'type': 'best_fields',
                    'fuzziness': 'auto',
                }
            },
            'sort': ['_score'],
        }


@lru_cache()
def get_transfer_service(
    redis: Redis = Depends(get_redis),
    elastic_service: ElasticService = Depends(get_elastic_service),
) -> BaseService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return BaseService(redis, elastic_service)
