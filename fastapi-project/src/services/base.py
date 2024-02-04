from functools import lru_cache
from typing import Optional

from fastapi import Depends

from src.models import ElasticModel
from src.schemas import Schema
from src.services.elastic import ElasticService, get_elastic_service
from src.services.redis import RedisService, get_redis_service


class BaseService:
    """
    Class for business logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """

    index: str
    elastic_model: ElasticModel
    redis_model: Schema
    search_field: str
    DEFAULT_SIZE = 100

    def __init__(self, redis_service: RedisService, elastic_service: ElasticService):
        self.redis_service: RedisService = redis_service
        self.elastic_service: ElasticService = elastic_service

    async def get_one(self, object_id: str) -> Optional[ElasticModel]:
        """
        Returns object Film/Person/Genre.
        It is optional since the object can be absent in the elastic/cache.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film | Genre | Person
        """
        entity = await self.redis_service.get(object_id=object_id, model=self.redis_model)
        if not entity:
            entity = await self.elastic_service.get(index=self.index, model=self.elastic_model, object_id=object_id)
            if entity:
                await self.redis_service.put(entity=entity)
        return entity

    async def get_many(self, **kwargs) -> Optional[tuple[list[ElasticModel], int]]:
        record_key = str(kwargs)
        result = await self.redis_service.get_many(record_key, self.redis_model)
        if result:
            return result

        if kwargs.get('page_number'):
            kwargs['from_'] = (kwargs.pop('page_number') - 1) * kwargs.get('size')
        if kwargs.get('search_query'):
            kwargs['body'] = self._build_search_body(kwargs.pop('search_query'))
        if not kwargs.get('size'):
            kwargs['size'] = self.DEFAULT_SIZE

        result = await self.elastic_service.search(index=self.index, model=self.elastic_model, **kwargs)
        if result:
            await self.redis_service.put_many(record_key, result)
        return result

    async def get_by_ids(self, object_ids: list[str]):
        return await self.elastic_service.mget(index=self.index, model=self.elastic_model, object_ids=object_ids)

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
    redis_service: RedisService = Depends(get_redis_service),
    elastic_service: ElasticService = Depends(get_elastic_service),
) -> BaseService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis_service:
    :param elastic_service:
    :return:
    """
    return BaseService(redis_service, elastic_service)
