from functools import lru_cache
from typing import Optional

from fastapi import Depends
from src.models import ElasticModel
from src.schemas import Schema
from src.services._redis import RedisService, get_redis_service
from src.services.elastic import ElasticService, get_elastic_service
from src.utils.kwargs_transformer.transformer import KwargsTransformer, get_kwargs_transformer


class BaseService:
    """
    Class for business logic to operate with film/person/genre entities.
    It contains only high-level functions to take data from elastic or redis and
    send it to api modules.
    """

    index: str
    elastic_model: ElasticModel
    redis_model: Schema
    DEFAULT_SIZE = 100

    def __init__(
            self, redis_service: RedisService, elastic_service: ElasticService, kwargs_transformer: KwargsTransformer
    ):
        self.redis_service: RedisService = redis_service
        self.elastic_service: ElasticService = elastic_service
        self.kwargs_transformer: KwargsTransformer = kwargs_transformer

    async def get_by_id(self, object_id: str) -> Optional[ElasticModel]:
        """
        Returns object Film/Person/Genre by its ID.

        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film | Genre | Person
        """

        entity = await self.redis_service.get(object_id=object_id, model=self.redis_model)
        if entity:
            return entity
        entity = await self.elastic_service.get(index=self.index, model=self.elastic_model, object_id=object_id)
        if entity:
            await self.redis_service.put(entity=entity)
        return entity

    async def get_many(self, **kwargs) -> Optional[tuple[list[ElasticModel], int]]:
        """
        Returns list of objects suitable to kwargs params, and total count of objects by query.

        Available kwargs (but not required):
            'page_number': int. Used with 'size' by PaginationHandler to build pagination kwargs.
            'sort': string. Used by SortHandler to fill 'sort' list of constraints.
            'filters': list of dicts like {'field': str, 'value': str, 'type': Optional[str]}.
                Used by FiltersHandler to fill 'must' and 'should' lists of constraints.
            'search': dict like {'field': str, 'value': str, 'fuzziness': Optional[str]}.
                Used by SearchHandler to append 'must' list by search constraint
                and to append 'sort' list by {'_score': 'desc'} constraint.
        More information about a specific parameter you can find in the related class.

        Any other params will be left without changes and unpacked to elastic.search().
        """

        if not kwargs.get('size'):
            kwargs['size'] = self.DEFAULT_SIZE
        record_key = self.__class__.__name__ + str(kwargs)
        kwargs = self.kwargs_transformer.transform(kwargs)

        result = await self.redis_service.get_many(record_key, self.redis_model)
        if not result:
            result = await self.elastic_service.search(self.index, self.elastic_model, **kwargs)
            if result:
                await self.redis_service.put_many(record_key, result)

        total_records = await self.elastic_service.count(self.index, query=kwargs['body']['query'])
        return result, total_records

    async def get_by_ids(self, object_ids: list[str]) -> Optional[list[ElasticModel]]:
        """
        Gets objects from Elasticsearch by its ids.
        No cache.
        Object_ids mustn't be empty, but function still can return None.
            Because all the objects from list may not exist in Elasticsearch.

        :param object_ids: list of ids. Not empty.
        :return: list[ElasticModel] | None.
        """
        return await self.elastic_service.mget(self.index, self.elastic_model, object_ids)


@lru_cache()
def get_base_service(
    redis_service: RedisService = Depends(get_redis_service),
    elastic_service: ElasticService = Depends(get_elastic_service),
        kwargs_transformer: KwargsTransformer = Depends(get_kwargs_transformer),
) -> BaseService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis_service:
    :param elastic_service:
    :param kwargs_transformer:
    :return:
    """
    return BaseService(redis_service, elastic_service, kwargs_transformer)
