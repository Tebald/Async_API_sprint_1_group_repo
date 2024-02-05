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
    It contains only high-level functions to take data from elastic or redis and
    send it to api modules.
    """

    index: str
    index_total_records: int
    elastic_model: ElasticModel
    redis_model: Schema
    DEFAULT_SIZE = 100

    def __init__(self, redis_service: RedisService, elastic_service: ElasticService):
        self.redis_service: RedisService = redis_service
        self.elastic_service: ElasticService = elastic_service

    async def get_one(self, object_id: str) -> Optional[ElasticModel]:
        """
        Returns object Film/Person/Genre by its ID.

        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film | Genre | Person
        """

        entity = await self.redis_service.get(object_id=object_id, model=self.redis_model)
        if not entity:
            entity = await self.elastic_service.get(index=self.index, model=self.elastic_model, object_id=object_id)
            if entity:
                await self.redis_service.put(entity=entity)
        return entity

    async def get_many(self, **kwargs) -> Optional[list[ElasticModel]]:
        """
        Returns list of objects suitable to kwargs params.

        Also sets self.index_total_records param.
        It represents count of records in current index and being used for pagination.

        Available all the kwargs from official documentation:
        https://elasticsearch-py.readthedocs.io/en/7.9.1/

        Additionally added:
            page_number - An integer number of page to start pagination. Uses with `size`. >= 1.
            size - A count of the record per page/answer. If empty, uses size=self.DEFAULT_SIZE.
            search_field - A string name of field to run fizzy search. Always used with search_query.
            search_query - A string value of field for fizzy search. Always used with search_field.

        :param kwargs:
        :return: [Film1, Film2, Film3]
        """
        record_key = str(kwargs)
        kwargs = self._process_kwargs(kwargs)

        result = await self.redis_service.get_many(record_key, self.redis_model)
        if not result:
            result = await self.elastic_service.search(self.index, self.elastic_model, **kwargs)
            if result:
                await self.redis_service.put_many(record_key, result)

        self.index_total_records = await self.elastic_service.count(self.index)
        return result

    async def get_by_ids(self, object_ids: list[str]) -> Optional[list[ElasticModel]]:
        """
        Gets objects from Elasticsearch by its ids.
        No cache.
        :param object_ids: list of ids.
        :return: list[ElasticModel] | None.
        """
        return await self.elastic_service.mget(self.index, self.elastic_model, object_ids)

    def _process_kwargs(self, kwargs: dict):
        """Handles available kwargs for get_many().

        More information in get_many().
        """
        if kwargs.get('page_number'):
            kwargs['from_'] = (kwargs.pop('page_number') - 1) * kwargs.get('size')
        if kwargs.get('search_query'):
            kwargs['body'] = self._build_search_body(kwargs.pop('search_field'), kwargs.pop('search_query'))
        if not kwargs.get('size'):
            kwargs['size'] = self.DEFAULT_SIZE
        return kwargs

    def _build_search_body(self, search_field: str, search_query: str) -> dict:
        """Fixture for ES fuzzy search query."""
        return {
            'query': {
                'multi_match': {
                    'query': search_query,
                    'fields': [search_field],
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
