import logging
from functools import lru_cache
from typing import Callable, List, Optional, Union

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db._redis import get_redis
from db.elastic import get_elastic
from models.film import Film
from models.genre import Genre
from models.person import Person


class BaseService:
    """
    Class for buisness logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    index: str
    elastic_model: Callable[..., Union[Film, Genre, Person]]
    redis_model: Callable[..., Union[Film, Genre, Person]]
    search_field: str
    CACHE_EXPIRE_IN_SECONDS: int = 60 * 5
    DEFAULT_SIZE = 100

    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_all_items(self, **kwargs) -> Optional[List]:
        return await self._get_items_from_elastic(**kwargs)

    async def search_items(self, search_query: str) -> Optional[List]:
        body = self._build_search_body(search_query)
        return await self._execute_elastic_search(body)

    async def _get_items_from_elastic(self, **kwargs) -> Optional[List]:
        body = {"query": {"match_all": {}}}
        return await self._execute_elastic_search(body)

    def _build_search_body(self, search_query: str) -> dict:
        return {
            "query": {
                "multi_match": {
                    "query": search_query,
                    "fields": [self.search_field],
                    "type": "best_fields",
                    "fuzziness": "auto"
                }
            },
            "sort": ["_score"]
        }

    async def _execute_elastic_search(self, body: dict) -> Optional[List]:
        try:
            response = await self.elastic.search(index=self.index, body=body, size=self.DEFAULT_SIZE)
            return [self.elastic_model(**item['_source']) for item in response['hits']['hits']]
        except NotFoundError:
            return None

    async def get_by_id(self, object_id: str) -> Optional[Film or Genre or Person]:
        """
        Returns object Film/Person/Genre.
        It is optional since the object can be absent in the elastic/cache.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film
        """
        entity = await self._object_from_cache(object_id=object_id)
        if not entity:
            entity = await self._get_object_from_elastic(object_id=object_id)
            if entity:
                await self._put_object_to_cache(entity)
        return entity

    async def _get_object_from_elastic(self, object_id: str) -> Optional[Film or Genre or Person]:
        """
        Returns an object if it exists in elastic.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film
        """
        try:
            doc = await self.elastic.get(index=self.index, id=object_id)
            logging.info('Retrieved object info from elastic: %s', doc['_source'])
            return self.elastic_model(**doc['_source'])
        except NotFoundError:
            return None

    async def _object_from_cache(self, object_id: str) -> Optional[Film or Genre or Person]:
        """
        Getting object info from cache using command get https://redis.io/commands/get/.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film
        """
        data = await self.redis.get(object_id)
        if not data:
            return None
        object_data = self.redis_model.parse_raw(data)
        logging.info('Retrieved object info from cache: %s', object_data)
        return object_data

    async def _put_object_to_cache(self, entity: Union[Film, Genre, Person]):
        """
        Save object info using set https://redis.io/commands/set/.
        Pydantic allows to serialize model to json.
        :param entity: Film
        :return:
        """

        await self.redis.set(entity.uuid, entity.json(), self.CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_transfer_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> BaseService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the servis object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return BaseService(redis, elastic)
