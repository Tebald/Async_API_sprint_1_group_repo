import logging
from functools import lru_cache
from typing import Optional

from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.film import Film
from models.genre import Genre
from models.person import Person
from redis.asyncio import Redis

CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class TransferService:
    """
    Class for buisness logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_all_items(self, index: str) -> Optional[list[Film or Genre or Person]]:

        items = await self._get_items_from_elastic(index)
        if not items:
            return None

        return items

    @staticmethod
    def get_model(index: str):
        indexes = {
            'movies': Film,
            'persons': Person,
            'genres': Genre
        }
        if index not in indexes:
            raise ValueError(
                f'Wrong model name received. Expected one from {indexes.items()}, received {index}.'
            )

        data_model = indexes[index]

        return data_model

    async def _get_items_from_elastic(self, index: str) -> Optional[list[Film or Genre or Person]]:
        """
        Retrieves all entries from elastic index.
        It is not recommended to use this method to retrieve large amount of rows.
        Maximum possible rows amount is 10k.
        :param index: 'movies'
        :return: [Film, Film_a, Film_b, ... Film_n]
        """
        result = []
        model = TransferService.get_model(index)
        body = {"query": {"match_all": {}}}
        try:

            response = await self.elastic.search(index=index, body=body, size=1000)
        except NotFoundError:
            return None
        logging.info('Retrieved %s info from elastic: %s', index, response['hits'])

        for item in response['hits']['hits']:
            data = model(**item['_source'])
            logging.debug(data)
            result.append(data)

        return result

    async def get_by_id(self, object_id: str, index: str) -> Optional[Film or Genre or Person]:
        """
        Returns object Film/Person/Genre.
        It is optional since the object can be absent in the elastic/cache.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :param index: 'movies'
        :return: Film
        """
        # Trying to get the data from cache.
        entity = await self._object_from_cache(object_id=object_id, index=index)
        if not entity:
            # If the entity is not in the cache, get it from Elasticsearch.
            entity = await self._get_object_from_elastic(object_id=object_id, index=index)
            if not entity:
                # If the entity is not in the Elasticsearch.
                return None
            # Save the data in cache.
            await self._put_object_to_cache(entity)

        return entity

    async def _get_object_from_elastic(self, object_id: str, index: str) -> Optional[Film or Genre or Person]:
        """
        Returns an object if it exists in elastic.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :param index: 'movies'
        :return: Film
        """
        model = TransferService.get_model(index)
        try:
            doc = await self.elastic.get(index=index, id=object_id)
        except NotFoundError:
            return None
        logging.info('Retrieved object info from elastic: %s', doc['_source'])
        return model(**doc['_source'])

    async def _object_from_cache(self, object_id: str, index: str) -> Optional[Film or Genre or Person]:
        """
        Getting object info from cache using command get https://redis.io/commands/get/.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :param index: 'movies'
        :return: Film
        """

        data = await self.redis.get(object_id)
        if not data:
            return None

        model = TransferService.get_model(index)
        # pydantic предоставляет удобное API для создания объекта моделей из json
        object_data = model.parse_raw(data)
        logging.info('Retrieved object info from cache: %s', object_data)
        return object_data

    async def _put_object_to_cache(self, entity: Film):
        """
        Save object info using set https://redis.io/commands/set/.
        Pydantic allows to serialize model to json.
        :param entity: Film
        :return:
        """

        await self.redis.set(entity.id, entity.json(), CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_transfer_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> TransferService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the servis object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return TransferService(redis, elastic)
