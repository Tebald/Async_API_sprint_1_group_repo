import logging
from functools import lru_cache
from typing import Optional, List

from db.elastic import get_elastic
from db._redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.film import Film
from models.genre import Genre
from models.person import Person
from redis.asyncio import Redis
from services.transfer import TransferService


class GenresService(TransferService):
    """
    Class for buisness logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    index = 'genres'
    model = Genre

    async def get_all_items(self) -> Optional[List[Film]]:
        items = await self._get_items_from_elastic()
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

    async def _get_items_from_elastic(self) -> Optional[list[Film or Genre or Person]]:
        """
        Retrieves all entries from elastic index.
        It is not recommended to use this method to retrieve large amount of rows.
        Maximum possible rows amount is 10k.
        :param index: 'movies'
        :return: [Film, Film_a, Film_b, ... Film_n]
        """
        sort_order = 'desc' if sort.startswith('-') else 'asc'
        sort_field = sort.lstrip('-')

        body = {
            "query": {
                "match_all": {}
            } if not genre else {
                "match": {
                    "genre": genre
                }
            },
            "sort": [
                {sort_field: {"order": sort_order}}
            ]
        }

        result = []
        scroll = '1m'
        try:
            response = await self.elastic.search(index=self.index, body=body, scroll=scroll,
                                                 size=100)
            while response['hits']['hits']:
                for item in response['hits']['hits']:
                    data = self.model(**item['_source'])
                    result.append(data)
                response = await self.elastic.scroll(scroll_id=response['_scroll_id'], scroll=scroll)
        except NotFoundError:
            return None
        if '_scroll_id' in response:
            await self.elastic.clear_scroll(scroll_id=response['_scroll_id'])

        return result

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

        await self.redis.set(entity.id, entity.json(), self.CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_genres_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenresService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the servis object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return GenresService(redis, elastic)
