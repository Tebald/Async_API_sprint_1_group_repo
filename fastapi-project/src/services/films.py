import json
import logging
from functools import lru_cache
from typing import List, Optional, Tuple, Union

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from src.db._redis import get_redis
from src.db.elastic import get_elastic
from src.models import Film
from src.schemas.films import FilmSchema
from src.services.base import BaseService


class FilmsService(BaseService):
    """
    Class for business logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    index = 'movies'
    elastic_model = Film
    redis_model = FilmSchema
    search_field = 'title'

    async def get_all_items(
            self,
            sort: str,
            page_size: int,
            page_number: int,
            genre: Optional[str] = None) -> Optional[Tuple[List[Union[Film, FilmSchema]], int]]:
        cache_key = f'films_page_{page_number}_size_{page_size}_sort_{sort}_genre_{genre}'
        cached_data = await self.redis.get(cache_key)
        if cached_data:
            logging.info('Retrieved object from cache - %s', cache_key)

            items = [self.redis_model.parse_raw(item) for item in json.loads(cached_data)]
            return items, len(items)

        items, total = await self._get_items_from_elastic(sort, page_size, page_number, genre)
        logging.info('Retrieved object from elastic - %s', cache_key)

        if items:
            await self.redis.set(
                cache_key, json.dumps([item.json() for item in items]),
                ex=self.CACHE_EXPIRE_IN_SECONDS
            )
            logging.info('Saved object in cache - %s', cache_key)

        return items, total

    async def get_items_by_ids(self, ids: List[str]) -> Optional[List[Film]]:
        try:
            response = await self.elastic.mget(index=self.index, body={'ids': ids})
            return [self.elastic_model(**item['_source']) for item in response['docs']]
        except NotFoundError:
            return None

    async def _get_items_from_elastic(
            self,
            sort: str,
            page_size: int,
            page_number: int,
            genre: Optional[str] = None) -> Optional[Tuple[List[Film], int]]:
        """
        Retrieves all entries from elastic index.
        It is not recommended to use this method to retrieve large amount of rows.
        Maximum possible rows amount is 10k.
        :return: [Film, Film_a, Film_b, ... Film_n]
        """
        sort_order = 'desc' if sort.startswith('-') else 'asc'
        sort_field = sort.lstrip('-')
        offset = (page_number - 1) * page_size

        body = {
            'query': {'match_all': {}} if not genre else {
                'nested': {
                    'path': 'genre',
                    'query': {'bool': {'must': [{'match': {'genre.id': genre}}]}}
                }
            },
            'sort': [{sort_field: {'order': sort_order}}]
        }

        try:
            response = await self.elastic.search(index=self.index, body=body, size=page_size, from_=offset)
            total = response['hits']['total']['value']
            return [self.elastic_model(**item['_source']) for item in response['hits']['hits']], total
        except NotFoundError:
            return None


@lru_cache()
def get_films_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmsService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return FilmsService(redis, elastic)
