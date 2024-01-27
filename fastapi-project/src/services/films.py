import logging
from functools import lru_cache
from typing import List, Optional, Tuple

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db._redis import get_redis
from db.elastic import get_elastic
from models import Film
from schemas.films import FilmSchema
from services.base import BaseService


class FilmsService(BaseService):
    """
    Class for buisness logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    index = 'movies'
    elastic_model = Film
    redis_model = FilmSchema
    search_field = "title"
    fuzziness = "0"

    async def get_all_items(
            self,
            sort: str,
            page_size: int,
            page_number: int,
            genre: Optional[str] = None) -> Optional[Tuple[List[Film], int]]:

        items = await self._get_items_from_elastic(sort, page_size, page_number, genre)
        if not items:
            return None

        return items

    async def get_items_by_ids(self, ids: List[str]) -> Optional[List[Film]]:
        result = []
        try:
            response = await self.elastic.mget(index=self.index, body={'ids': ids})
        except NotFoundError:
            return None

        for item in response['docs']:
            data = self.elastic_model(**item['_source'])
            logging.debug(data)
            result.append(data)

        return result

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
            "query": {
                "match_all": {}
            } if not genre else {
                "nested": {
                    "path": "genre",
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"genre.id": genre}}
                            ]
                        }
                    }
                }
            },
            "sort": [
                {sort_field: {"order": sort_order}}
            ]
        }

        result = []
        try:
            response = await self.elastic.search(index=self.index, body=body, size=page_size, from_=offset)
            total = response['hits']['total']['value']
            for item in response['hits']['hits']:
                data = self.elastic_model(**item['_source'])
                result.append(data)
        except NotFoundError:
            return None

        return result, total


@lru_cache()
def get_films_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmsService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the servis object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return FilmsService(redis, elastic)
