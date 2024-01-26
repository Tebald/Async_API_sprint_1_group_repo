from functools import lru_cache
from typing import List, Optional

from db._redis import get_redis
from db.elastic import get_elastic
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.film import Film
from models.genre import Genre
from models.person import Person
from redis.asyncio import Redis
from services.transfer import TransferService


class FilmsService(TransferService):
    """
    Class for buisness logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    index = 'movies'
    model = Film

    async def get_all_items(
            self,
            sort: str,
            genre: Optional[str] = None
    ) -> Optional[List[Film]]:
        items = await self._get_items_from_elastic(sort, genre)
        if not items:
            return None

        return items

    async def search_films(self, film_title: str) -> Optional[List[Film]]:
        result = []
        body = {
            "query": {
                "multi_match": {
                    "query": film_title,
                    "fields": ["title"],
                    "type": "best_fields",
                    "fuzziness": "0"
                }
            },
            "sort": [
                "_score"

            ]
        }

        try:
            response = await self.elastic.search(index=self.index, body=body)
            for item in response['hits']['hits']:
                data = self.model(**item['_source'])
                result.append(data)
        except NotFoundError:
            return None

        return result

    async def _get_items_from_elastic(
            self,
            sort: str,
            genre: Optional[str] = None
    ) -> Optional[list[Film or Genre or Person]]:
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
