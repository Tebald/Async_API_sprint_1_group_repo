import logging
from functools import lru_cache
from typing import List, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db._redis import get_redis
from db.elastic import get_elastic
from models.person import Person
from services.transfer import BaseService


class PersonsService(BaseService):
    """
    Class for buisness logic to operate with person entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    index = 'persons'
    model = Person

    async def get_all_items(
            self,
            index: str,
            filter_body: dict = None) -> Optional[List]:
        """

        :param index:
        :param filter_body:
        :return:
        """

        items = await self._get_items_from_elastic(index, filter_body)
        if not items:
            return None

        return items

    async def search_persons(self, person_name: str) -> Optional[List[Person]]:
        result = []
        body = {
            "query": {
                "multi_match": {
                    "query": person_name,
                    "fields": ["full_name"],
                    "type": "best_fields",
                    "fuzziness": "auto"
                }
            },
            "sort": [
                "_score"

            ]
        }

        try:
            response = await self.elastic.search(index=self.index, body=body, size=100)
            for item in response['hits']['hits']:
                data = self.model(**item['_source'])
                result.append(data)
        except NotFoundError:
            return None

        return result

    async def _get_items_from_elastic(
            self,
            index: str,
            filter_body: dict = None) -> Optional[List]:
        """
        Retrieves entries from elastic index using a filter.
        If filter is not set, all the docs are retrieved.
        Maximum possible rows amount is 10k.
        :param index: 'movies'
        :param filter_body: {"query": {}}
        :return: [Film, Film_a, Film_b, ... Film_n]
        """
        result = []
        model = BaseService.get_model(index)

        if not filter_body:
            filter_body = {"query": {"match_all": {}}}

        try:

            response = await self.elastic.search(index=index, body=filter_body, size=1000)
        except NotFoundError:
            return None
        logging.info('Retrieved %s info from elastic: %s', index, response['hits'])

        for item in response['hits']['hits']:
            data = model(**item['_source'])
            logging.debug(data)
            result.append(data)

        return result


@lru_cache()
def get_persons_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonsService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the servis object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return PersonsService(redis, elastic)
