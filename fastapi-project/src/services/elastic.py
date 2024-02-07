import logging
from functools import lru_cache
from typing import Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from src.db.elastic.elastic import get_elastic
from src.models import ElasticModel


class ElasticService:
    """
    A class to combine all the Elasticsearch functions in the one place.
    It contains all functions, used to fetch data from Elasticsearch.
    """
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def search(self, index: str, model: type[ElasticModel], **kwargs) -> Optional[list[ElasticModel]]:
        """Process search query to ElasticSearch.

        :param index: A ElasticSearch index to fetch.
        :param model: A destination model to parse.
        :param kwargs: Other optional keyword arguments.

        Available all the kwargs from official documentation:
        https://elasticsearch-py.readthedocs.io/en/7.9.1/

        :return: List of fetched objects parsed in Models or None.
        """
        try:
            response = await self.elastic.search(index=index, **kwargs)
        except NotFoundError:
            return None
        result = [model(**item['_source']) for item in response['hits']['hits']]
        logging.info('Retrieved objects from elasticsearch: count=(%s)', len(result))
        return result

    async def get(self, index: str, model: type[ElasticModel], object_id: str) -> Optional[ElasticModel]:
        """Process `get` query to ElasticSearch.

        Returns an object if it exists in elastic.
        :param model:
        :param index: A name of the index in ElasticSearch.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film | Genre | Person
        """
        try:
            item = await self.elastic.get(index=index, id=object_id)
            result = model(**item['_source'])
            logging.info('Retrieved object info from elasticsearch: %s', result)
            return result
        except NotFoundError:
            return None

    async def mget(self, index: str, model: type[ElasticModel], object_ids: list[str]) -> Optional[list[ElasticModel]]:
        """Process `mget` query to Elasticsearch.

        Gets list of API objects by ids.
        :param index: index to fetch.
        :param model: destination model to parse.
        :param object_ids: list of objects ids.
        :return: List of API objects.
        """
        try:
            response = await self.elastic.mget(index=index, body={'ids': object_ids})
            result = [model(**item['_source']) for item in response['docs']]
            logging.info('Retrieved objects from elasticsearch: count=(%s)', len(result))
            return result
        except NotFoundError:
            return None

    async def count(self, index: str, query: dict = None) -> int:
        """Process `count` query to Elasticsearch.

        Gets count of records in specified index.
        :param index: A name of the index.
        :param query: A query to process count.
        :return: An integer count of records.
        """
        count = await self.elastic.count(index=index, body={'query': query})
        return count.get('count')


@lru_cache()
def get_elastic_service(
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> ElasticService:
    """Provider of ElasticService.

    :param elastic: An AsyncElasticsearch exemplar which represents async connection to ES.
    :return: A Singleton of ElasticService.
    """
    return ElasticService(elastic)
