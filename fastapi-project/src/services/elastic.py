import logging
from functools import lru_cache
from typing import Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from src.db.elastic import get_elastic
from src.models import ElasticModel


class ElasticService:
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def search(self, index: str, model: type[ElasticModel], **kwargs) -> Optional[tuple[list[ElasticModel], int]]:
        """Process search query to ElasticSearch.

        :return: Tuple with two values. (List of fetched objects parsed in Models, Integer total count of records in ES)
        """
        try:
            response = await self.elastic.search(index=index, **kwargs)
        except NotFoundError:
            return None
        total = response['hits']['total']['value']
        result = [model(**item['_source']) for item in response['hits']['hits']], total
        logging.info('Retrieved objects from elasticsearch: count=(%s)', len(result[0]))
        return result

    async def get(self, index: str, model: type[ElasticModel], object_id: str) -> Optional[ElasticModel]:
        """Process get query to ElasticSearch.

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
        try:
            response = await self.elastic.mget(index=index, body={'ids': object_ids})
            result = [model(**item['_source']) for item in response['docs']]
            logging.info('Retrieved objects from elasticsearch: count=(%s)', len(result))
            return result
        except NotFoundError:
            return None


@lru_cache()
def get_elastic_service(
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> ElasticService:
    """Provider of ElasticService.

    :param elastic: An AsyncElasticsearch exemplar which represents async connection to ES.
    :return: A Singleton of ElasticService.
    """
    return ElasticService(elastic)
