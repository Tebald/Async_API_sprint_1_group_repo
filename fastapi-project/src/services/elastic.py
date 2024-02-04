from functools import lru_cache
from typing import Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends

from src.db.elastic import get_elastic
from src.models import ElasticModel, Film, Genre, Person


class ElasticService:
    INDEX_TO_BACKEND_MODEL = {'movies': Film, 'genres': Genre, 'persons': Person}

    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def search(self, index: str, **kwargs) -> Optional[tuple[list[ElasticModel], int]]:
        """Process search query to ElasticSearch.

        :return: Tuple with two values. (List of fetched objects parsed in Models, Integer total count of records in ES)
        """
        model = self.INDEX_TO_BACKEND_MODEL.get(index)
        try:
            response = await self.elastic.search(index=index, **kwargs)
            total = response['hits']['total']['value']
            return [model(**item['_source']) for item in response['hits']['hits']], total
        except NotFoundError:
            return None

    async def get(self, index: str, object_id: str) -> Optional[ElasticModel]:
        """Process get query to ElasticSearch.

        Returns an object if it exists in elastic.
        :param index: A name of the index in ElasticSearch.
        :param object_id: '00af52ec-9345-4d66-adbe-50eb917f463a'
        :return: Film | Genre | Person
        """
        model = self.INDEX_TO_BACKEND_MODEL.get(index)
        try:
            item = await self.elastic.get(index=index, id=object_id)
            return model(**item['_source'])
        except NotFoundError:
            return None

    async def mget(self, index, object_ids):
        model = self.INDEX_TO_BACKEND_MODEL.get(index)
        try:
            response = await self.elastic.mget(index=index, body={'ids': object_ids})
            return [model(**item['_source']) for item in response['docs']]
        except NotFoundError:
            return None


@lru_cache()
def get_elastic_service(
    elastic: AsyncElasticsearch = Depends(get_elastic),
) -> ElasticService:
    """Provider of ElasticService.

    :param elastic: An AsyncElasticSearch exemplar which represents async connection to ES.
    :return: A Singleton of ElasticService.
    """
    return ElasticService(elastic)
