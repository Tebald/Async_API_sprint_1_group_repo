from functools import lru_cache
from typing import List, Optional, Tuple, Union

from fastapi import Depends

from src.models import Film
from src.schemas.films import FilmSchema
from src.services.base import BaseService
from src.services.elastic import ElasticService, get_elastic_service
from src.services.redis import RedisService, get_redis_service


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

    async def get_many(self, **kwargs) -> Optional[Tuple[List[Union[Film, FilmSchema]], int]]:
        """
        Retrieves all entries from elastic index.
        It is not recommended to use this method to retrieve large amount of rows.
        Maximum possible rows amount is 10k.
        :return: [Film, Film_a, Film_b, ... Film_n]
        """
        kwargs['body'] = {
            'query': self._process_genre_filter(kwargs.pop('genre', None)),
            'sort': self._process_sort(kwargs.pop('sort', '-_score')),
        }
        return await super().get_many(**kwargs)

    @staticmethod
    def _process_sort(sort: str):
        return {sort.lstrip('-'): 'desc' if sort.startswith('-') else 'asc'}

    @staticmethod
    def _process_genre_filter(genre: str | None):
        if genre:
            return {'nested': {'path': 'genre', 'query': {'bool': {'must': [{'match': {'genre.id': genre}}]}}}}
        return {'match_all': {}}


@lru_cache()
def get_films_service(
    redis_service: RedisService = Depends(get_redis_service),
    elastic_service: ElasticService = Depends(get_elastic_service),
) -> FilmsService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis_service:
    :param elastic_service:
    :return:
    """
    return FilmsService(redis_service, elastic_service)
