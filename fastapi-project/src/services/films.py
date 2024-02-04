from functools import lru_cache

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

    def _process_kwargs(self, kwargs: dict):
        """
        Override kwargs processing from BaseService and adds its own.
        kwargs will be added to `.search()` function of ElasticService.

        Added kwargs has default value If they are not being provided.
        """
        kwargs = super()._process_kwargs(kwargs)
        kwargs['body'] = {
            'query': self._process_genre_filter(kwargs.pop('genre', None)),
            'sort': self._process_sort(kwargs.pop('sort', '-_score')),
        }
        return kwargs

    @staticmethod
    def _process_sort(sort: str) -> dict[str, str]:
        """
        Creates a sort param for _process_kwargs.
        Converts `+/-param` to {'param': 'asc'|'desc'}
        """
        return {sort.lstrip('-'): 'desc' if sort.startswith('-') else 'asc'}

    @staticmethod
    def _process_genre_filter(genre: str | None) -> dict:
        """
        Creates a nested filter param for _process_kwargs.
        Filter passes only films with the same genre as {genre}.
        If genre not specified, every film will pass. ('match_all')

        :param genre: filter param.
        :return: dict for `body` of request for Elasticsearch.
        """
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
