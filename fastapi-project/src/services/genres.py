from functools import lru_cache

from fastapi import Depends
from redis.asyncio import Redis

from src.db._redis import get_redis
from src.models.genre import Genre
from src.schemas import GenreSchema
from src.services.base import BaseService
from src.services.elastic import ElasticService, get_elastic_service


class GenresService(BaseService):
    """
    Class for business logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """

    index = 'genres'
    elastic_model = Genre
    redis_model = GenreSchema
    DEFAULT_SIZE = 1000


@lru_cache()
def get_genres_service(
    redis: Redis = Depends(get_redis),
    elastic_service: ElasticService = Depends(get_elastic_service),
) -> GenresService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis:
    :param elastic_service:
    :return:
    """
    return GenresService(redis, elastic_service)
