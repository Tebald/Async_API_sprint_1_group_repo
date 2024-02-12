from functools import lru_cache

from fastapi import Depends
from src.models.genre import Genre
from src.schemas import GenreSchema
from src.services._redis import RedisService, get_redis_service
from src.services.base import BaseService
from src.services.elastic import ElasticService, get_elastic_service
from src.utils.kwargs_transformer.transformer import KwargsTransformer, get_kwargs_transformer


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
    redis_service: RedisService = Depends(get_redis_service),
    elastic_service: ElasticService = Depends(get_elastic_service),
        kwargs_transformer: KwargsTransformer = Depends(get_kwargs_transformer),
) -> GenresService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis_service:
    :param elastic_service:
    :param kwargs_transformer:
    :return:
    """
    return GenresService(redis_service, elastic_service, kwargs_transformer)
