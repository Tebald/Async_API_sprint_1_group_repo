from functools import lru_cache

from db._redis import get_redis
from db.elastic import get_elastic
from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from models.genre import Genre
from redis.asyncio import Redis
from services.transfer import TransferService


class GenresService(TransferService):
    """
    Class for buisness logic to operate with film/person/genre entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    index = 'genres'
    model = Genre


@lru_cache()
def get_genres_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenresService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the servis object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return GenresService(redis, elastic)