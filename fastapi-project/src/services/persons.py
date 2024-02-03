from functools import lru_cache

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from redis.asyncio import Redis

from src.db._redis import get_redis
from src.db.elastic import get_elastic
from src.models.person import Person
from src.schemas import PersonSchema
from src.services.base import BaseService


class PersonsService(BaseService):
    """
    Class for business logic to operate with person entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    index = 'persons'
    elastic_model = Person
    redis_model = PersonSchema
    search_field = 'full_name'


@lru_cache()
def get_persons_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonsService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return PersonsService(redis, elastic)
