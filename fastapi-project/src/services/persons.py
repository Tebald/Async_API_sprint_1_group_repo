from functools import lru_cache

from fastapi import Depends
from redis.asyncio import Redis

from src.db._redis import get_redis
from src.models.person import Person
from src.schemas import PersonSchema
from src.services.base import BaseService
from src.services.elastic import ElasticService, get_elastic_service


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
    elastic_service: ElasticService = Depends(get_elastic_service),
) -> PersonsService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis:
    :param elastic_service:
    :return:
    """
    return PersonsService(redis, elastic_service)
