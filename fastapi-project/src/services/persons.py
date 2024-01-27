import logging
from functools import lru_cache
from typing import List, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db._redis import get_redis
from db.elastic import get_elastic
from models.person import Person
from services.transfer import BaseService


class PersonsService(BaseService):
    """
    Class for buisness logic to operate with person entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """
    index = 'persons'
    model = Person
    search_field = "full_name"


@lru_cache()
def get_persons_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonsService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the servis object in a single exemplar (singleton).

    :param redis:
    :param elastic:
    :return:
    """
    return PersonsService(redis, elastic)
