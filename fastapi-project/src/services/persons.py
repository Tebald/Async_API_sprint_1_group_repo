from functools import lru_cache

from fastapi import Depends
from src.models.person import Person
from src.schemas import PersonSchema
from src.services._redis import RedisService, get_redis_service
from src.services.base import BaseService
from src.services.elastic import ElasticService, get_elastic_service
from src.utils.kwargs_transformer.transformer import KwargsTransformer, get_kwargs_transformer


class PersonsService(BaseService):
    """
    Class for business logic to operate with person entities.
    It contains functions to take data from elastic or redis and
    send it to api modules.
    """

    index = 'persons'
    elastic_model = Person
    redis_model = PersonSchema


@lru_cache()
def get_persons_service(
    redis_service: RedisService = Depends(get_redis_service),
    elastic_service: ElasticService = Depends(get_elastic_service),
        kwargs_transformer: KwargsTransformer = Depends(get_kwargs_transformer),
) -> PersonsService:
    """
    Provider of TransferService.
    'Depends' declares that Redis and Elasticsearch are necessary.
    lru_cache decorator makes the service object in a single exemplar (singleton).

    :param redis_service:
    :param elastic_service:
    :param kwargs_transformer:
    :return:
    """
    return PersonsService(redis_service, elastic_service, kwargs_transformer)
