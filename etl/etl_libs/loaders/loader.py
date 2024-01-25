import logging
from typing import Any, Generator

import backoff
from elastic_transport import TransportError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ElasticsearchLoader:
    def __init__(self, dsn: dict[str, str]):
        self.client = Elasticsearch(dsn)

    def close(self):
        if self.client:
            self.client.close()
            logger.info("Соединение с Elasticsearch закрыто")

    def index_exists(self, index_name):
        return self.client.indices.exists(index=index_name)

    @staticmethod
    def prepare_data(index: str, data: list[BaseModel]) -> Generator[dict[str, Any], None, None]:
        for doc in data:
            yield {"_index": index, "_id": doc.id, "_source": doc.model_dump()}

    @backoff.on_exception(backoff.expo, TransportError, max_time=300, jitter=backoff.random_jitter)
    def load_to_elasticsearch(self, index: str, data: list) -> None:
        if not self.index_exists(index):
            logger.error("Loader. Ошибка при записи в индекс. Индекс %s не найден.", index)
            return
        try:
            bulk(self.client, self.prepare_data(index, data))
            logger.info("Loader. Записи успешно загружены в индекс %s", index)
        except BulkIndexError:
            logger.error("Loader. При записи в индекс возникла ошибка.", exc_info=True)
