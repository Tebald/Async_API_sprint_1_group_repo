import logging
from typing import Any, Generator

import backoff
from elastic_transport import TransportError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk

from etl.models import FilmworkModel

logger = logging.getLogger(__name__)


class ElasticsearchLoader:
    def __init__(self, host: str):
        self.client = Elasticsearch(host)

    def close(self):
        if self.client:
            self.client.close()
            logger.info("Соединение с Elasticsearch закрыто")

    def prepare_data(self, index: str, data: list[FilmworkModel]) -> Generator[dict[str, Any], None, None]:
        for doc in data:
            yield {"_index": index, "_id": doc.id, "_source": doc.model_dump()}

    @backoff.on_exception(backoff.expo, TransportError, max_time=300, jitter=backoff.random_jitter)
    def load_to_elasticsearch(self, index: str, data: list) -> None:
        try:
            bulk(self.client, self.prepare_data(index, data))
            logger.info(f"Loader. Записи успешно загружены в индекс {index}")
        except BulkIndexError:
            logger.error("Ошибка индексации", exc_info=True)
