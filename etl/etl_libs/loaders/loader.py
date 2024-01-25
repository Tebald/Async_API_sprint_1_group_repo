import logging
from typing import Any, Generator, Iterable

import backoff
from elastic_transport import TransportError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class ElasticsearchLoader:
    def __init__(self, dsn: dict[str, str]):
        """Opens connection with Elasticsearch after initializing."""
        self.client = Elasticsearch(dsn)

    def close(self) -> None:
        """Closes connection with Elasticsearch if it is opened."""
        if self.client:
            self.client.close()
            logger.info("Соединение с Elasticsearch закрыто")

    def index_exists(self, index_name) -> bool:
        """Checks if index exists in Elasticsearch."""
        return self.client.indices.exists(index=index_name)

    @staticmethod
    def prepare_data(index: str, data: Iterable[BaseModel]) -> Generator[dict[str, Any], None, None]:
        """Decorates every model from data.

        Dumps model in predefined structure. Format:
        {'_index': name of index, '_id': id of document, '_source': dump of model

        Args:
            index: A name of the index in Elasticsearch.
            data: A list of the Models.

        Returns:
            A Generator of the dicts.
        """
        for doc in data:
            yield {"_index": index, "_id": doc.id, "_source": doc.model_dump()}

    @backoff.on_exception(backoff.expo, TransportError, max_time=300, jitter=backoff.random_jitter)
    def load_to_elasticsearch(self, index: str, data: Iterable[BaseModel]) -> None:
        """Main loading function.

        Make requests to ElasticSearch and loads data in index.
        Uses bulk to optimize loading multiple records.

        If bulk raises BulkIndexError, just silently logs.

        Args:
            index: A string name of the index in ElasticSearch.
            data: An Iterable of the Models to load.

        Returns:
            None.
        """
        if not self.index_exists(index):
            logger.error("Loader. Ошибка при записи в индекс. Индекс %s не найден.", index)
            return
        try:
            bulk(self.client, self.prepare_data(index, data))
            logger.info("Loader. Записи успешно загружены в индекс %s", index)
        except BulkIndexError:
            logger.error("Loader. При записи в индекс возникла ошибка.", exc_info=True)
