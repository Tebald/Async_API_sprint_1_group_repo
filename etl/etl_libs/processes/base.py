import logging
from abc import ABC
from datetime import datetime

from pydantic import BaseModel

from etl_libs.extractors.base import BaseExtractor
from etl_libs.loaders.loader import ElasticsearchLoader
from etl_libs.storage import State, JsonFileStorage
from etl_libs.transformers.base import BaseTransformer

logger = logging.getLogger(__name__)


class BaseETLProcess(ABC):
    TABLES: tuple
    INDICES_MAPPING: dict
    MAIN_TABLE: str
    extractor: BaseExtractor
    transformer: BaseTransformer
    loader: ElasticsearchLoader

    def __init__(self):
        self.state = State(storage=JsonFileStorage(self.MAIN_TABLE + '.json'))

    @staticmethod
    def get_state_last_modified_key(table: str) -> str:
        return table + "_last_updated"

    @staticmethod
    def get_state_last_uuid_key(table: str) -> str:
        return table + '_last_uuid'

    def run(self):
        logger.info("==============================================================================")
        logger.info("Запуск ETL")
        for table in self.TABLES:
            try:
                self.process_table(table)
            except Exception as e:
                logger.error(f"Таблица {self.MAIN_TABLE}: ошибка при обработке таблицы {table}: {e}")
                continue
        logger.info("ETL завершил работу")
        logger.info("==============================================================================")
        self.extractor.disconnect()
        self.loader.close()

    def get_last_modified(self, state: str, table: str) -> datetime:
        if not state:
            return self.extractor.get_earliest_modified_date(table)
        return datetime.fromisoformat(state)

    def transform_data(self, table: str, updated_records: list[str]) -> list[BaseModel]:
        if table == self.MAIN_TABLE:
            film_details = self.extractor.fetch_by_ids(updated_records)
        else:
            film_ids = self.extractor.fetch_mains_by_related_table(self.MAIN_TABLE, table, updated_records)
            film_details = self.extractor.fetch_by_ids(film_ids)
        return self.transformer.consolidate(film_details)

    def process_table(self, table: str):
        last_stated_modified = self.state.get_state(self.get_state_last_modified_key(table))
        last_stated_uuid = self.state.get_state(self.get_state_last_uuid_key(table))
        last_modified = self.get_last_modified(last_stated_modified, table)
        last_uuid = last_stated_uuid or "00000000-0000-0000-0000-000000000000"

        logger.info("==============================================================================")
        logger.info(f"Таблица {self.MAIN_TABLE}: начат процесс загрузки обновлений по {table}")
        while True:
            logger.info("==============================================================================")
            updated_records, last_modified_of_batch, last_uuid_of_batch = self.extractor.fetch_updated_records(table,
                                                                                                               last_modified,
                                                                                                               last_uuid)
            if not updated_records:
                logger.info(f"Таблица {self.MAIN_TABLE}: все обновления по {table} загружены")
                break

            transformed_data = self.transform_data(table, updated_records)
            self.loader.load_to_elasticsearch(self.INDICES_MAPPING[self.MAIN_TABLE], transformed_data)

            self.state.set_state(key=self.get_state_last_modified_key(table), value=str(last_modified_of_batch))
            self.state.set_state(key=self.get_state_last_uuid_key(table), value=last_uuid_of_batch)

            last_modified = last_modified_of_batch
            last_uuid = last_uuid_of_batch
