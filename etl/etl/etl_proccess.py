import logging
from abc import ABC
from datetime import datetime

from pydantic import BaseModel

from etl.extract import FilmworkExtractor, BaseExtractor, GenreExtractor, PersonExtractor
from etl.load import ElasticsearchLoader
from etl.storage import State, JsonFileStorage
from etl.transform import FilmworkTransformer, BaseTransformer, GenreTransformer, PersonTransformer

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


class FilmworkETLProcess(BaseETLProcess):
    TABLES = ("film_work", "genre", "person",)
    INDICES_MAPPING = {"film_work": "movies", "genre": "genres", "person": "persons"}
    MAIN_TABLE = "film_work"
    EXTRACTOR_CLASS = FilmworkExtractor
    TRANSFORMER_CLASS = FilmworkTransformer
    LOADER_CLASS = ElasticsearchLoader

    def __init__(self, postgres_dsl: dict, es_host: str):
        self.extractor = self.EXTRACTOR_CLASS(postgres_dsl)
        self.transformer = self.TRANSFORMER_CLASS()
        self.loader = self.LOADER_CLASS(es_host)
        super().__init__()


class GenreETLProcess(BaseETLProcess):
    TABLES = ("film_work", "genre",)
    INDICES_MAPPING = {"film_work": "movies", "genre": "genres"}
    MAIN_TABLE = "genre"
    EXTRACTOR_CLASS = GenreExtractor
    TRANSFORMER_CLASS = GenreTransformer
    LOADER_CLASS = ElasticsearchLoader

    def __init__(self, postgres_dsl: dict, es_host: str):
        self.extractor = self.EXTRACTOR_CLASS(postgres_dsl)
        self.transformer = self.TRANSFORMER_CLASS()
        self.loader = self.LOADER_CLASS(es_host)
        super().__init__()


class PersonETLProcess(BaseETLProcess):
    TABLES = ("film_work", "person",)
    INDICES_MAPPING = {"film_work": "movies", "person": "persons"}
    MAIN_TABLE = "person"
    EXTRACTOR_CLASS = PersonExtractor
    TRANSFORMER_CLASS = PersonTransformer
    LOADER_CLASS = ElasticsearchLoader

    def __init__(self, postgres_dsl: dict, es_host: str):
        self.extractor = self.EXTRACTOR_CLASS(postgres_dsl)
        self.transformer = self.TRANSFORMER_CLASS()
        self.loader = self.LOADER_CLASS(es_host)
        super().__init__()
