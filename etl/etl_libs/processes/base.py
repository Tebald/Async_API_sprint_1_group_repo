import logging
from abc import ABC
from datetime import datetime

from etl_libs.extractors.base import BaseExtractor
from etl_libs.loaders.loader import ElasticsearchLoader
from etl_libs.storage import State, JsonFileStorage
from etl_libs.transformers.base import BaseTransformer
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BaseETLProcess(ABC):
    TABLES: tuple
    INDEXES_MAPPING: dict
    MAIN_TABLE: str
    extractor: BaseExtractor
    transformer: BaseTransformer
    loader: ElasticsearchLoader

    def __init__(self):
        self.state = State(storage=JsonFileStorage(self.MAIN_TABLE + ".json"))

    @staticmethod
    def configure_logging(log_path: str, log_level: str = "INFO",
                          log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s") -> None:
        """Logging config.

        Args:
            log_path: A string of the path to save logs.
            log_level: A string of the logging level.
            log_format: A string of the logging format.

        Returns:
            None.

        Raises:
            ValueError if the log_level not valid.
        """

        log_levels = logging.getLevelNamesMapping()
        if log_level not in log_levels:
            print(f"Wrong log level. Received {log_level}, "
                  f"Expected one from: {log_levels.keys()}")
            raise ValueError()

        logging.basicConfig(
            handlers=[logging.FileHandler(log_path), logging.StreamHandler()],
            level=log_levels[log_level],
            format=log_format)

    @staticmethod
    def get_state_last_modified_key(table: str) -> str:
        """Returns a name of last_modified key for state."""
        return table + "_last_updated"

    @staticmethod
    def get_state_last_uuid_key(table: str) -> str:
        """Returns a name of last_uuid key for state."""
        return table + '_last_uuid'

    def run(self) -> None:
        """Sequentially runs the ETL process for tables which requires to extract.

        For each table, listed in the self.TABLES, runs a .process_table method.
        Logs when it starts and finishes.
        If the error occurs - logs and skips.
        """
        logger.info("=" * 80)
        logger.info("Запуск ETL")
        for table in self.TABLES:
            try:
                self.process_table(table)
            except Exception as e:
                logger.error("Таблица %s: ошибка при обработке таблицы %s: %s", self.MAIN_TABLE, table, e)
                continue
        logger.info("ETL завершил работу")
        logger.info("=" * 80)
        self.extractor.disconnect()
        self.loader.close()

    def get_last_modified(self, stated: str, table: str) -> datetime:
        """Returns the datetime to start extracting by 'modified'.

        If state has value of the last extracted datetime, returns it.
            Otherwise, need to extract full table starting from the oldest modified date.
            So returns oldest modified date.

        Args:
            stated: A datetime of the last processed record, which was saved to the state.
            table: A name of the table, which being processed now.

        Returns:
            The datetime from which start extraction process.
        """
        if stated:
            return datetime.fromisoformat(stated)
        return self.extractor.get_oldest_modified_date(table)

    def transform_data(self, extracted_table: str, extracted_ids: list[str]) -> list[BaseModel]:
        """Takes the name of the table and ids from it.
            Finds in the self.MAIN_TABLE all ids, related to the extracted.
            By ids from the self.MAIN_TABLE gets the full records.
            Returns a parsed Models of the records.

        If name of the self.MAIN_TABLE is equal to name of the extracted table,
            then ids from the self.MAIN_TABLE is equal to the extracted ids.

        Args:
            extracted_table: A string name of the table.
            extracted_ids: A list of the strings ids.

        Returns:
            A list of the Models.
            Type is equal to type of the objects in the self.MAIN_TABLE.
        """
        if extracted_table == self.MAIN_TABLE:
            objects_ids = extracted_ids
        else:
            objects_ids = self.extractor.fetch_mains_by_related_table(self.MAIN_TABLE, extracted_table, extracted_ids)

        film_details = self.extractor.fetch_by_ids(objects_ids)
        return self.transformer.consolidate(film_details)

    def process_table(self, extracted_table: str) -> None:
        """Method which starts ETL process, related to one pair of main and extracted table.

        How:
            1. Retrieve values `last_modified` and `last_uuid` from the state.
            2. Gets batch of the ids for updated records from the extracted_table (`fetch_updated_records`).
            3. Calls the `transform_data`, which returns the List of Models of the same objects as the self.MAIN_TABLE.
            4. Loads the data by `load_to_elasticsearch`.
            5. Saves to the state new `last_modified` and `last_uuid`.
            6. Repeats, while Extractor returns batches.


        Args:
            extracted_table: A string name of the current extracted table.

        Returns:
            None.
        """
        last_stated_modified = self.state.get_state(self.get_state_last_modified_key(extracted_table))
        last_stated_uuid = self.state.get_state(self.get_state_last_uuid_key(extracted_table))
        last_modified = self.get_last_modified(last_stated_modified, extracted_table)
        last_uuid = last_stated_uuid or "00000000-0000-0000-0000-000000000000"

        logger.info("=" * 80)
        logger.info("Таблица %s: начат процесс загрузки обновлений по %s", self.MAIN_TABLE, extracted_table)
        while True:
            logger.info("=" * 80)

            updated_records, last_modified_of_batch, last_uuid_of_batch = \
                self.extractor.fetch_updated_records(extracted_table, last_modified, last_uuid)
            if not updated_records:
                logger.info("Таблица %s: все обновления по %s загружены", self.MAIN_TABLE, extracted_table)
                break

            transformed_data = self.transform_data(extracted_table, updated_records)
            self.loader.load_to_elasticsearch(self.INDEXES_MAPPING[self.MAIN_TABLE], transformed_data)

            self.state.set_state(key=self.get_state_last_modified_key(extracted_table),
                                 value=str(last_modified_of_batch))
            self.state.set_state(key=self.get_state_last_uuid_key(extracted_table), value=last_uuid_of_batch)

            last_modified = last_modified_of_batch
            last_uuid = last_uuid_of_batch
