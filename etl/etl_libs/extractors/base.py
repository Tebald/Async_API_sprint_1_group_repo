import logging
from abc import ABC, abstractmethod
from datetime import datetime

import backoff
import psycopg2
from psycopg2.extras import DictCursor

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    def __init__(self, dsn: dict):
        self.dsn = dsn
        self.connection = None

    def connect(self):
        if self.connection is None or self.connection.closed:
            self.connection = psycopg2.connect(**self.dsn, cursor_factory=DictCursor)
            logger.info("Соединение с Postgres открыто")

    def disconnect(self):
        if self.connection is not None and not self.connection.closed:
            self.connection.close()
            logger.info("Соединение с Postgres закрыто")

    def get_cursor(self):
        self.connect()
        return self.connection.cursor()

    @backoff.on_exception(backoff.expo, psycopg2.Error, max_time=300, jitter=backoff.random_jitter)
    def execute_query(self, query: str, params=None):
        with self.get_cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()

    def fetch_updated_records(self, table: str, last_modified: datetime, last_id: str) -> tuple[
        list[str], datetime, str]:
        query = """
            SELECT id, modified
            FROM content.{table}
            WHERE (modified = '{last_modified}' AND id > '{last_id}')
            OR modified > '{last_modified}'
            ORDER BY modified, id
            LIMIT 100;
        """.format(
            table=table,
            last_modified=last_modified,
            last_id=last_id
        )
        results = self.execute_query(query)
        logger.info("Extractor. Получено %s записей", len(results))
        try:
            last_record = results[-1]
            return [result["id"] for result in results], last_record["modified"], last_record["id"],
        except IndexError:
            return [], last_modified, last_id

    def fetch_mains_by_related_table(self, main_table: str, related_table: str, related_ids: list[str]) -> list[str]:
        not_fw_table = main_table if not main_table == 'film_work' else related_table
        query = """
            SELECT DISTINCT t.id, t.modified
            FROM content.{main_table} t
            JOIN content.{not_fw_table}_film_work tfw on tfw.{main_table}_id = t.id
            WHERE tfw.{related_table}_id in %s
            ORDER BY t.modified;
        """.format(
            main_table=main_table,
            not_fw_table=not_fw_table,
            related_table=related_table
        )
        results = self.execute_query(query, params=(tuple(related_ids),))
        logger.info(
            'Extractor %s: получены ID обновлённых записей из %s. Получено: %s->%s',
            main_table, related_table, len(related_ids), len(results))
        return [result["id"] for result in results]

    def get_earliest_modified_date(self, table: str) -> datetime:
        query = """
            SELECT MIN(modified) as earliest_modified
            FROM content.{};
        """.format(table)
        result = self.execute_query(query)[0]
        return result["earliest_modified"] if result else None

    @abstractmethod
    def fetch_by_ids(self, ids: list[str]):
        pass
