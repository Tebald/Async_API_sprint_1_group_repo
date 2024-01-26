import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Union, Iterable

import backoff
import psycopg2
from psycopg2.extensions import connection as _connection, cursor as _cursor
from psycopg2.extras import DictCursor

logger = logging.getLogger(__name__)


class BaseExtractor(ABC):
    def __init__(self, dsn: dict[str, Union[str, int]]):

        self.dsn = dsn
        self.connection: Optional[_connection] = None

    def connect(self) -> None:
        """Opens a connection with dsn and saves it to self.connection.

        If connection already exists does nothing.
        """
        if self.connection is None or self.connection.closed:
            self.connection = psycopg2.connect(**self.dsn, cursor_factory=DictCursor)
            logger.info("Соединение с Postgres открыто")

    def disconnect(self) -> None:
        """Closes a connection with dsn.

        If connection already closed does nothing.
        """
        if self.connection is not None and not self.connection.closed:
            self.connection.close()
            logger.info("Соединение с Postgres закрыто")

    def get_cursor(self) -> _cursor:
        """Returns a cursor."""
        self.connect()
        return self.connection.cursor()

    @backoff.on_exception(backoff.expo, psycopg2.Error, max_time=300, jitter=backoff.random_jitter)
    def execute_query(self, query: str, params: Iterable) -> list:
        """Executes a query.

        Args:
            query: A SQL query with '%s' in the place for params.
            params: An Iterable. Will be pasted instead of %s in sql.

        Returns:
            A list of sequences of chosen cursor_type.
        """
        with self.get_cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()

    def fetch_updated_records(self, table: str, last_modified: datetime, last_id: str) \
            -> tuple[list[str], datetime, str]:
        """Fetches the oldest records from the table, which wasn't fetched.

        How?:
            Sorts all records by the modified date and returns the oldest.
        Limited by batch size.
        If count(records with equal last_modified) > (batch size),
        returns no more than batch size.
        On the next call will make slice from the last_fetched_id.

        Args:
            table: A string name of the fetched table.
            last_modified: A last datetime value of the modified field from the last fetched record.
            last_id: A string uuid of the last fetched record.

        Returns:
            A Tuple with 3 elements:
            - A List of fetched record (List[str]).
            - A Datetime of modified from the newest of fetched records.
            - A string value of id from the newest of fetched records.
        """
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
        """Fetch the uuids of objects from main_table, which connected to related_ids.

        Uuids fetched from the m2m table by joining uuids of the related objects.
        The name of m2m table creates by both main_table and related_table.

        not_fw_table variable chooses one of the main_table and related_table not equal to 'film_work'.
        It is important to prevent getting a 'film_work_film_work' name of the m2m table in query.

        Args:
            main_table: A name of the table, which contains the ids of the fetched objects.
            related_table: A name of the table, which contains the ids of the related ids.
            related_ids: A list of the objects ids extracted from the related_table.

        Returns:
            A List of the main objects ids.
        """
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

    def get_oldest_modified_date(self, table: str) -> Optional[datetime]:
        """Returns the minimal modified value from the table.

        Args:
            table: A string name of the table.

        Returns:
            A datetime of the smallest date.
            If the table is empty, returns None.
        """
        query = """
            SELECT MIN(modified) as oldest_modified
            FROM content.{};
        """.format(table)
        result = self.execute_query(query)[0]
        return result["oldest_modified"] if result else None

    @abstractmethod
    def fetch_by_ids(self, ids: list[str]):
        """Base method for the Extractors.

        The extractors will override this method by the adding its own query.
        Each extractor has its custom set of the fields by related business-entity.

        Args:
            ids: list of the records ids.

        Returns:
            List of the records.
        """
        pass
