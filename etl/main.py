import logging
import time

from elasticsearch import Elasticsearch

from etl_libs.config import get_settings
from etl_libs.processes.base import BaseETLProcess
from etl_libs.processes.filmwork import FilmworkETLProcess
from etl_libs.processes.genre import GenreETLProcess
from etl_libs.processes.person import PersonETLProcess


def check_indexes_first(indexes: list, es_dsn: str) -> None:
    """
    Function to check if necessary indexes are present in elastic.
    If all indexes are present, proceeds back to main function.
    :param indexes: ['movies', 'persons', 'genres']
    :param es_dsn: 'http://es_host:es_port'
    :return:
    """
    es = Elasticsearch(es_dsn)

    while True:
        # This part is necessary to prevent data migration (automatic index creation)
        # until we add index manually.
        missing_indexes = [index for index in indexes if not es.indices.exists(index=index)]

        if not missing_indexes:
            break

        time.sleep(10)


def main():
    settings = get_settings()

    logger = logging.getLogger(__name__)
    BaseETLProcess.configure_logging(
        log_path=settings.logger.path,
        log_level=settings.logger.level,
        log_format=settings.logger.format
    )

    pg_dsn = {
        "dbname": settings.postgres.db,
        "user": settings.postgres.user,
        "password": settings.postgres.password,
        "host": settings.postgres.host,
        "port": settings.postgres.port,
    }
    es_dsn = f"http://{settings.elastic.host}:{settings.elastic.port}"

    # Соединения с PG и ES открываются и закрываются единожды (questionable)
    etl_film_works = FilmworkETLProcess(pg_dsn=pg_dsn, es_dsn=es_dsn)
    etl_genres = GenreETLProcess(pg_dsn=pg_dsn, es_dsn=es_dsn)
    etl_persons = PersonETLProcess(pg_dsn=pg_dsn, es_dsn=es_dsn)

    logger.info('Ожидается создание индексов...')
    check_indexes_first(indexes=settings.elastic.indexes, es_dsn=es_dsn)

    logger.info("НАЧИНАЕМ")
    try:
        while True:
            logger.info("Запущен новый цикл ETL")
            etl_genres.run()
            etl_film_works.run()
            etl_persons.run()
            time.sleep(settings.interval)
    except Exception as exc_info:
        logger.error("Непредвиденная ошибка: ", exc_info=exc_info)
    logger.error("Произошёл выход из цикла")


if __name__ == "__main__":
    main()
