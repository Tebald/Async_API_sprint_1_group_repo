import logging
import time

from etl_libs.config import settings
from etl_libs.elastic_lib import Elastic
from etl_libs.processes.base import BaseETLProcess
from etl_libs.processes.filmwork import FilmworkETLProcess
from etl_libs.processes.genre import GenreETLProcess
from etl_libs.processes.person import PersonETLProcess


def check_indexes_first(es_indexes: list, host: str, port: int) -> None:
    """
    Function to check if necessary indexes are present in elastic.
    If all indexes are present, proceeds back to main function.
    :param es_indexes: ['movies', 'persons', 'genres']
    :param host: 'elasticsearch'
    :param port: 9200
    :return:
    """
    while True:
        # This part is necessary to prevent data migration (automatic index creation)
        # until we add index manually.
        index_exist = []
        for index in es_indexes:
            check = Elastic.check_index(
                host=host,
                port=port,
                index_name=index)

            logging.info('Status code: %s. Text: %s', check['status_code'], check['text'])

            if not check.get('errors', ''):
                index_exist.append(True)

        if len(index_exist) == len(es_indexes):
            return

        time.sleep(30)


def main():
    logger = logging.getLogger(__name__)
    BaseETLProcess.configure_logging(
        log_path=settings.log_path,
        log_level=settings.log_level,
        log_format=settings.log_format
    )

    dsl = {
        "dbname": settings.postgres_db,
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "host": settings.db_host,
        "port": settings.db_port,
    }

    es_host = settings.es_host

    # Соединения с PG и ES открываются и закрываются единожды (questionable)
    etl_film_works = FilmworkETLProcess(postgres_dsl=dsl, es_host=es_host)
    etl_genres = GenreETLProcess(postgres_dsl=dsl, es_host=es_host)
    etl_persons = PersonETLProcess(postgres_dsl=dsl, es_host=es_host)

    check_indexes_first(es_indexes=settings.es_indexes, host=settings.host, port=settings.port)

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
