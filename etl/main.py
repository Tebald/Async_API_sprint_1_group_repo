import logging
import time

from etl_libs.config import settings
from etl_libs.create_index import index_files_in_directory
from etl_libs.processes.base import BaseETLProcess
from etl_libs.processes.filmwork import FilmworkETLProcess
from etl_libs.processes.genre import GenreETLProcess
from etl_libs.processes.person import PersonETLProcess


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

    logger.info("Создание индексов")
    index_files_in_directory(settings.index_jsons_dir, es_host)
    logger.info("Индексы созданы")

    # Соединения с PG и ES открываются и закрываются единожды (questionable)
    etl_film_works = FilmworkETLProcess(postgres_dsl=dsl, es_host=es_host)
    etl_genres = GenreETLProcess(postgres_dsl=dsl, es_host=es_host)
    etl_persons = PersonETLProcess(postgres_dsl=dsl, es_host=es_host)

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
