import logging

from etl_libs.config import settings
from etl_libs.processes.filmwork import FilmworkETLProcess
from etl_libs.processes.genre import GenreETLProcess
from etl_libs.processes.person import PersonETLProcess


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    logger = logging.getLogger(__name__)

    logger.info("НАЧИНАЕМ")
    dsl = {
        "dbname": settings.postgres_db,
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "host": settings.db_host,
        "port": settings.db_port,
    }
    es_host = settings.es_host

    etl_film_works = FilmworkETLProcess(postgres_dsl=dsl, es_host=es_host)
    etl_film_works.run()

    etl_genres = GenreETLProcess(postgres_dsl=dsl, es_host=es_host)
    etl_genres.run()

    etl_persons = PersonETLProcess(postgres_dsl=dsl, es_host=es_host)
    etl_persons.run()


if __name__ == "__main__":
    main()
