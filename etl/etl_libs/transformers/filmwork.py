import logging
from typing import Any

from etl_libs.models import FilmworkModel
from etl_libs.transformers.base import BaseTransformer

logger = logging.getLogger(__name__)


class FilmworkTransformer(BaseTransformer):
    MODEL = FilmworkModel

    def consolidate(self, details: list[dict[str, Any]]) -> list[FilmworkModel]:
        films = {}

        for detail in details:
            film_id = detail["fw_id"]
            if film_id not in films:
                films[film_id] = {
                    "id": film_id,
                    "imdb_rating": detail["rating"],
                    "title": detail["title"],
                    "description": detail["description"],
                    "genre": [],
                    "actors_names": [],
                    "writers_names": [],
                    "directors_names": [],
                    "actors": [],
                    "writers": [],
                    "directors": [],
                }
            self._process_role(film_id, detail, films)

            if detail["genre_name"] is not None:
                genre = {'id': detail['genre_id'], 'name': detail['genre_name']}
                if genre not in films[film_id]['genre']:
                    films[film_id]['genre'].append(genre)

        for film in films.values():
            film["genre"] = list(film["genre"])

        result = [self.MODEL(**film) for film in films.values()]
        logger.info("Transformer. Записи преобразованы в %s: %s->%s", self.MODEL.__name__, len(details), len(result))
        return result

    def _process_role(self, film_id: str, detail: dict[str, Any], films: dict[str, Any]) -> None:
        roles = ['actor', 'writer', 'director', None]
        role = detail["role"]
        person = {"id": detail["person_id"], "name": detail["full_name"]}
        full_name = detail["full_name"]

        if role not in roles:
            logger.error(f"Не удалось определить роль %s", role)
            return

        if role == "actor" and person not in films[film_id]["actors"]:
            films[film_id]["actors"].append(person)
            films[film_id]["actors_names"].append(full_name)

        elif role == "writer" and person not in films[film_id]["writers"]:
            films[film_id]["writers"].append(person)
            films[film_id]["writers_names"].append(full_name)

        elif role == "director" and person not in films[film_id]["directors"]:
            films[film_id]["directors"].append(person)
            films[film_id]["directors_names"].append(full_name)
