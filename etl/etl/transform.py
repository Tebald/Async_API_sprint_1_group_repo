import logging
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

from etl.models import FilmworkModel, GenreModel, PersonModel

logger = logging.getLogger(__name__)


class BaseTransformer(ABC):
    TRANSFORM_TO_MODEL: BaseModel

    @abstractmethod
    def consolidate(self, details: list[dict[str, Any]]) -> list[BaseModel]:
        pass


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
                    "genre": set(),
                    "title": detail["title"],
                    "description": detail["description"],
                    "actors_names": [],
                    "writers_names": [],
                    "directors_names": [],
                    "actors": [],
                    "writers": [],
                    "directors": [],
                }
            self._process_role(film_id, detail, films)

            if detail["genre_name"] is not None:
                films[film_id]["genre"].add(detail["genre_name"])

        for film in films.values():
            film["genre"] = list(film["genre"])

        result = [self.MODEL(**film) for film in films.values()]
        logger.info(f"Transformer. Записи преобразованы в {self.MODEL.__name__}: {len(details)}->{len(result)}")
        return result

    def _process_role(self, film_id: str, detail: dict[str, Any], films: dict[str, Any]) -> None:
        role = detail["role"]
        person = {"id": detail["person_id"], "name": detail["full_name"]}
        full_name = detail["full_name"]

        try:
            if role == "actor" and person not in films[film_id]["actors"]:
                films[film_id]["actors"].append(person)
                films[film_id]["actors_names"].append(full_name)

            elif role == "writer" and person not in films[film_id]["writers"]:
                films[film_id]["writers"].append(person)
                films[film_id]["writers_names"].append(full_name)

            elif role == "director" and person not in films[film_id]["directors"]:
                films[film_id]["directors"].append(person)
                films[film_id]["directors_names"].append(full_name)
        except KeyError:
            logger.error(f"Не удалось определить роль {role}")


class GenreTransformer(BaseTransformer):
    MODEL = GenreModel

    def consolidate(self, details: list[dict[str, Any]]) -> list[GenreModel]:
        result = [self.MODEL(**genre) for genre in details]
        logger.info(f"Transformer. Записи преобразованы в {self.MODEL.__name__}: {len(details)}->{len(result)}")
        return result


class PersonTransformer(BaseTransformer):
    MODEL = PersonModel

    def consolidate(self, details: list[dict[str, Any]]) -> list[PersonModel]:
        persons = {}

        for detail in details:
            person_id = detail["p_id"]
            if person_id not in persons:
                persons[person_id] = {
                    "id": person_id,
                    "full_name": detail["full_name"],
                    "films": []
                }

            film_id = detail.get("film_id")
            if film_id:
                person_films = persons[person_id]['films']
                matching_films = [film for film in person_films if film["id"] == film_id]

                if matching_films:
                    matching_films[0]["roles"].append(detail['role'])
                else:
                    film = {"id": film_id, "roles": [detail["role"]]}
                    persons[person_id]["films"].append(film)

        result = [self.MODEL(**person) for person in persons.values()]
        logger.info(f"Transformer. Записи преобразованы в {self.MODEL.__name__}: {len(details)}->{len(result)}")
        return result
