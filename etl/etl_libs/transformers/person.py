import logging
from typing import Any

from etl_libs.models import PersonModel
from etl_libs.transformers.base import BaseTransformer

logger = logging.getLogger(__name__)


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
