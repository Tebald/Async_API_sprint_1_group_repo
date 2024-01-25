import logging
from typing import Any

from etl_libs.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class PersonExtractor(BaseExtractor):
    def fetch_by_ids(self, person_ids: list[str]) -> list[dict[str, Any]]:
        """Fetches persons by their ids.

        Args:
            person_ids: A list of the ids from the 'persons' table.

        Returns:
            List of the dicts where keys are requested fields,
                and values are values from record.
        """
        query = """
            SELECT DISTINCT
                p.id as p_id,
                p.full_name,
                pfw.role,
                f.id as film_id
            FROM content.person p
            LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
            LEFT JOIN content.film_work f ON f.id = pfw.film_work_id
            WHERE p.id in %s;
        """
        result = self.execute_query(query, params=(tuple(person_ids),))
        logger.info("По ID persons получены необходимые поля: %s->%s", len(person_ids), len(result))
        return result
