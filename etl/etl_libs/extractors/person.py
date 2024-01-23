import logging
from typing import Any

from etl_libs.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class PersonExtractor(BaseExtractor):
    def fetch_by_ids(self, person_ids: list[str]) -> list[dict[str, Any]]:
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
        logger.info(f"По ID persons получены необходимые поля: {len(person_ids)}->{len(result)}")
        return result
