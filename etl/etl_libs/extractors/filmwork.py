import logging
from typing import Any

from etl_libs.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class FilmworkExtractor(BaseExtractor):
    def fetch_by_ids(self, film_ids: list[str]) -> list[dict[str, Any]]:
        """Fetches film_works by their ids.

        Args:
            film_ids: A list of the ids from 'film_work' table.

        Returns:
            List of the dicts where keys are requested fields,
                and values are values of record.
        """
        query = """
            SELECT DISTINCT
                fw.id as fw_id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.created,
                fw.modified,
                pfw.role,
                p.id as person_id,
                p.full_name,
                g.id as genre_id,
                g.name as genre_name
            FROM content.film_work fw
            LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            WHERE fw.id IN %s;
        """
        result = self.execute_query(query, params=(tuple(film_ids),))
        logger.info("По ID film_works получены необходимые поля: %s->%s", len(film_ids), len(result))
        return result
