import logging
from typing import Any

from etl_libs.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class GenreExtractor(BaseExtractor):
    def fetch_by_ids(self, genre_ids: list[str]) -> list[dict[str, Any]]:
        """Fetches genres by their ids.

        Args:
            genre_ids: A list of the ids from 'genre' table.

        Returns:
            List of the dicts where keys are requested fields,
                and values are values of record.
        """
        query = """
            SELECT DISTINCT
                g.id as id,
                g.name,
                g.description
            FROM content.genre g
            WHERE g.id in %s;
        """
        result = self.execute_query(query, params=(tuple(genre_ids),))
        logger.info("По ID genres получены необходимые поля: %s->%s", len(genre_ids), len(result))
        return result
