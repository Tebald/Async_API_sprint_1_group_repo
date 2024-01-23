import logging
from typing import Any

from etl_libs.extractors.base import BaseExtractor

logger = logging.getLogger(__name__)


class GenreExtractor(BaseExtractor):
    def fetch_by_ids(self, genre_ids: list[str]) -> list[dict[str, Any]]:
        query = """
            SELECT DISTINCT
                g.id as id,
                g.name,
                g.description
            FROM content.genre g
            WHERE g.id in %s;
        """
        result = self.execute_query(query, params=(tuple(genre_ids),))
        logger.info(f"По ID genres получены необходимые поля: {len(genre_ids)}->{len(result)}")
        return result
