import logging
from typing import Any

from etl_libs.models import GenreModel
from etl_libs.transformers.base import BaseTransformer

logger = logging.getLogger(__name__)


class GenreTransformer(BaseTransformer):
    MODEL = GenreModel

    def consolidate(self, details: list[dict[str, Any]]) -> list[GenreModel]:
        result = [self.MODEL(**genre) for genre in details]
        logger.info(f"Transformer. Записи преобразованы в {self.MODEL.__name__}: {len(details)}->{len(result)}")
        return result
