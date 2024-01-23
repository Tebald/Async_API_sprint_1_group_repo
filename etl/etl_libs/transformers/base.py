from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel


class BaseTransformer(ABC):
    TRANSFORM_TO_MODEL: BaseModel

    @abstractmethod
    def consolidate(self, details: list[dict[str, Any]]) -> list[BaseModel]:
        pass
