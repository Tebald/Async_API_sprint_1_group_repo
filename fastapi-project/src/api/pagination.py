from __future__ import annotations

from typing import Sequence, Generic, Any
from typing import TypeVar

from pydantic import BaseModel

from .params import Params

T = TypeVar('T')


class Page(BaseModel, Generic[T]):
    total: int
    items: Sequence[T]
    page: int
    size: int

    def __init__(self, **data: Any):
        super().__init__(**data)

    @classmethod
    def create(cls, items: Sequence[T], total: int, params: Params) -> Page[T]:
        return cls(total=total, items=items, page=params.page, size=params.size)

    class Config:
        arbitrary_types_allowed = True
