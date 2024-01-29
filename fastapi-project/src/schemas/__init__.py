from .films import FilmSchema, FilmShort  # noqa
from .genre import GenreSchema  # noqa
from .person import PersonSchema  # noqa

from typing import TypeVar

Schema = TypeVar("Schema", bound=type[FilmSchema, GenreSchema, PersonSchema])
