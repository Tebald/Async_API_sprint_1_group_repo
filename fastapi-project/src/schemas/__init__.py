from .films import FilmSchema, FilmShort  # noqa
from .genre import GenreSchema  # noqa
from .person import PersonSchema  # noqa

Schema = FilmSchema | GenreSchema | PersonSchema
