from pydantic.main import BaseModel
from pydantic.types import UUID4


class PersonForFilm(BaseModel):
    uuid: UUID4
    full_name: str


class Film(BaseModel):
    """
    Response model for Film object.
    This class contains info we return to a user.
    """
    uuid: UUID4
    title: str
    description: str | None
    genre: list[str] | None
    imdb_rating: float | None
    actors: list[PersonForFilm]
    writers: list[PersonForFilm]
    directors: list[PersonForFilm]


class FilmShort(BaseModel):
    """
    Short version of Film object.
    It is used to form a list of films.
    """
    uuid: UUID4
    title: str
    imdb_rating: float | None
