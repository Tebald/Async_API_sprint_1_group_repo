from pydantic.main import BaseModel

from src.schemas.genre import GenreSchema


class PersonForFilm(BaseModel):
    uuid: str
    full_name: str


class FilmSchema(BaseModel):
    """
    Response model for Film object.
    This class contains info we return to a user.
    """

    uuid: str
    title: str
    description: str | None
    genre: list[GenreSchema] | None
    imdb_rating: float | None
    actors: list[PersonForFilm]
    writers: list[PersonForFilm]
    directors: list[PersonForFilm]


class FilmShort(BaseModel):
    """
    Short version of Film object.
    It is used to form a list of films.
    """

    uuid: str
    title: str
    imdb_rating: float | None
