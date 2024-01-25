from pydantic.main import BaseModel


class PersonForFilm(BaseModel):
    id: str
    name: str


class Film(BaseModel):
    """
    Response model for Film object.
    This class contains info we return to a user.
    """
    uuid: str
    title: str
    description: str | None
    genres: list[str] | None
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
