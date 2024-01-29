from pydantic import BaseModel
from pydantic.fields import Field

from models.genre import Genre

from .base_orjson import BaseOrjsonModel


class PersonFromElastic(BaseModel):
    uuid: str = Field(..., alias="id")
    full_name: str = Field(..., alias="name")


class Film(BaseOrjsonModel):
    """
    Class to store data received from elastic.
    index: movies
    """
    uuid: str = Field(alias='id')
    title: str
    description: str | None
    genre: list[Genre] | None
    imdb_rating: float | None
    actors: list[PersonFromElastic]
    writers: list[PersonFromElastic]
    directors: list[PersonFromElastic]
