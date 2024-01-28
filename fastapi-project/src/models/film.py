import orjson
from pydantic import BaseModel
from pydantic.fields import Field

from models.genre import Genre


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class PersonFromElastic(BaseModel):
    uuid: str = Field(..., alias="id")
    full_name: str = Field(..., alias="name")


class Film(BaseModel):
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

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps
