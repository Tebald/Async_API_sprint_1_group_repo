import orjson
from pydantic import BaseModel
from pydantic.fields import Field


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
    id: str
    title: str
    description: str | None
    genre: list | None
    imdb_rating: float | None
    actors: list[PersonFromElastic]
    writers: list[PersonFromElastic]
    directors: list[PersonFromElastic]

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps
