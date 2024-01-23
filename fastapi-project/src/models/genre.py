import orjson

from pydantic import BaseModel


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class Genre(BaseModel):
    """
    Class to store data received from elastic.
    index: genres
    """
    id: str
    name: str

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps
