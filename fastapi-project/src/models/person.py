import orjson
from pydantic import BaseModel


def orjson_dumps(v, *, default):
    return orjson.dumps(v, default=default).decode()


class Person(BaseModel):
    """
    Class to store data received from elastic.
    index: persons
    """
    id: str
    full_name: str
    films: list | None

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps
