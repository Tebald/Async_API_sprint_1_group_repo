import orjson

# Используем pydantic для упрощения работы при перегонке данных из json в объекты
from pydantic import BaseModel


def orjson_dumps(v, *, default):
    # orjson.dumps возвращает bytes, а pydantic требует unicode, поэтому декодируем
    return orjson.dumps(v, default=default).decode()


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
    actors: list
    writers: list
    directors: list

    class Config:
        # Заменяем стандартную работу с json на более быструю
        json_loads = orjson.loads
        json_dumps = orjson_dumps
