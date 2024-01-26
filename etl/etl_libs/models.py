from typing import Optional

from pydantic import BaseModel, field_validator


class BaseID(BaseModel):
    id: str


class ActorInline(BaseID):
    name: str


class WriterInline(BaseID):
    name: str


class DirectorInline(BaseID):
    name: str


class FilmworkInline(BaseID):
    roles: list[str]


class GenreInline(BaseID):
    name: str


class FilmworkModel(BaseID):
    title: str
    description: Optional[str]
    imdb_rating: float
    genre: list[GenreInline]
    actors_names: list[str]
    writers_names: list[str]
    directors_names: list[str]
    actors: list[ActorInline]
    writers: list[WriterInline]
    directors: list[DirectorInline]

    @field_validator("imdb_rating", mode="before")
    def validate_imdb_rating(cls, value):
        return value or 0.0


class PersonModel(BaseID):
    full_name: str
    films: Optional[list[FilmworkInline]]


class GenreModel(BaseID):
    name: str
    description: Optional[str]
