from .film import Film  # noqa
from .genre import Genre  # noqa
from .person import Person  # noqa

from typing import TypeVar

ElasticModel = TypeVar("ElasticModel", bound=type[Film, Genre, Person])
