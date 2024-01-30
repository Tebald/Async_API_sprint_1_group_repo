from .film import Film  # noqa
from .genre import Genre  # noqa
from .person import Person  # noqa

ElasticModel = Film | Genre | Person
