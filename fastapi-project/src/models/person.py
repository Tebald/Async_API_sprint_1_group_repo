from pydantic.fields import Field

from .base_orjson import BaseOrjsonModel


class Person(BaseOrjsonModel):
    """
    Class to store data received from elastic.
    index: persons
    """

    uuid: str = Field(..., alias="id")
    full_name: str
    films: list | None
