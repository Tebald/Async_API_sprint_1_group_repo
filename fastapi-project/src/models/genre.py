from pydantic.fields import Field

from .base_orjson import BaseOrjsonModel


class Genre(BaseOrjsonModel):
    """
    Class to store data received from elastic.
    index: genres
    """

    uuid: str = Field(..., alias='id')
    name: str
