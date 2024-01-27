from pydantic.main import BaseModel
from pydantic.types import UUID4


class GenreSchema(BaseModel):
    """
    Class to store data received from elastic.
    index: genres
    """
    uuid: UUID4
    name: str
