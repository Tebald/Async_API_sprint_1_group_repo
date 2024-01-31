from pydantic.main import BaseModel


class GenreSchema(BaseModel):
    """
    Class to store data received from elastic.
    index: genres
    """

    uuid: str
    name: str
