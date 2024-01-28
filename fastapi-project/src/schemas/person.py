from pydantic.main import BaseModel
from pydantic.types import UUID4


class PersonSchema(BaseModel):
    """
    Response model for Person object.
    This class contains info we return to a user.
    """
    uuid: UUID4
    full_name: str
    films: list | None
