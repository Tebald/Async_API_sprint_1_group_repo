from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel


router = APIRouter()


class Person(BaseModel):
    """
    Response model for Person object.
    This class contains info we return to a user.
    """
    uuid: str
    full_name: str
    films: list | None

