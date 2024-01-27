from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from pydantic.types import UUID4

from schemas import GenreSchema
from services import GenresService, get_genres_service

router = APIRouter()


class Genre(BaseModel):
    """
    Response model for Genre object.
    This class contains info we return to a user.
    """
    uuid: UUID4
    name: str


@router.get('/', response_model=List[GenreSchema])
async def list_of_genres(genre_service: GenresService = Depends(get_genres_service)):
    """
    Returns a list of all genres.
    :param genre_service:
    :return:
    """
    genres = await genre_service.get_all_items()
    if not genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='List of genres is empty')

    return [GenreSchema(uuid=genre.uuid, name=genre.name) for genre in genres]


@router.get('/{genre_id}', response_model=GenreSchema)
async def genre_details(uuid: str, genre_service: GenresService = Depends(get_genres_service)):
    genre = await genre_service.get_by_id(uuid)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')

    return GenreSchema(uuid=genre.uuid, name=genre.name)
