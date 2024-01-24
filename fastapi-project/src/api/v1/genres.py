from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from pydantic.types import UUID4

from services.transfer import TransferService, get_transfer_service


router = APIRouter()


class Genre(BaseModel):
    """
    Response model for Genre object.
    This class contains info we return to a user.
    """
    uuid: UUID4
    name: str


@router.get('/', response_model=List[Genre])
async def list_of_genres(genre_service: TransferService = Depends(get_transfer_service)) -> list:
    """
    Returns a list of all genres.
    :param genre_service:
    :return:
    """
    genres = await genre_service.get_all_items(index='genres')
    if not genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='List of genres is empty')

    return [Genre(uuid=genre.id, name=genre.name) for genre in genres]


@router.get('/{genre_id}', response_model=Genre)
async def genre_details(genre_id: str, genre_service: TransferService = Depends(get_transfer_service)) -> Genre:
    genre = await genre_service.get_by_id(object_id=genre_id, index='genres')
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='genre not found')

    return Genre(uuid=genre.id, name=genre.name)
