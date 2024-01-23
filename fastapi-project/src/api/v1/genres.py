from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.transfer import TransferService, get_transfer_service


router = APIRouter()


class Genre(BaseModel):
    """
    Response model for Genre object.
    This class contains info we return to a user.
    """
    uuid: str
    name: str


@router.get('/')
async def list_of_genres(genre_service: TransferService = Depends(get_transfer_service)) -> list:
    """
    Returns a list of all genres.
    :param genre_service:
    :return:
    """
    genres = await genre_service.get_all_items(index='genres')
    if not genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='List of genres is empty')

    return [Genre(uuid=genre.id, title=genre.name) for genre in genres]
