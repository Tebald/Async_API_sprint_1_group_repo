from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException

from schemas import GenreSchema
from services import GenresService, get_genres_service

router = APIRouter()


@router.get('/',
            response_model=List[GenreSchema],
            summary="All genres",
            description="List of all genres",
            response_description="List of genres")
async def list_of_genres(genre_service: GenresService = Depends(get_genres_service)):
    """
    Returns a list of all genres.
    """
    genres, _ = await genre_service.get_all_items()
    if not genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    return [GenreSchema(uuid=genre.uuid, name=genre.name) for genre in genres]


@router.get('/{genre_id}',
            response_model=GenreSchema,
            summary="Genre info",
            description="Search a genre by id",
            response_description="UUID and name")
async def genre_details(uuid: str, genre_service: GenresService = Depends(get_genres_service)):
    genre = await genre_service.get_by_id(uuid)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    return GenreSchema(uuid=genre.uuid, name=genre.name)
