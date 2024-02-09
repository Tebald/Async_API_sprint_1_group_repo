from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import UUID4

from src.schemas import GenreSchema
from src.services import GenresService, get_genres_service

router = APIRouter()


@router.get('/',
            response_model=List[GenreSchema],
            summary='All genres',
            description='List of all genres',
            response_description='List of genres')
async def list_of_genres(genre_service: GenresService = Depends(get_genres_service)):
    """
    Returns a list of all genres.
    """
    genres = await genre_service.get_many()
    if not genres:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    return [GenreSchema(uuid=genre.uuid, name=genre.name) for genre in genres]


@router.get('/{genre_id}',
            response_model=GenreSchema,
            summary='Genre info',
            description='Search a genre by id',
            response_description='UUID and name')
async def genre_details(genre_id: UUID4, genre_service: GenresService = Depends(get_genres_service)):
    genre = await genre_service.get_one(str(genre_id))
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    return GenreSchema.parse_obj(genre)
