from http import HTTPStatus
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from fastapi_pagination import Page, paginate

from services import (
    FilmsService, GenresService, TransferService,
    get_genres_service, get_films_service, get_transfer_service
)
from schemas import Film, FilmShort


router = APIRouter()


@router.get('/', response_model=Page[FilmShort])
async def list_of_films(
        film_service: FilmsService = Depends(get_films_service),
        genre_service: TransferService = Depends(get_genres_service),
        sort: str = Query('-imdb_rating', description="Sort by field, prefix '-' for descending order"),
        genre: Optional[str] = Query(None, description="Genre UUID for filtering")
):
    genre_name = None
    if genre is not None:
        genre_obj = await genre_service.get_by_id(genre)
        genre_name = genre_obj.name if genre_obj is not None else 'null'
    films = await film_service.get_all_items(sort=sort, genre=genre_name)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    res = [FilmShort(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating) for film in films]
    return paginate(res)


@router.get('/{film_id}', response_model=Film)
async def film_details(uuid: str, film_service: FilmsService = Depends(get_films_service)):
    film = await film_service.get_by_id(uuid)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    # Перекладываем данные из models.Film в Film
    # Данное решение позволяет скрыть некоторые данные возвращаемые эластиком,
    # которые, возможно не стоит показывать клиентам.
    return Film(
        uuid=film.id,
        title=film.title,
        description=film.description,
        genre=film.genre,
        imdb_rating=film.imdb_rating,
        actors=film.actors,
        writers=film.writers,
        directors=film.directors
    )


@router.get('/search/', response_model=Page[FilmShort])
async def search_films(
        film_service: FilmsService = Depends(get_films_service),
        query: Optional[str] = Query('', description="Film title for searching")
):
    films = await film_service.search_films(query)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    res = [FilmShort(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating) for film in films]
    return paginate(res)
