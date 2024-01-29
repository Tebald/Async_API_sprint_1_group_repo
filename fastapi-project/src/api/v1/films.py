from http import HTTPStatus
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_pagination.api import AbstractParams, resolve_params

from src.api.pagination import Page
from src.schemas import FilmSchema, FilmShort
from src.services import FilmsService, get_films_service

router = APIRouter()


def check_params() -> AbstractParams:
    params = resolve_params()

    if params.page * params.size > 10000:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='Amount of entries > 10k is not supported. Please try to use api/v1/films/search endpoint.'
        )
    return params


@router.get('/', response_model=Page[FilmShort])
async def list_of_films(
        film_service: FilmsService = Depends(get_films_service),
        sort: str = Query(
            '-imdb_rating',
            description="Sort by field, prefix '-' for descending order",
            regex="^-?imdb_rating$"
        ),
        genre: Optional[str] = Query(None, description="Genre UUID for filtering"),
):
    params = check_params()

    films, total = await film_service.get_all_items(
        sort=sort, genre=genre, page_number=params.page, page_size=params.size
    )

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    res = [FilmShort(uuid=film.uuid, title=film.title, imdb_rating=film.imdb_rating) for film in films]
    return Page.create(items=res, total=total, params=params)


@router.get('/{film_id}', response_model=FilmSchema)
async def film_details(uuid: str, film_service: FilmsService = Depends(get_films_service)):
    film = await film_service.get_by_id(uuid)
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    return FilmSchema(
        uuid=film.uuid,
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
    params = check_params()
    films, total = await film_service.search_items(search_query=query, page_number=params.page, page_size=params.size)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Nothing found')

    res = [FilmShort(uuid=film.uuid, title=film.title, imdb_rating=film.imdb_rating) for film in films]
    return Page.create(items=res, total=total, params=params)
