from http import HTTPStatus
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_pagination import Page, paginate
from fastapi_pagination.api import resolve_params

from schemas import FilmSchema, FilmShort
from services import (FilmsService, GenresService, get_films_service,
                      get_genres_service)

router = APIRouter()


@router.get('/', response_model=Page[FilmShort])
async def list_of_films(
        film_service: FilmsService = Depends(get_films_service),
        genre_service: GenresService = Depends(get_genres_service),
        sort: str = Query(
            '-imdb_rating',
            description="Sort by field, prefix '-' for descending order",
            regex="^-?imdb_rating$"
        ),
        genre: Optional[str] = Query(None, description="Genre UUID for filtering"),
):

    if genre:
        genre_obj = await genre_service.get_by_id(genre)
        genre_name = genre_obj.name if genre_obj is not None else 'null'
    else:
        genre_name = None

    params = resolve_params()

    films, total = await film_service.get_all_items(sort=sort, genre=genre_name, page_number=params.page, page_size=params.size)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    res = [FilmShort(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating) for film in films]
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
    films = await film_service.search_items(query)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    res = [FilmShort(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating) for film in films]
    return paginate(res)
