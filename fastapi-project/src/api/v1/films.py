from http import HTTPStatus
from typing import Optional, Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import UUID4

from src.api.pagination import Page
from src.api.validators import check_params
from src.schemas import FilmSchema, FilmShort
from src.services import FilmsService, get_films_service

router = APIRouter()


@router.get('/', response_model=Page[FilmShort])
async def list_of_films(
    film_service: FilmsService = Depends(get_films_service),
    sort: str = Query(
        '-imdb_rating', description="Sort by field, prefix '-' for descending order", regex='^-?imdb_rating$'
    ),
    genre: Optional[str] = Query(None, description='Genre UUID for filtering'),
):
    params = check_params()

    films, total = await film_service.get_many(sort=sort, genre=genre, page_number=params.page, size=params.size)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    res = [FilmShort.parse_obj(film) for film in films]
    return Page.create(items=res, total=total, params=params)


@router.get('/{film_id}', response_model=FilmSchema)
async def film_details(uuid: UUID4, film_service: FilmsService = Depends(get_films_service)):
    film = await film_service.get_one(str(uuid))
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    return FilmSchema.parse_obj(film)


@router.get('/search/', response_model=Page[FilmShort])
async def search_films(
    query: Annotated[str, Query('', description='Film title for searching', min_length=1)],
    film_service: FilmsService = Depends(get_films_service),
):
    search_field = 'title'
    params = check_params()
    films, total = await film_service.get_many(
        search_query=query, search_field=search_field, page_number=params.page, size=params.size
    )

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    res = [film.dict() for film in films]
    return Page.create(items=res, total=total, params=params)
