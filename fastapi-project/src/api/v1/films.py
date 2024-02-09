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
    """
    Return a list of films.
    Available options:
    - Sort by rating.
    - Filter by genre uuid.
    - Results pagination.
    """
    params = check_params()
    filter_ = {'field': 'genre.id', 'value': genre} if genre else None
    films, total = await film_service.get_many(sort=sort, filters=[filter_], page_number=params.page, size=params.size)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    res = [FilmShort.parse_obj(film) for film in films]
    return Page.create(items=res, total=total, params=params)


@router.get('/{film_id}', response_model=FilmSchema, description='Return details about film by UUID')
async def film_details(film_id: UUID4, film_service: FilmsService = Depends(get_films_service)):
    film = await film_service.get_by_id(str(film_id))
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    return FilmSchema.parse_obj(film)


@router.get('/search/', response_model=Page[FilmShort])
async def search_films(
    query: Annotated[str, Query('', description='Film title for searching', min_length=1)],
    film_service: FilmsService = Depends(get_films_service),
):
    """
    Return a list of films with the most relevant title.
    Available options:
    - Search by film title.
    - Results pagination.
    """
    search = {'field': 'title', 'value': query}
    params = check_params()
    films, total = await film_service.get_many(
        search=search, page_number=params.page, size=params.size
    )

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    res = [film.dict() for film in films]
    return Page.create(items=res, total=total, params=params)
