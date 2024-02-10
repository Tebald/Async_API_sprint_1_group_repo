from http import HTTPStatus
from typing import List, Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import UUID4
from src.api.pagination import Page
from src.api.validators import check_params
from src.schemas import PersonSchema
from src.schemas.films import FilmShort
from src.services import FilmsService, get_films_service
from src.services.persons import PersonsService, get_persons_service

router = APIRouter()


@router.get(
    path='/search',
    response_model=Page[PersonSchema],
    summary='Search for a person',
    description='Full-text person search',
    response_description='List of people',
)
async def search_persons(
    query: Annotated[str, Query('', description="Person's name for searching", min_length=1)],
    person_service: PersonsService = Depends(get_persons_service),
):
    """
    Search person by full_name.

    Available options:
    - Search by full_name.
    """
    search = {'field': 'full_name', 'value': query}
    params = check_params()
    persons, total = await person_service.get_many(
        search=search, page_number=params.page, size=params.size
    )
    if not persons:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    res = [person.dict() for person in persons]
    return Page.create(items=res, total=total, params=params)


@router.get(
    path='/{person_id}',
    response_model=PersonSchema,
    summary='Information about a person',
    description='Search a person by id',
    response_description='Name and filmography',
)
async def person_details(person_id: UUID4, person_service: PersonsService = Depends(get_persons_service)):
    """
    Returns info regarding a Person, found by person_id.
    """
    person = await person_service.get_by_id(str(person_id))
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    return PersonSchema.parse_obj(person)


@router.get(
    path='/{person_id}/film',
    response_model=List[FilmShort],
    summary='Filmography information',
    description='Filmography',
    response_description='Name and imdb_rating of films',
)
async def person_films(
    person_id: UUID4,
    person_service: PersonsService = Depends(get_persons_service),
    films_service: FilmsService = Depends(get_films_service),
):
    """
    Returns a list of films associated with a Person.
    """
    person = await person_service.get_by_id(str(person_id))
    if not person or not person.films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    film_ids = [film['id'] for film in person.films]

    films = await films_service.get_by_ids(film_ids)
    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='Not Found')

    return [film.dict() for film in films]
