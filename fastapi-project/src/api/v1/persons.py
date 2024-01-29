from http import HTTPStatus
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_pagination.api import AbstractParams, resolve_params

from src.api.pagination import Page
from src.schemas import PersonSchema
from src.schemas.films import FilmShort
from src.services import FilmsService, get_films_service
from src.services.persons import PersonsService, get_persons_service

router = APIRouter()


def check_params() -> AbstractParams:
    params = resolve_params()

    if params.page * params.size > 10000:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail='Amount of entries > 10k is not supported.'
        )
    return params


@router.get(path='/search',
            response_model=Page[PersonSchema],
            summary="Search for a person",
            description="Full-text person search",
            response_description="List of people")
async def search_persons(
        person_service: PersonsService = Depends(get_persons_service),
        query: Optional[str] = Query('', description="Person's name for searching")):
    """
    Returns a list of persons depends on filter.
    """
    params = check_params()
    persons, total = await person_service.search_items(
        search_query=query,
        page_number=params.page,
        page_size=params.size)
    if not persons:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='List of persons is empty')

    res = [PersonSchema(uuid=person.uuid, full_name=person.full_name, films=person.films) for person in persons]

    return Page.create(items=res, total=total, params=params)


@router.get(path='/{person_id}',
            response_model=PersonSchema,
            summary="Information about a person",
            description="Search a person by id",
            response_description="Name and filmography")
async def person_details(
        person_id: str,
        person_service: PersonsService = Depends(get_persons_service)):
    """
    Returns info regarding a Person, found by person_id.
    """
    person = await person_service.get_by_id(object_id=person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    return PersonSchema(uuid=person.uuid, full_name=person.full_name, films=person.films)


@router.get(path='/{person_id}/film',
            response_model=List[FilmShort],
            summary="Filmography information",
            description="Filmography",
            response_description="Name and imdb_rating of films")
async def person_films(
        person_id: str,
        person_service: PersonsService = Depends(get_persons_service),
        films_service: FilmsService = Depends(get_films_service)) -> List[FilmShort]:
    """
    Returns a list of films associated with a Person.
    """
    person = await person_service.get_by_id(object_id=person_id)

    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    film_ids = [film['id'] for film in person.films]

    films = await films_service.get_items_by_ids(film_ids)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='associated films not found')

    return [FilmShort(uuid=film.uuid, title=film.title, imdb_rating=film.imdb_rating) for film in films]
