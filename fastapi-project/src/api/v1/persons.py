from http import HTTPStatus
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_pagination import Page, paginate
from pydantic import BaseModel

from services import FilmsService, get_films_service
from services.persons import PersonsService, get_persons_service

from .films import FilmShort

router = APIRouter()


class Person(BaseModel):
    """
    Response model for Person object.
    This class contains info we return to a user.
    """
    uuid: str
    full_name: str
    films: list | None


def get_films_ids(films: list) -> list:
    """
    Retuns list of uuids.
    :param films: [
        {
          "id": "c20c249f-91ac-4c6a-9afe-f1c85aa9b277",
          "roles": ["director"]
        },
        {
          "id": "00af52ec-9345-4d66-adbe-50eb917f463a",
          "roles": ["director", "writer"]
        }
    ]
    :return:
    """
    result = []
    for film in films:
        result.append(film['id'])
    return result


@router.get(path='/search',
            response_model=Page[Person],
            summary="Search for a person",
            description="Full-text person search",
            response_description="List of people")
async def search_persons(
        person_service: PersonsService = Depends(get_persons_service),
        query: Optional[str] = Query('', description="Person's name for searching")):
    """
    Returns a list of persons depends on filter.
    :param person_service:
    :param query:
    :return:
    """
    persons = await person_service.search_items(query)
    if not persons:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='List of persons is empty')

    res = [Person(uuid=person.id, full_name=person.full_name, films=person.films) for person in persons]

    return paginate(res)


@router.get(path='/{person_id}',
            response_model=Person,
            summary="Information about a person",
            description="Search a person by id",
            response_description="Name and filmography")
async def person_details(
        person_id: str,
        person_service: PersonsService = Depends(get_persons_service)) -> Person:
    """
    Returns info regarding a Person, found by person_id.
    :param person_id:
    :param person_service:
    :return:
    """
    person = await person_service.get_by_id(object_id=person_id)
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    return Person(uuid=person.id, full_name=person.full_name, films=person.films)


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
    :param person_id:
    :param person_service:
    :return:
    """
    person = await person_service.get_by_id(object_id=person_id)

    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    film_ids = [film['id'] for film in person.films]

    films = await films_service.get_items_by_ids(film_ids)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='associated films not found')

    return [FilmShort(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating) for film in films]
