from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.transfer import TransferService, get_transfer_service
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
            response_model=List[Person],
            summary="Поиск персон",
            description="Полнотекстовый поиск по персонам",
            response_description="Имя и список фильмов, в которых принимал участие человек")
async def list_of_persons(
        person_service: TransferService = Depends(get_transfer_service)) -> list:
    """
    Returns a list of all persons.
    :param person_service:
    :return:
    """
    persons = await person_service.get_all_items(index='persons')
    if not persons:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='List of persons is empty')

    return [Person(uuid=person.id, full_name=person.full_name, films=person.films) for person in persons]


@router.get(path='/{person_id}',
            response_model=Person,
            summary="Информация о персоне",
            description="Поиск персоны по id",
            response_description="Имя и список фильмов, в которых принимал участие человек")
async def person_details(
        person_id: str,
        person_service: TransferService = Depends(get_transfer_service)) -> Person:
    """
    Returns info regarding a Person, found by person_id.
    :param person_id:
    :param person_service:
    :return:
    """
    person = await person_service.get_by_id(object_id=person_id, index='persons')
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    return Person(uuid=person.id, full_name=person.full_name, films=person.films)


@router.get(path='/{person_id}/film',
            response_model=List[FilmShort],
            summary="Информация о фильмографии",
            description="Фильмография персоны",
            response_description="Название и рейтинг фильмов, в которых принимал участие человек")
async def person_films(
        person_id: str,
        person_service: TransferService = Depends(get_transfer_service)) -> List[FilmShort]:
    """
    Returns a list of films associated with a Person.
    :param person_id:
    :param person_service:
    :return:
    """
    person = await person_service.get_by_id(object_id=person_id, index='persons')

    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    film_ids = get_films_ids(person.films)

    filter_body = {
        'query': {
            'bool': {
                'must': {'match_all': {}},
                'filter': {'ids': {'values': film_ids}}
            }
        }
    }
    films = await person_service.get_all_items(index='movies', filter_body=filter_body)

    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='associated films not found')

    return [FilmShort(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating) for film in films]
