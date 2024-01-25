from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from services.transfer import TransferService, get_transfer_service


router = APIRouter()


class Person(BaseModel):
    """
    Response model for Person object.
    This class contains info we return to a user.
    """
    uuid: str
    full_name: str
    films: list | None


@router.get(path='/search', response_model=List[Person], summary="Поиск персон",
            description="Полнотекстовый поиск по персонам",
            response_description="Имя и список фильмов, в которых принимал участие человек",
            tags=['Полнотекстовый поиск'])
async def list_of_persons(person_service: TransferService = Depends(get_transfer_service)) -> list:
    """
    Returns a list of all persons.
    :param person_service:
    :return:
    """
    persons = await person_service.get_all_items(index='persons')
    if not persons:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='List of persons is empty')

    return [Person(uuid=person.id, full_name=person.name, films=person.films) for person in persons]


@router.get(path='/{person_id}', response_model=Person, summary="Информация о персоне",
            description="Поиск персоны по id",
            response_description="Имя и список фильмов, в которых принимал участие человек",
            tags=['person'])
async def genre_details(person_id: str, person_service: TransferService = Depends(get_transfer_service)) -> Person:
    """
    Returns info regarding a Person, found by person_id.
    :param person_id:
    :param person_service:
    :return:
    """
    person = await person_service.get_by_id(object_id=person_id, index='persons')
    if not person:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='person not found')

    return Person(uuid=person.id, full_name=person.name, films=person.films)
