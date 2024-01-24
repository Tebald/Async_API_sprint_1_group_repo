from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

# from services.film import FilmService, get_film_service
from services.transfer import TransferService, get_transfer_service

router = APIRouter()


class Film(BaseModel):
    """
    Response model for Film object.
    This class contains info we return to a user.
    """
    uuid: str
    title: str
    description: str | None
    genres: list | None
    imdb_rating: float | None
    actors: list
    writers: list
    directors: list


class FilmShort(BaseModel):
    """
    Short version of Film object.
    It is used to form a list of films.
    """
    uuid: str
    title: str
    imdb_rating: float | None


@router.get('/search')
async def list_of_films(film_service: TransferService = Depends(get_transfer_service)) -> list:
    # todo: use a different method to retrieve all films from elastic.
    films = await film_service.get_all_items(index='movies')
    if not films:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    return [FilmShort(uuid=film.id, title=film.title, imdb_rating=film.imdb_rating) for film in films]


@router.get('/{film_id}', response_model=Film)
async def film_details(film_id: str, film_service: TransferService = Depends(get_transfer_service)) -> Film:
    film = await film_service.get_by_id(object_id=film_id, index='movies')
    if not film:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail='film not found')

    # Перекладываем данные из models.Film в Film
    # Данное решение позволяет скрыть некоторые данные возвращаемые эластиком,
    # которые, возможно не стоит показывать клиентам.
    return Film(
        uuid=film.id,
        title=film.title,
        description=film.description,
        genres=film.genre,
        imdb_rating=film.imdb_rating,
        actors=film.actors,
        writers=film.writers,
        directors=film.directors
    )
