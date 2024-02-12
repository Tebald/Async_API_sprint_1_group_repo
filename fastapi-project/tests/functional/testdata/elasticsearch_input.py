import uuid
from typing import Iterable

import pytest
import pytest_asyncio

from tests.functional.utils.generate import generate_film_data


@pytest_asyncio.fixture(name='es_single_film')
def es_single_film():
    async def inner():
        es_data = {
            'id': '95b7ddb4-1f59-4a2f-982d-65d733934b53',
            'imdb_rating': 8.5,
            'genre': [
                {'id': '812e88bd-7db1-4827-967e-53c946a602b3', 'name': 'Action'},
                {'id': 'b0585c47-e74a-4154-97f3-343fcb8b34d7', 'name': 'Sci-Fi'}],
            'title': 'The Star',
            'description': 'New World',
            'directors_names': ['Stan', 'Bill'],
            'actors_names': ['Ann', 'Bob'],
            'writers_names': ['Ben', 'Howard'],
            'directors': [
                {'id': str(uuid.uuid4()), 'name': 'Stan'},
                {'id': str(uuid.uuid4()), 'name': 'Bill'},
            ],
            'actors': [
                {'id': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95', 'name': 'Ann'},
                {'id': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'name': 'Bob'}
            ],
            'writers': [
                {'id': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'name': 'Ben'},
                {'id': 'b45bd7bc-2e16-46d5-b125-983d356768c6', 'name': 'Howard'}
            ],
        }

        query: list[dict] = []
        data = {'_index': 'movies', '_id': es_data['id']}
        data.update({'_source': es_data})
        query.append(data)

        return query

    return inner


@pytest_asyncio.fixture(name='es_films_search_data')
def es_films_search_data():

    async def inner():
        """
        Finction to prepare data for testing '/api/v1/films/search/' endpoint.
        :return:
        """
        action_genre = {'id': '812e88bd-7db1-4827-967e-53c946a602b3', 'name': 'Action'}
        sci_fi_genre = {'id': 'b0585c47-e74a-4154-97f3-343fcb8b34d7', 'name': 'Sci-Fi'}

        action_films = generate_film_data(genres=[action_genre], title_prefix='Action', count=30)
        sci_fi_films = generate_film_data(genres=[sci_fi_genre], title_prefix='Sci-Fi', count=30)

        es_data = action_films + sci_fi_films

        bulk_query = [{'_index': 'movies', '_id': film['id'], '_source': film} for film in es_data]

        return bulk_query

    return inner


@pytest_asyncio.fixture(name='es_persons_search_data')
def es_persons_search_data():
    async def inner():
        """
        Finction to prepare data for testing '/api/v1/persons/search/' endpoint.
        :return:
        """
        es_data = [{
            'id': str(uuid.uuid4()),
            'full_name': 'Nash Bridges',
            'films': [
                {'id': str(uuid.uuid4()), 'roles': ['actor']},
                {'id': str(uuid.uuid4()), 'roles': ['actor', 'writer']}
            ],
        } for _ in range(60)]

        bulk_query: list[dict] = []
        for row in es_data:
            data = {'_index': 'persons', '_id': row['id']}
            data.update({'_source': row})
            bulk_query.append(data)

        return bulk_query

    return inner


@pytest_asyncio.fixture(name='es_list_genres')
def es_list_genres():
    async def inner():
        es_data = [
            {
                'id': uuid.uuid4(),
                'name': 'Action',
                'description': 'Some description'
            } for _ in range(10)
        ]
        bulk_query: list[dict] = []
        for genre in es_data:
            data = {'_index': 'genres', '_id': genre['id']}
            data.update({'_source': genre})
            bulk_query.append(data)

        return bulk_query

    return inner


@pytest_asyncio.fixture(name='es_single_genre')
def es_single_genre():
    async def inner():
        es_data = {
            'id': 'f2998290-8ea4-48ae-a3a0-1ea43becfa9b',
            'name': 'Action',
            'description': 'Some description'
        }

        query: list[dict] = []
        data = {'_index': 'genres', '_id': es_data['id']}
        data.update({'_source': es_data})
        query.append(data)

        return query

    return inner


@pytest.fixture(name='es_person_without_films')
def es_person_without_films(from_dict_to_bulk):
    """Fixture for person with empty films."""
    person = {'id': 'd7bdcbf2-d4b4-401e-8a9e-c3022f20e565', 'full_name': 'Ralph Sergeev', 'films': []}

    bulk_query = from_dict_to_bulk(es_index='persons', data_to_bulk=person)
    return bulk_query


@pytest.fixture(name='es_person_with_two_films')
def es_person_with_two_films(from_dict_to_bulk):
    """Fixture for person with two films."""
    person = {'id': '230a5bbf-3e31-4e53-b2a1-156dc51cd070', 'full_name': 'Kai Angel', 'films': []}

    person_inline = {'id': person['id'], 'name': 'Kai Angel'}
    films = generate_film_data(title_prefix='Some Kai', count=2, directors=[person_inline])

    person['films'] = [{'id': film['id'], 'roles': 'director'} for film in films]

    bulk_person = from_dict_to_bulk(es_index='persons', data_to_bulk=person)
    bulk_films = from_dict_to_bulk(es_index='movies', data_to_bulk=films)
    bulk_result = bulk_person + bulk_films

    return bulk_result


@pytest.fixture(name='es_person_with_four_films')
def es_person_with_four_films(from_dict_to_bulk):
    """Fixture for person with four films and double role in one of them."""
    person = {'id': '5ad2a0ae-14b4-4204-9516-a83fba77e6e8', 'full_name': 'Mike Wazowski', 'films': []}
    person_inline = {'id': person['id'], 'name': 'Mike Wazowski'}
    person_roles = [['writer', 'actor'], ['director'], ['actor'], ['writer']]

    films: list[dict] = []

    # loop for every list of roles
    for roles_list in person_roles:
        # create a dict with roles.
        # like ['writer', 'actor'] -> {'writers': [person_inline], 'actors': [person_inline]}
        roles_dict = {role + 's': [person_inline] for role in roles_list}

        # create and add film to the all films
        film = generate_film_data(title_prefix='Random Mike', count=1, **roles_dict)[0]
        films.append(film)

        # add film inline to the person (Mike)
        film_inline = {'id': film['id'], 'roles': roles_list}
        person['films'].append(film_inline)

    bulk_person = from_dict_to_bulk(es_index='persons', data_to_bulk=person)
    bulk_movies = from_dict_to_bulk(es_index='movies', data_to_bulk=films)
    bulk_result = bulk_person + bulk_movies
    return bulk_result


@pytest.fixture(name='es_person_with_inexisting_films')
def es_person_with_inexisting_films(from_dict_to_bulk):
    """Data for person with two film ids but these ids are missing in elastic movie index."""

    person = {'id': 'bc029812-5ac8-4ff3-841b-c7e0efa72013', 'full_name': 'Empty Man', 'films': []}

    person_inline = {'id': person['id'], 'name': 'Empty Man'}
    films = generate_film_data(title_prefix='Missing Film', count=2, directors=[person_inline])

    person['films'] = [{'id': film['id'], 'roles': 'director'} for film in films]

    bulk_person = from_dict_to_bulk(es_index='persons', data_to_bulk=person)

    return bulk_person, 0


@pytest_asyncio.fixture(name='from_dict_to_bulk')
def from_dict_to_bulk():
    def inner(es_index: str, data_to_bulk: [dict | Iterable[dict]]) -> list[dict]:
        if isinstance(data_to_bulk, dict):
            bulk_query = [{'_index': es_index, '_id': data_to_bulk['id'], '_source': data_to_bulk}]
        else:
            bulk_query = [
                {'_index': es_index,
                 '_id': data_dict['id'],
                 '_source': data_dict}
                for data_dict in data_to_bulk
            ]

        return bulk_query

    return inner


@pytest_asyncio.fixture(name='es_single_person_bulks_valid', params=['es_person_with_four_films', 'es_person_with_two_films', 'es_person_without_films'])
def es_single_person_bulks_valid(request):
    """Fixture, containing all correct single person fixtures, used in tests."""
    fixture = request.param
    return request.getfixturevalue(fixture)
