import uuid

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

        action_films = generate_film_data(action_genre['id'], action_genre['name'], 'Action', 30)
        sci_fi_films = generate_film_data(sci_fi_genre['id'], sci_fi_genre['name'], 'Sci-Fi', 30)

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


@pytest_asyncio.fixture(name='es_single_person_data')
def es_single_person_data():
    async def inner():
        """
        Function to prepare data for testing '/api/v1/persons/{person_id}' endpoint.
        :return:
        """
        es_data = {
            "id": "5ad2a0ae-14b4-4204-9516-a83fba77e6e8",
            "full_name": "Mike Wazowski",
            "films": [
                {"id": str(uuid.uuid4()), "roles": ["writer", "actor"]},
                {"id": str(uuid.uuid4()), "roles": ["director"]},
                {"id": str(uuid.uuid4()), "roles": ["actor"]},
                {"id": str(uuid.uuid4()), "roles": ["writer"]}
            ],
        }

        bulk_query = [{'_index': 'persons', '_id': es_data['id'], '_source': es_data}]
        return bulk_query

    return inner
