import datetime
import uuid

import pytest_asyncio


@pytest_asyncio.fixture(name='es_films_search_data')
def es_films_search_data():

    async def inner():
        """
        Finction to prepare data for testing '/api/v1/films/search/' endpoint.
        :return:
        """
        es_data = [{
            'id': str(uuid.uuid4()),
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
        } for _ in range(60)]

        bulk_query: list[dict] = []
        for row in es_data:
            data = {'_index': 'movies', '_id': row['id']}
            data.update({'_source': row})
            bulk_query.append(data)

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
