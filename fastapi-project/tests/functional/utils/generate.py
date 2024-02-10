import uuid
from typing import Any


def generate_film_data(genre_id: str, genre_name: str, title_prefix: str, count: int, actor_uuid=None) -> list[dict[str, Any]]:
    return [
        {
            'id': str(uuid.uuid4()),
            'imdb_rating': 8.5 + i % 2,
            'genre': [{'id': genre_id, 'name': genre_name}],
            'title': f'{title_prefix} Movie {i}',
            'description': f'{title_prefix} themed movie',
            'directors_names': ['Stan', 'Bill'],
            'actors_names': ['Ann', 'Bob'],
            'writers_names': ['Ben', 'Howard'],
            'directors': [
                {'id': str(uuid.uuid4()), 'name': 'Stan'},
                {'id': str(uuid.uuid4()), 'name': 'Bill'},
            ],
            'actors': [
                {'id': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95', 'name': 'Ann'},
                {'id': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'name': 'Bob'},
            ],
            'writers': [
                {'id': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'name': 'Ben'},
                {'id': 'b45bd7bc-2e16-46d5-b125-983d356768c6', 'name': 'Howard'},
            ],
        }
        for i in range(count)
    ]


def generate_person_data():
    person_data = {
        'id': '5ad2a0ae-14b4-4204-9516-a83fba77e6e8',
        'full_name': 'Mike Wazowski',
        'films': [
            {'id': '89c192d9-53fb-492a-8f37-11ccbe9158ad', 'roles': ['writer', 'actor']},
            {'id': str(uuid.uuid4()), 'roles': ['director']},
            {'id': str(uuid.uuid4()), 'roles': ['actor']},
            {'id': str(uuid.uuid4()), 'roles': ['writer']},
        ],
    }
    return person_data


def generate_person_data_1():
    person_data = {
        'id': '9baafca4-d93f-4bae-af94-52a6477a3627',
        'full_name': 'Kai Angel',
        'films': [
            {'id': '4decf576-cc31-4104-a1ee-97091b95d340', 'roles': ['director']},
            {'id': '29116349-27fa-4658-a16f-0b54662222f2', 'roles': ['director']},
        ],
    }
    return person_data


def generate_person_data_without_films():
    person_data = {'id': '7847c001-1040-4b4c-b846-51376517ff08', 'full_name': 'Ralph Sergeev', 'films': []}
    return person_data


def generate_film_data_for_persons_film():
    films = generate_film_data(
        genre_id=str(uuid.uuid4()), genre_name='Freestyle', title_prefix='PersonsFilms', count=3
    )
    p1 = generate_person_data()
    films[0]['id'] = '89c192d9-53fb-492a-8f37-11ccbe9158ad'
    films[0]['actors'] = [{'id': p1['id'], 'name': p1['full_name']}]
    films[0]['writers'].append({'id': p1['id'], 'name': p1['full_name']})
    films[0]['actors_names'] = [p1['full_name']]
    films[0]['writers_names'].append(p1['full_name'])

    p2 = generate_person_data_1()
    films[1]['id'] = '29116349-27fa-4658-a16f-0b54662222f2'
    films[1]['directors'] = [{'id': p2['id'], 'name': p2['full_name']}]
    films[1]['directors_names'] = [p2['full_name']]
    films[2]['id'] = '4decf576-cc31-4104-a1ee-97091b95d340'
    films[2]['directors'].append({'id': p2['id'], 'name': p2['full_name']})
    films[2]['directors_names'].append(p2["full_name"])
    return films


def bulk_query_from_data(index, data):
    if type(data) is dict:
        return [{'_index': index, '_id': data['id'], '_source': data}]
    else:
        return [{'_index': index, '_id': source['id'], '_source': source} for source in data]
