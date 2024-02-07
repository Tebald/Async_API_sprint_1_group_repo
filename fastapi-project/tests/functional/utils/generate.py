import uuid


def generate_film_data(genre_id: str, genre_name: str, title_prefix: str, count: int):
    return [{
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
            {'id': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'name': 'Bob'}
        ],
        'writers': [
            {'id': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'name': 'Ben'},
            {'id': 'b45bd7bc-2e16-46d5-b125-983d356768c6', 'name': 'Howard'}
        ],
    } for i in range(count)]
