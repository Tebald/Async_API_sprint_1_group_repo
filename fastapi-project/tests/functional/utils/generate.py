import uuid
from typing import Optional, Any


def _actors_stub():
    return [
        {'id': 'ef86b8ff-3c82-4d31-ad8e-72b69f4e3f95', 'name': 'Ann'},
        {'id': 'fb111f22-121e-44a7-b78f-b19191810fbf', 'name': 'Bob'},
    ]


def _writers_stub():
    return [
        {'id': 'caf76c67-c0fe-477e-8766-3ab3ff2574b5', 'name': 'Ben'},
        {'id': 'b45bd7bc-2e16-46d5-b125-983d356768c6', 'name': 'Howard'},
    ]


def _directors_stub():
    return [
        {'id': '4f146ad7-ecbc-4361-b3b5-d15fc51a310b', 'name': 'Stan'},
        {'id': 'cf27fd33-b5d7-4769-a74b-81ea255f07ee', 'name': 'Bill'},
    ]


def _genres_stub():
    return [
        {'id': '2b966832-e6b7-48dd-9617-ab4065480917', 'name': 'Comedy'},
        {'id': '544b5e68-584a-4eb4-80c5-553bcf024e1b', 'name': 'Drama'},
    ]


def generate_film_data(
    title_prefix: str,
    count: int,
    genres: Optional[list[dict]] = None,
    directors: Optional[list[dict]] = None,
    actors: Optional[list[dict]] = None,
    writers: Optional[list[dict]] = None,
) -> list[dict[str, Any]]:
    """Generate list of films.

    All films will be generated with the same attributes.

    :param title_prefix: A prefix for movie title. Title will be '{title_prefix} Movie {serial movie number}'.
    :param count: A count of movies.
    :param genres: Optional. A list of genres. Each genre must be in format {'id': genre_id, 'name': genre_name}.
    :param directors: Optional. A list of directors. Each director must be in format {'id': director_id, 'name': director_name}.
    :param actors: Optional. A list of actors. Each actor must be in format {'id': actor_id, 'name': actor_name}.
    :param writers: Optional. A list of writers. Each writer must be in format {'id': writer_id, 'name': writer_name}

    :return: A list of generated films.
    """
    result = [
        {
            'id': str(uuid.uuid4()),
            'imdb_rating': 8.5 + i % 2,
            'genre': genres if genres else _genres_stub(),
            'title': f'{title_prefix} Movie {i}',
            'description': f'{title_prefix} themed movie',
            'directors': directors or _directors_stub(),
            'actors': actors or _actors_stub(),
            'writers': writers or _writers_stub(),
            'directors_names': [director['name'] for director in directors or _directors_stub()],
            'actors_names': [actor['name'] for actor in actors or _actors_stub()],
            'writers_names': [writer['name'] for writer in writers or _writers_stub()],
        }
        for i in range(count)
    ]
    return result
