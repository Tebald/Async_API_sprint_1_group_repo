import pytest_asyncio


@pytest_asyncio.fixture(name='prepare_films_data_factory')
def prepare_films_data_factory(request):
    def _prepare_data_func(name):
        if name == "es_single_film":
            return request.getfixturevalue("es_single_film")()
        elif name == "es_films_search_data":
            return request.getfixturevalue("es_films_search_data")()
        else:
            raise ValueError(f"Unknown fixture: {name}")

    return _prepare_data_func


@pytest_asyncio.fixture(name='prepare_genres_data_factory')
def prepare_genres_data_factory(request):
    def _prepare_data_func(name):
        if name == "es_list_genres":
            return request.getfixturevalue("es_list_genres")()
        elif name == "es_single_genre":
            return request.getfixturevalue("es_single_genre")()
        else:
            raise ValueError(f"Unknown fixture: {name}")

    return _prepare_data_func


@pytest_asyncio.fixture(name='prepare_search_data_factory')
def prepare_search_data_factory(request):
    def _prepare_data_func(name):
        if name == "es_films_search_data":
            return request.getfixturevalue("es_films_search_data")()
        if name == "es_persons_search_data":
            return request.getfixturevalue("es_persons_search_data")()

        raise ValueError(f"Unknown fixture: {name}")

    return _prepare_data_func
