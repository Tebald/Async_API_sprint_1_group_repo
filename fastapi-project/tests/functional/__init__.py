from .settings import (genres_test_settings, movies_test_settings,  # noqa
                       persons_test_settings)

test_index_settings = movies_test_settings or genres_test_settings or persons_test_settings
