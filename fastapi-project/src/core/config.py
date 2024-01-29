import os
from logging import config as logging_config

from core.logger import LOGGING

from .api_settings import settings

# Применяем настройки логирования
logging_config.dictConfig(LOGGING)

# Название проекта. Используется в Swagger-документации
PROJECT_NAME = settings.project_name

# Настройки Redis
REDIS_HOST = settings.redis_host
REDIS_PORT = settings.redis_port

# Настройки Elasticsearch
ES_HOST = settings.es_host
ES_PORT = settings.es_port

# Корень проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
