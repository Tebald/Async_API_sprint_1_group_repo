from functools import lru_cache

from pydantic import BaseSettings, Field


@lru_cache
def get_settings():
    return Settings()


class Settings(BaseSettings):
    """
    Class to store fastapi project settings.
    """
    project_name: str = Field('Read-only API for online-cinema', env='API_PROJECT_NAME')
    redis_host: str = Field('127.0.0.1', env='REDIS_HOST')
    redis_port: int = Field(6379, env='REDIS_PORT')
    es_host: str = Field('127.0.0.1', env='ES_HOST')
    es_port: int = Field(9200, env='ES_PORT')
    log_format: str = Field('%(asctime)s - %(name)s - %(levelname)s - %(message)s', env='API_LOG_FORMAT')
    log_default_handlers: list = Field(['console', ], env='API_LOG_DEFAULT_HANDLERS')
    console_log_lvl: str = Field('DEBUG', env='API_CONSOLE_LOG_LVL')
    loggers_handlers_log_lvl: str = Field('INFO', env='API_LOGGERS_HANDLERS_LOG')
    unicorn_error_log_lvl: str = Field('INFO', env='API_UNICORN_ERROR_LOG_LVL')
    unicorn_acess_log_lvl: str = Field('INFO', env='API_UNICORN_ACCESS_LOG_LVL')
    root_log_lvl: str = Field('INFO', env='API_ROOT_LOG_LVL')

    class Config:
        env_file = '../../.env'
