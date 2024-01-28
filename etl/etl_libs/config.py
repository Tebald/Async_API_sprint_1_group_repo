from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = '../../.env'


@lru_cache
def get_settings():
    return Settings()


class PostgresSettings(BaseSettings):
    db: str
    user: str
    password: str
    host: str
    port: int

    model_config = SettingsConfigDict(env_prefix='PG_', env_file=ENV_FILE, env_file_encoding='utf-8')


class ElasticSettings(BaseSettings):
    host: str
    port: int
    indexes: list

    model_config = SettingsConfigDict(env_prefix='ES_', env_file=ENV_FILE, env_file_encoding='utf-8')


class LoggerSettings(BaseSettings):
    path: str
    level: str
    format: str

    model_config = SettingsConfigDict(env_prefix='LOG_', env_file=ENV_FILE, env_file_encoding='utf-8')


class Settings(BaseSettings):
    postgres: PostgresSettings = PostgresSettings()
    elastic: ElasticSettings = ElasticSettings()
    logger: LoggerSettings = LoggerSettings()
    interval: int

    model_config = SettingsConfigDict(env_file=ENV_FILE, env_file_encoding='utf-8', extra='ignore')
