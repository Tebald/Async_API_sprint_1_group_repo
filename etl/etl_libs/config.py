from pydantic.v1 import BaseSettings


class Config(BaseSettings):
    postgres_db: str
    postgres_user: str
    postgres_password: str
    db_port: str
    db_host: str
    es_host: str
    es_indexes: list
    interval: int
    log_path: str
    log_level: str
    log_format: str
    index_jsons_dir: str
    host: str
    port: int

    class Config:
        env_file = "../.env"
        env_file_encoding = "utf-8"


settings = Config()
