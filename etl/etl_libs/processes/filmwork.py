import logging

from etl_libs.extractors.filmwork import FilmworkExtractor
from etl_libs.loaders.loader import ElasticsearchLoader
from etl_libs.processes.base import BaseETLProcess
from etl_libs.transformers.filmwork import FilmworkTransformer

logger = logging.getLogger(__name__)


class FilmworkETLProcess(BaseETLProcess):
    TABLES = ("film_work", "genre", "person",)
    INDICES_MAPPING = {"film_work": "movies", "genre": "genres", "person": "persons"}
    MAIN_TABLE = "film_work"
    EXTRACTOR_CLASS = FilmworkExtractor
    TRANSFORMER_CLASS = FilmworkTransformer
    LOADER_CLASS = ElasticsearchLoader

    def __init__(self, postgres_dsl: dict, es_host: str):
        self.extractor = self.EXTRACTOR_CLASS(postgres_dsl)
        self.transformer = self.TRANSFORMER_CLASS()
        self.loader = self.LOADER_CLASS(es_host)
        super().__init__()
