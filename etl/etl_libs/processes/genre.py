import logging

from etl_libs.extractors.genre import GenreExtractor
from etl_libs.loaders.loader import ElasticsearchLoader
from etl_libs.processes.base import BaseETLProcess
from etl_libs.transformers.genre import GenreTransformer

logger = logging.getLogger(__name__)


class GenreETLProcess(BaseETLProcess):
    TABLES = ("film_work", "genre",)
    INDICES_MAPPING = {"film_work": "movies", "genre": "genres"}
    MAIN_TABLE = "genre"
    EXTRACTOR_CLASS = GenreExtractor
    TRANSFORMER_CLASS = GenreTransformer
    LOADER_CLASS = ElasticsearchLoader

    def __init__(self, postgres_dsl: dict, es_host: str):
        self.extractor = self.EXTRACTOR_CLASS(postgres_dsl)
        self.transformer = self.TRANSFORMER_CLASS()
        self.loader = self.LOADER_CLASS(es_host)
        super().__init__()
