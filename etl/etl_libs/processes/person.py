import logging

from etl_libs.extractors.person import PersonExtractor
from etl_libs.loaders.loader import ElasticsearchLoader
from etl_libs.processes.base import BaseETLProcess
from etl_libs.transformers.person import PersonTransformer

logger = logging.getLogger(__name__)


class PersonETLProcess(BaseETLProcess):
    TABLES = ("film_work", "person",)
    INDEXES_MAPPING = {"film_work": "movies", "person": "persons"}
    MAIN_TABLE = "person"
    EXTRACTOR_CLASS = PersonExtractor
    TRANSFORMER_CLASS = PersonTransformer
    LOADER_CLASS = ElasticsearchLoader

    def __init__(self, pg_dsn: dict, es_dsn: str):
        self.extractor = self.EXTRACTOR_CLASS(pg_dsn)
        self.transformer = self.TRANSFORMER_CLASS()
        self.loader = self.LOADER_CLASS(es_dsn)
        super().__init__()
