import logging

from etl_libs.extractors.person import PersonExtractor
from etl_libs.loaders.loader import ElasticsearchLoader
from etl_libs.processes.base import BaseETLProcess
from etl_libs.transformers.person import PersonTransformer

logger = logging.getLogger(__name__)


class PersonETLProcess(BaseETLProcess):
    TABLES = ("film_work", "person",)
    INDICES_MAPPING = {"film_work": "movies", "person": "persons"}
    MAIN_TABLE = "person"
    EXTRACTOR_CLASS = PersonExtractor
    TRANSFORMER_CLASS = PersonTransformer
    LOADER_CLASS = ElasticsearchLoader

    def __init__(self, postgres_dsl: dict, es_host: str):
        self.extractor = self.EXTRACTOR_CLASS(postgres_dsl)
        self.transformer = self.TRANSFORMER_CLASS()
        self.loader = self.LOADER_CLASS(es_host)
        super().__init__()
