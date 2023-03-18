from abc import ABC
from typing import Generator

from dataclasses import dataclass
import logging

from arquivo import Arquivo
from data import Repository, Fact

logger = logging.getLogger(__name__)


@dataclass
class ExtractionParams:
    """Params for an extraction job"""

    month: int
    day: int
    start_year: int
    end_year: int
    extractors: list


class Extractor(ABC):
    def __init__(
        self, arquivo: Arquivo, repository: Repository, params: ExtractionParams
    ):
        self.arquivo = arquivo
        self.repository = repository
        self.params = params

    applicable_time_span: tuple[int, int] = None

    async def extract(self) -> Generator[Fact, None, None]:
        raise NotImplementedError("Abstract Method")


class ExtractionJob:
    """Params for an extraction job"""

    def __init__(self, arquivo: Arquivo, repo: Repository, params: ExtractionParams):
        self.params = params
        self.repo = repo
        self.arquivo = arquivo

    async def run(self):
        logger.info(f"Extracting facts for {self.params}")

        extractors = (
            extractor_cls(self.arquivo, self.repo, self.params)
            for extractor_cls in self.params.extractors
        )

        for extractor in extractors:
            facts = [fact async for fact in extractor.extract()]
            self.repo.insert_facts(facts)
