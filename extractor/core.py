import sys
from abc import ABC
from typing import Generator

from dataclasses import dataclass
import logging

from arquivo import Arquivo, VersionEntry
from data import Repository, Fact

logger = logging.getLogger(__name__)


@dataclass
class ExtractionParams:
    """Params for an extraction job"""

    month: int
    day: int | None
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
            facts = []
            async for fact in extractor.extract():
                facts.append(fact)
                if len(facts) >= 2000:
                    self.repo.insert_facts(facts)
                    facts = []

            self.repo.insert_facts(facts)


@dataclass
class ExtractionTargetURL:
    value: str
    _from: int | None
    _to: int | None

    def applicable(self, year: int):
        _from = self._from or 0
        _to = self._to or sys.maxsize
        return _from <= year <= _to


def fact_builder(
    version_entry: VersionEntry,
    category: str,
    source: str,
    version: str,
    content: dict,
    date_id: int,
) -> Fact:
    data = {
        "content": content,
        "source_url": version_entry.linkToArchive,
        "arquivo_url": version_entry.linkToNoFrame,
        "screenshot_url": version_entry.linkToScreenshot,
        "canonical_url": version_entry.originalURL,
        "version": version,
        "date_id": date_id,
        "category_id": category,
        "source_id": source,
    }
    return Fact(**data)
