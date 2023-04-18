import asyncio
import itertools
import logging
from typing import Generator

from pyquery import PyQuery as pq

from arquivo import ArchivedURL, VersionEntry
from data import (
    Fact,
    CategoryID,
    SourceID,
    NewsHighlight,
    NewsHighlightAccessory,
    ExtractorDim,
)
from extractor.core import (
    Extractor,
    ExtractionTargetURL,
    arquivo_fact_builder,
    ExtractionResult,
)

logger = logging.getLogger(__name__)


def extract_news_highlight_XXX(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    """
    return []


class OJOGOV1(Extractor):
    version = "v1"
    urls = [
        ExtractionTargetURL("http://www.ojogo.pt/", 1998, 2022),
    ]

    def extract_news_highlight(
        self, version_entry: VersionEntry, archived_url: ArchivedURL
    ) -> Generator[Fact, None, None]:
        dt = self.repository.fetch_date(
            version_entry.dt.year, version_entry.dt.month, version_entry.dt.day
        )

        content = archived_url.content
        if not content:
            logger.warning(f"No content for {version_entry.linkToNoFrame}")

        if dt and content:
            results = itertools.chain(
                extract_news_highlight_XXX(archived_url.content),
            )

            for result in results:
                yield arquivo_fact_builder(
                    version_entry,
                    CategoryID.news_highlight,
                    SourceID.desarquivo,
                    self.version,
                    result,
                    dt.id,
                    self.extractor_dim.id,
                )

    async def extract(self) -> Generator[Fact, None, None]:
        yearly_tasks = [
            self.arquivo.fetch_url_versions(url.value, str(year), str(year))
            for year in range(self.params.start_year, self.params.end_year + 1)
            for url in self.urls
            if url.applicable(year)
        ]
        all_resp = await asyncio.gather(*yearly_tasks)
        all_versions = itertools.chain(*all_resp)

        for version in all_versions:
            if (
                self.params.day is None or version.dt.day == self.params.day
            ) and version.dt.month == self.params.month:
                archived_url = await self.arquivo.fetched_archived_url(version)
                news_facts = self.extract_news_highlight(version, archived_url)
                for fact in news_facts:
                    yield fact

    def extractor_specification(self) -> ExtractorDim:
        return ExtractorDim(
            **{"id": f"ojogo_{self.version}", "name": "O Jogo"}
        )
