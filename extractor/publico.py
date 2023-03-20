import asyncio
import itertools
import logging
from typing import Generator

from pyquery import PyQuery as pq
import pendulum

from arquivo import ArchivedURL, VersionEntry, Arquivo
from data import Fact, CategoryID, SourceID, NewsHighlight
from extractor.core import Extractor, ExtractionTargetURL, fact_builder

logger = logging.getLogger(__name__)


def extract_news_highlight_2008(content, full_url_fn) -> [NewsHighlight]:
    """Extracts_news_highlight from layout in this example"""
    d = pq(content)
    selector = ""
    title = d(selector).text()
    more_link = d(selector).attr("href")
    results = []
    if title and more_link:
        results.append(
            NewsHighlight(
                **{"title": title, "article": "", "more_link": full_url_fn(more_link)}
            )
        )

    return results


class PublicoV1(Extractor):

    version = "v1"
    urls = [
        ExtractionTargetURL("https://www.publico.pt", 1996, pendulum.now().year),
    ]

    def extract_news_highlight(
        self, version_entry: VersionEntry, archived_url: ArchivedURL
    ) -> Generator[Fact, None, None]:
        dt = self.repository.fetch_date(
            version_entry.dt.year, version_entry.dt.month, version_entry.dt.day
        )

        if dt:
            results = itertools.chain(
                extract_news_highlight_2008(
                    archived_url.content, self.arquivo.to_absolute_url
                ),
            )

            for result in results:
                yield fact_builder(
                    version_entry,
                    CategoryID.news_highlight,
                    SourceID.desarquivo,
                    self.version,
                    result.dict(),
                    dt.id,
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
