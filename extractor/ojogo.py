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


def extract_news_highlight_2014(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/wayback/20140427170213/http://www.ojogo.pt/
    """
    d = pq(content)
    results = []

    table = d(".LinkList_3 .LLItem")
    if table:
        for item in table[:3]:
            item_pq = pq(item)
            title = item_pq("h2").text()
            link = item_pq("a").attr("href")
            summary = item_pq(".LLIBLead").text()

            if title and link:
                results.append(
                    ExtractionResult(
                        content=NewsHighlight(
                            **{
                                "title": title,
                                "summary": summary,
                            }
                        ),
                        accessory_content=NewsHighlightAccessory(
                            **{
                                "more_link": link,
                            }
                        ),
                    )
                )
    return results


def extract_news_highlight_2017(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/noFrame/replay/20170101180216/http://www.ojogo.pt/
    """
    d = pq(content)
    results = []

    table = d(".t-g1-featured-1")
    if table:
        title = table("h2 a").text()
        link = table("h2 a").attr("href")
        summary = ""

        if title and link:
            results.append(
                ExtractionResult(
                    content=NewsHighlight(
                        **{
                            "title": title,
                            "summary": summary,
                        }
                    ),
                    accessory_content=NewsHighlightAccessory(
                        **{
                            "more_link": link,
                        }
                    ),
                )
            )
    return results


class OJOGOV1(Extractor):
    version = "v1"
    urls = [
        ExtractionTargetURL("http://www.ojogo.pt", 2003, 2022),
        ExtractionTargetURL("http://www.ojogo.pt/index.asp", 2003, 2022),
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
                extract_news_highlight_2014(archived_url.content),
                extract_news_highlight_2017(archived_url.content),
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
