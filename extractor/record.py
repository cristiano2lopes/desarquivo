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


def extract_news_highlight_2005(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/wayback/20051013062923/http://www.record.pt:80/
    """
    d = pq(content)
    results = []

    table = d("table").filter(lambda i: d(this).attr("width") == "300")
    if table:
        title = table("a").text().strip()
        link = table("a").attr("href")
        summary = table("span.record").text()

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
                            "more_link": f"/{link}",
                        }
                    ),
                )
            )

    return results

def extract_news_highlight_2006(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/noFrame/replay/20060101015503/http://www.record.pt:80/
    """
    d = pq(content)
    results = []

    table = d(".tr_preto")
    if table:
        title = table("a.v18b_red").text().strip()
        link = table("a.v18b_red").attr("href")
        summary = table("td.v12_black").text()

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
                            "more_link": f"/{link}",
                        }
                    ),
                )
            )

    return results

def extract_news_highlight_2008(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/noFrame/replay/20080101144702/http://www.record.pt:80/
    """
    d = pq(content)
    results = []

    table = d("#tcontent1")
    if table:
        title = table("a.tituleira18red1").text().strip()
        link = table("a.tituleira18red1").attr("href")
        summary = table("td.apreto12n").text()

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
                            "more_link": f"/{link}",
                        }
                    ),
                )
            )

    return results

def extract_news_highlight_2011(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/noFrame/replay/20110101160208/http://www.record.xl.pt/
    """
    d = pq(content)
    results = []

    table = d("#manchetesHome")
    if table:
        title = table("div.titBlHoje a").text().replace("\n", " ").lower().capitalize().strip()
        link = table("div.titBlHoje a").attr("href")
        summary = table("a.hl2").text().replace("\n", " ").lower().capitalize()

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


def extract_news_highlight_2016(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/noFrame/replay/20160101180213/http://www.record.xl.pt/
    """
    d = pq(content)
    results = []

    table = d(".top-content .item")
    if table:
        for item in table:
            item_pq = pq(item)
            title = item_pq("h1 a").text().replace("\n", " ").lower().capitalize().strip()
            link = item_pq("h1 a").attr("href")
            summary = item_pq("p").text().replace("\n", " ").lower().capitalize()

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
    https://arquivo.pt/noFrame/replay/20171201180223/http://www.record.pt/
    """
    d = pq(content)
    results = []

    table = d(".destaquescarrosel .thumb-info")
    if table:
        for item in table:
            item_pq = pq(item)
            title = item_pq("a").text().replace("\n", " ").lower().capitalize().strip()
            link = item_pq("a").attr("href")
            summary = table("p").text().replace("\n", " ").lower().capitalize()

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


class RecordV1(Extractor):
    version = "v1"
    urls = [
        ExtractionTargetURL("http://www.record.pt/", 2003, 2022),
        ExtractionTargetURL("http://www.record.xl.pt/", 2007, 2022),
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
                extract_news_highlight_2005(archived_url.content),
                extract_news_highlight_2006(archived_url.content),
                extract_news_highlight_2008(archived_url.content),
                extract_news_highlight_2011(archived_url.content),
                extract_news_highlight_2016(archived_url.content),
                extract_news_highlight_2017(archived_url.content),
            )

            for result in results:
                yield arquivo_fact_builder(
                    version_entry,
                    CategoryID.sports_highlight,
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
            **{"id": f"record_{self.version}", "name": "Record"}
        )
