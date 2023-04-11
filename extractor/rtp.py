import asyncio
import itertools
import logging
from typing import Generator

from pyquery import PyQuery as pq

from arquivo import ArchivedURL, VersionEntry
from data import Fact, CategoryID, SourceID, NewsHighlight, NewsHighlightAccessory
from extractor.core import (
    Extractor,
    ExtractionTargetURL,
    fact_builder,
    ExtractionResult,
)

logger = logging.getLogger(__name__)


def extract_news_highlight_2008(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/wayback/20081021163216/http://ww1.rtp.pt/homepage/
    """
    d = pq(content)
    selector = "#NoticiasArea .DestkManchete a"
    title = d(selector).text()
    more_link = d(selector).attr("href")
    results = []
    if title and more_link:
        ExtractionResult(
            content=NewsHighlight(
                **{
                    "title": title,
                    "summary": "",
                }
            ),
            accessory_content=NewsHighlightAccessory(
                **{
                    "more_link": more_link,
                }
            ),
        )

    return results


def extract_news_highlight_2011(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/wayback/20110121145922/http://ww1.rtp.pt/homepage/
    """
    d = pq(content)
    selector = "#NewsContent .DestkPrincipal .Elemento .Text"
    elems = d(selector)
    results = []
    for elem in elems:
        title = d(elem)("h2").text()
        more_link = d(elem)("a").attr("href")
        if title and more_link:
            results.append(
                ExtractionResult(
                    content=NewsHighlight(
                        **{
                            "title": title,
                            "summary": "",
                        }
                    ),
                    accessory_content=NewsHighlightAccessory(
                        **{
                            "more_link": more_link,
                        }
                    ),
                )
            )

    return results


def extract_news_highlight_2012(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/wayback/20120121192742/http://ww1.rtp.pt/homepage/
    """
    d = pq(content)
    selector = ".DestkAllNews .Elemento.DestkNews.Separador .Text"
    results = []
    title = d(selector)("h3 a").text()
    more_link = d(selector)("h3 a").attr("href")
    subtitle = d(selector)("p").text()
    if title and more_link and subtitle:
        results.append(
            ExtractionResult(
                content=NewsHighlight(
                    **{
                        "title": title,
                        "summary": subtitle,
                    }
                ),
                accessory_content=NewsHighlightAccessory(
                    **{
                        "more_link": more_link,
                    }
                ),
            )
        )

    return results


def extract_news_highlight_2016(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/wayback/20160301180236/http://www.rtp.pt/homepage/
    """
    d = pq(content)
    selector = ".EmDestk .Area div"
    elems = d(selector)
    results = []
    for elem in elems:
        title = d(elem)("a").text()
        more_link = d(elem)("a").attr("href")
        subtitle = d(elem)("p").text()
        if title and more_link and subtitle:
            results.append(
                ExtractionResult(
                    content=NewsHighlight(
                        **{
                            "title": title,
                            "summary": subtitle,
                        }
                    ),
                    accessory_content=NewsHighlightAccessory(
                        **{
                            "more_link": more_link,
                        }
                    ),
                )
            )

    return results


def extract_news_highlight_2017(content) -> [ExtractionResult]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/wayback/20170201180213/http://www.rtp.pt/
    """
    d = pq(content)
    selector = ".page-cover-content"
    results = []
    title = d(selector).text()
    more_link = d(selector).attr("href")
    subtitle = ""
    if title and more_link:
        results.append(
            ExtractionResult(
                content=NewsHighlight(
                    **{
                        "title": title,
                        "summary": subtitle,
                    }
                ),
                accessory_content=NewsHighlightAccessory(
                    **{
                        "more_link": more_link,
                    }
                ),
            )
        )

    return results


class RTPV1(Extractor):
    version = "v1"
    urls = [
        ExtractionTargetURL("https://ww1.rtp.pt/homepage/", 2008, 2016),
        ExtractionTargetURL("https://www.rtp.pt/homepage/", 2009, 2023),
        ExtractionTargetURL("https://www.rtp.pt/", 2009, 2023),
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
                extract_news_highlight_2008(archived_url.content),
                extract_news_highlight_2011(archived_url.content),
                extract_news_highlight_2012(archived_url.content),
                extract_news_highlight_2016(archived_url.content),
                extract_news_highlight_2017(archived_url.content),
            )

            for result in results:
                yield fact_builder(
                    version_entry,
                    CategoryID.news_highlight,
                    SourceID.desarquivo,
                    self.version,
                    result,
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
