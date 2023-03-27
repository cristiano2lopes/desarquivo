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


def extract_news_highlight_2005(content) -> [NewsHighlight]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/wayback/20050104091811/http://www.publico.pt:80/
    """
    d = pq(content)
    selector = ".gerlinks table"
    first_table = d(selector).eq(0)
    results = []
    if first_table:
        red_title = d(first_table)(".textoTituloVermelho")
        black_title = d(first_table)(".textoTituloPreto")
        title1 = red_title.text()
        title2 = black_title.text()
        title1_link = red_title.attr("href")
        title2_link = black_title.attr("href")

        summary1 = d(first_table)(".textoCaixa").eq(0).text()
        summary2 = d(first_table)(".textoCaixa").eq(1).text()

        if title1 and title1_link:
            results.append(
                NewsHighlight(
                    **{"title": title1, "summary": summary1, "more_link": title1_link}
                )
            )

        if title2 and title2_link:
            results.append(
                NewsHighlight(
                    **{"title": title2, "summary": summary2, "more_link": title2_link}
                )
            )

    return results


def extract_news_highlight_2010(content) -> [NewsHighlight]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/noFrame/replay/20100502143105/http://www.publico.pt/
    """
    d = pq(content)
    selector = ".headlines .featured"
    featured = d(selector)
    results = []
    if featured:
        for e in featured:
            elem = d(e)
            if elem:
                title1 = elem("h2").text()
                title1_link = elem("a").attr("href")
                summary1 = elem(".entry-body p:not(.author)").text()
                if title1 and title1_link and summary1:
                    results.append(
                        NewsHighlight(
                            **{
                                "title": title1,
                                "summary": summary1,
                                "more_link": title1_link,
                            }
                        )
                    )

    return results


def extract_news_highlight_2013(content) -> [NewsHighlight]:
    """Extracts_news_highlight from layout in this example
    https://arquivo.pt/wayback/20130110160340/http://www.publico.pt/
    """
    d = pq(content)
    selector = ".primary .entries-primary .top-entry"
    featured = d(selector)
    results = []
    if featured:
        title1 = featured(".entry-title").text()
        title1_link = featured(".entry-header a").attr("href")
        summary1 = featured(".entry-summary").text()
        if title1 and title1_link and summary1:
            results.append(
                NewsHighlight(
                    **{"title": title1, "summary": summary1, "more_link": title1_link}
                )
            )

    return results


class PublicoV1(Extractor):
    version = "v1"
    urls = [
        ExtractionTargetURL("https://www.publico.pt", 1996, pendulum.now().year),
        # ExtractionTargetURL("https://www.publico.clix.pt", 2005, 2019),
    ]

    def extract_news_highlight(
        self, version_entry: VersionEntry, archived_url: ArchivedURL
    ) -> Generator[Fact, None, None]:
        dt = self.repository.fetch_date(
            version_entry.dt.year, version_entry.dt.month, version_entry.dt.day
        )

        if dt:
            results = itertools.chain(
                extract_news_highlight_2005(archived_url.content),
                extract_news_highlight_2010(archived_url.content),
                extract_news_highlight_2013(archived_url.content),
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
