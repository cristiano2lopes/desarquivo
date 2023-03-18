import asyncio
import functools
import itertools
import logging
import random
from typing import Generator, Optional

from pyquery import PyQuery as pq

from arquivo import ArchivedURL, VersionEntry
from data import Fact, CategoryID, SourceID
from extractor.core import Extractor

logger = logging.getLogger(__name__)


class RadioComercialV1(Extractor):

    version = "v1"
    urls = [
        "https://radiocomercial.clix.pt",
        "https://radiocomercial.iol.pt",
        "https://radiocomercial.iol.pt/programas/7/todos-no-top-votacao",
    ]

    """
    https://arquivo.pt/wayback/20081021131315/http://radiocomercial.clix.pt/
    """
    def extract_music_1(self, content):
        d = pq(content)
        selector = "#hp-leftbottom tr table tr .t11-white"
        elems = d(selector)
        results = []
        for elem in elems:
            artist = d(elem)(".t11-yellow-bold").text()
            song = d(elem)(".t11-lightgrey").text()
            results.append((artist, song))

        return results

    def extract_music_high_rotation(
        self, version_entry: VersionEntry, archived_url: ArchivedURL
    ) -> Optional[Fact]:
        dt = self.repository.fetch_date(
            version_entry.dt.year, version_entry.dt.month, version_entry.dt.day
        ).id

        results = self.extract_music_1(archived_url.content)

        data = {
            "content": {
                "data": random.randint(0, 1_000_000_000)
            },
            "source_url": version_entry.linkToArchive,
            "canonical_url": version_entry.originalURL,
            "version": self.version,
            "date_id": dt,
            "category_id": CategoryID.music_high_rotation,
            "source_id": SourceID.desarquivo,
        }
        return Fact(**data)

    async def extract(self) -> Generator[Fact, None, None]:
        yearly_tasks = [
            self.arquivo.fetch_url_versions(url, year, year)
            for year in range(
                int(self.params.start_year), int(self.params.end_year) + 1
            )
            for url in self.urls
        ]
        all_resp = await asyncio.gather(*yearly_tasks)
        all_versions = itertools.chain(*all_resp)

        for version in all_versions:
            archived_url = await self.arquivo.fetched_archived_url(version)
            music_fact = self.extract_music_high_rotation(version, archived_url)
            if music_fact:
                yield music_fact

