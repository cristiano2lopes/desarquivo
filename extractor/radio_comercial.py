import asyncio
import itertools
import logging
from typing import Generator

from pyquery import PyQuery as pq
import pendulum

from arquivo import ArchivedURL, VersionEntry
from data import Fact, CategoryID, SourceID, HighRotationMusic
from extractor.core import Extractor

logger = logging.getLogger(__name__)


def extract_music_circa_2008(content) -> [HighRotationMusic]:
    """Extracts song/artist from layout in this example
    https://arquivo.pt/noFrame/replay/20081021131315/http://radiocomercial.clix.pt/
    """

    d = pq(content)
    selector = "#hp-leftbottom tr table tr .t11-white"
    elems = d(selector)
    results = []
    for elem in elems:
        artist = d(elem)(".t11-yellow-bold").text().strip()
        song = d(elem)(".t11-lightgrey").text().strip()
        if artist and song:
            results.append(HighRotationMusic(**{"artist": artist, "song": song}))

    return results


def extract_music_circa_2012(content) -> [HighRotationMusic]:
    """Extracts song/artist from layout in this example
    https://arquivo.pt/noFrame/replay/20120121123254/http://radiocomercial.clix.pt/
    """

    d = pq(content)
    selector = ".tnt_hp_list"
    elems = d(selector)
    results = []
    for elem in elems:
        artist = pq(elem)(".tnt_hp_artist").text().strip()
        song = pq(elem)(".tnt_hp_song").text().strip()
        if artist and song:
            results.append(HighRotationMusic(**{"artist": artist, "song": song}))

    return results


def extract_music_circa_2016(content) -> [HighRotationMusic]:
    """Extracts song/artist from layout in this example
    https://arquivo.pt/wayback/20160226180211/http://radiocomercial.iol.pt/
    """
    d = pq(content)
    results = []
    for i in range(1, 4):
        selector = f"#panel-{i}"
        matches = d(selector).children()
        if len(matches) == 5:
            (artist_elem, song_elem) = d(selector).children()[2:4]
            artist = artist_elem.text.strip()
            song = song_elem.text.strip()
            if artist and song:
                results.append(HighRotationMusic(**{"artist": artist, "song": song}))

    return results

def extract_music_circa_2019(content) -> [HighRotationMusic]:
    """Extracts song/artist from layout in this example
    https://arquivo.pt/noFrame/replay/20190101051253/https://radiocomercial.iol.pt/programas/8/todos-no-top-semana
    """
    d = pq(content)
    selector = ".media-box-title.votes"
    elems = d(selector)
    results = []
    for elem in d(elems)[:10]:
        artist = d(elem)("span").text().strip()
        song = d(elem)("p").text().strip()
        if artist and song:
            results.append(HighRotationMusic(**{"artist": artist, "song": song}))

    return results


def extract_music_circa_2020(content) -> [HighRotationMusic]:
    """Extracts song/artist from layout in this example
    https://arquivo.pt/noFrame/replay/20200313185844/https://radiocomercial.iol.pt/programas/tnt-todos-no-top
    """
    d = pq(content)
    selector = ".song-info"
    elems = d(selector)
    results = []
    for elem in d(elems)[:10]:
        artist = d(elem)(".songArtist").text().strip()
        song = d(elem)(".songTitle").text().strip()
        if artist and song:
            results.append(HighRotationMusic(**{"artist": artist, "song": song}))

    return results


class RadioComercialV1(Extractor):

    version = "v1"
    urls = [
        "https://radiocomercial.clix.pt",
        "https://radiocomercial.iol.pt",
        "https://radiocomercial.iol.pt/programas/8/todos-no-top-semana",
        "https://radiocomercial.iol.pt/programas/tnt-todos-no-top"
    ]

    applicable_time_span = (2008, pendulum.now().year)

    def extract_music_high_rotation(
        self, version_entry: VersionEntry, archived_url: ArchivedURL
    ) -> Generator[Fact, None, None]:
        dt = self.repository.fetch_date(
            version_entry.dt.year, version_entry.dt.month, version_entry.dt.day
        )

        if dt:
            results = itertools.chain(
                extract_music_circa_2008(archived_url.content),
                extract_music_circa_2012(archived_url.content),
                extract_music_circa_2016(archived_url.content),
                extract_music_circa_2019(archived_url.content),
                extract_music_circa_2020(archived_url.content)
            )

            for result in results:
                data = {
                    "content": result.dict(),
                    "source_url": version_entry.linkToArchive,
                    "arquivo_url": version_entry.linkToNoFrame,
                    "screenshot_url": version_entry.linkToScreenshot,
                    "canonical_url": version_entry.originalURL,
                    "version": self.version,
                    "date_id": dt.id,
                    "category_id": CategoryID.music_high_rotation,
                    "source_id": SourceID.desarquivo,
                }
                yield Fact(**data)

    async def extract(self) -> Generator[Fact, None, None]:
        _from = max(self.applicable_time_span[0], self.params.start_year)
        _to = min(self.applicable_time_span[1], self.params.end_year)
        yearly_tasks = [
            self.arquivo.fetch_url_versions(url, str(year), str(year + 1))
            for year in range(_from, _to + 1)
            for url in self.urls
        ]
        all_resp = await asyncio.gather(*yearly_tasks)
        all_versions = itertools.chain(*all_resp)

        for version in all_versions:
            if version.dt.day == self.params.day and version.dt.month == self.params.month:
                archived_url = await self.arquivo.fetched_archived_url(version)
                music_facts = self.extract_music_high_rotation(version, archived_url)
                for fact in music_facts:
                    yield fact
