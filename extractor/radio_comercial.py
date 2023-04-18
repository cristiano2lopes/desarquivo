import asyncio
import itertools
import logging
from typing import Generator

from pyquery import PyQuery as pq
import pendulum

from arquivo import ArchivedURL, VersionEntry
from data import Fact, CategoryID, SourceID, HighRotationMusic, ExtractorDim
from extractor.core import (
    Extractor,
    ExtractionTargetURL,
    arquivo_fact_builder,
    ExtractionResult,
)

logger = logging.getLogger(__name__)


def extract_music_circa_2008(content) -> [ExtractionResult]:
    """Extracts song/artist from layout in this example
    https://arquivo.pt/noFrame/replay/20081021131315/http://radiocomercial.clix.pt/
    """

    d = pq(content)
    selector = "#hp-leftbottom tr table tr .t11-white"
    elems = d(selector)
    results = []
    for elem in elems:
        artist = d(elem)(".t11-yellow-bold").text()
        song = d(elem)(".t11-lightgrey").text()
        if artist and song:
            results.append(
                ExtractionResult(
                    content=HighRotationMusic(**{"artist": artist, "song": song}),
                    accessory_content=None,
                )
            )

    return results


def extract_music_circa_2012(content) -> [ExtractionResult]:
    """Extracts song/artist from layout in this example
    https://arquivo.pt/noFrame/replay/20120121123254/http://radiocomercial.clix.pt/
    """

    d = pq(content)
    selector = ".tnt_hp_list"
    elems = d(selector)
    results = []
    for elem in elems:
        artist = pq(elem)(".tnt_hp_artist").text()
        song = pq(elem)(".tnt_hp_song").text()
        if artist and song:
            results.append(
                ExtractionResult(
                    content=HighRotationMusic(**{"artist": artist, "song": song}),
                    accessory_content=None,
                )
            )

    return results


def extract_music_circa_2016(content) -> [ExtractionResult]:
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
            artist = artist_elem.text
            song = song_elem.text
            if artist and song:
                results.append(
                    ExtractionResult(
                        content=HighRotationMusic(**{"artist": artist, "song": song}),
                        accessory_content=None,
                    )
                )

    return results


def extract_music_circa_2019(content) -> [ExtractionResult]:
    """Extracts song/artist from layout in this example
    https://arquivo.pt/noFrame/replay/20190101051253/https://radiocomercial.iol.pt/programas/8/todos-no-top-semana
    """
    d = pq(content)
    selector = ".media-box-title.votes"
    elems = d(selector)
    results = []
    for elem in d(elems)[:10]:
        artist = d(elem)("span").text()
        song = d(elem)("p").text()
        if artist and song:
            results.append(
                ExtractionResult(
                    content=HighRotationMusic(**{"artist": artist, "song": song}),
                    accessory_content=None,
                )
            )

    return results


def extract_music_circa_2020(content) -> [ExtractionResult]:
    """Extracts song/artist from layout in this example
    https://arquivo.pt/noFrame/replay/20200313185844/https://radiocomercial.iol.pt/programas/tnt-todos-no-top
    """
    d = pq(content)
    selector = ".song-info"
    elems = d(selector)
    results = []
    for elem in d(elems)[:10]:
        artist = d(elem)(".songArtist").text()
        song = d(elem)(".songTitle").text()
        if artist and song:
            results.append(
                ExtractionResult(
                    content=HighRotationMusic(**{"artist": artist, "song": song}),
                    accessory_content=None,
                )
            )

    return results


class RadioComercialV1(Extractor):
    version = "v1"
    urls = [
        ExtractionTargetURL("https://radiocomercial.clix.pt", 2005, 2021),
        ExtractionTargetURL("https://radiocomercial.iol.pt", 2000, pendulum.now().year),
        ExtractionTargetURL(
            "https://radiocomercial.iol.pt/programas/8/todos-no-top-semana", 2016, 2020
        ),
        ExtractionTargetURL(
            "https://radiocomercial.iol.pt/programas/tnt-todos-no-top",
            2020,
            pendulum.now().year,
        ),
    ]

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
                extract_music_circa_2020(archived_url.content),
            )

            for result in results:
                yield arquivo_fact_builder(
                    version_entry,
                    CategoryID.music_high_rotation,
                    SourceID.desarquivo,
                    self.version,
                    result,
                    dt.id,
                    self.extractor_dim.id,
                )

    async def extract(self) -> Generator[Fact, None, None]:
        yearly_tasks = [
            self.arquivo.fetch_url_versions(url.value, str(year), str(year + 1))
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
                if archived_url is not None:
                    music_facts = (
                        version, archived_url
                    )
                    for fact in music_facts:
                        yield fact

    def extractor_specification(self) -> ExtractorDim:
        return ExtractorDim(
            **{"id": f"radio_comercial_{self.version}", "name": "Rádio Comercial"}
        )
