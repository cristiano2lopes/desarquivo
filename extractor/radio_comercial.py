import asyncio
import itertools
import logging
import random
from typing import Generator

from data import Fact
from extractor.core import Extractor

logger = logging.getLogger(__name__)


class RadioComercialV1(Extractor):

    version = "v1"
    url = "https://publico.pt"

    async def extract(self) -> Generator[Fact, None, None]:
        yearly_tasks = [
            self.arquivo.fetch_url_versions("https://publico.pt", year, year)
            for year in range(
                int(self.params.start_year), int(self.params.end_year) + 1
            )
        ]
        all_resp = await asyncio.gather(*yearly_tasks)
        all_versions = itertools.chain(*all_resp)

        for version in all_versions:
            # Scrap and yield ?
            some_data = {
                "content": {
                    "data": random.randint(0, 1_000_000_000)
                },
                "source_url": self.url,
                "canonical_url": self.url,
                "version": self.version,
                "date_id": self.repository.fetch_date(2010, 1, 1).id,
                "category_id": "music_high_rotation",
                "source_id": "desarquivo",
            }
            yield Fact(**some_data)
