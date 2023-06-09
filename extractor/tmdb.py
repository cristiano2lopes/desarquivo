import asyncio
import itertools
import logging
import os
import time
from typing import Generator, Self

import httpx
import pendulum
from pendulum import Date, DateTime
from pydantic import validator, BaseModel

from data import (
    Fact,
    CategoryID,
    SourceID,
    ExtractorDim,
    CinemaOnTheatersAccessory,
    CinemaOnTheaters,
)
from extractor.core import (
    Extractor,
)

logger = logging.getLogger(__name__)


def make_tmdb_absolute_url(path_or_url):
    if path_or_url.startswith("/"):
        return f"https://image.tmdb.org/t/p/original{path_or_url}"
    else:
        return path_or_url


class TMDBCinemaOnTheatersAccessory(CinemaOnTheatersAccessory):
    _make_absolute_url = validator("poster_image_link", allow_reuse=True)(
        make_tmdb_absolute_url
    )


class TMDBResult(BaseModel):
    id: int
    title: str
    overview: str
    poster_path: str | None

    @property
    def link(self):
        return f"https://www.themoviedb.org/movie/{self.id}"


class TMDBResultPage(BaseModel):
    page: int
    results: list[TMDBResult]


class TMDBClient:
    client: httpx.AsyncClient

    DEFAULT_WAIT = 30
    wait_until = None

    def __init__(self, api_key):
        params = {
            "api_key": api_key,
            "language": "pt-PT",
            "region": "PT",
        }
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(120),
            base_url="https://api.themoviedb.org/3/",
            follow_redirects=True,
            params=params,
        )
        self.__semaphore = asyncio.Semaphore(20)
        self.wait_until_lock = asyncio.Lock()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self.client.aclose()

    async def handle_resp(self, resp: httpx.Response):
        try:
            resp.raise_for_status()
            return resp
        except httpx.HTTPStatusError as status_error:
            if status_error.response.status_code == httpx.codes.TOO_MANY_REQUESTS:
                logger.error(f"Too Many Requests: {resp.headers}")
                async with self.wait_until_lock:
                    self.wait_until = time.time() + self.DEFAULT_WAIT
                    logger.info(f"Too Many Requests: wait until {self.wait_until}")
            raise status_error

    async def fetch_url(self, url: str, params: dict = None) -> httpx.Response:
        async with self.__semaphore:
            return await self.handle_resp(
                await self.client.get(url, params=params or {})
            )


def intervals_for(year, month, day=None) -> Generator[tuple[DateTime, DateTime], None, None]:
    if day is None:
        dt = pendulum.datetime(year, month, 1)
    else:
        dt = pendulum.datetime(year, month, day)

    if dt.day_of_week == pendulum.THURSDAY:
        dt = dt.subtract(days=1)

    while month == dt.next(pendulum.THURSDAY).month:
        dt = dt.next(pendulum.THURSDAY)
        yield dt, dt.next(pendulum.WEDNESDAY)


class TMDBV1(Extractor):
    version = "v1"

    def __init__(self, *args, **kwargs):
        super(TMDBV1, self).__init__(*args, **kwargs)

    async def extract_movie_releases(
            self, tmdb_client: TMDBClient, start: DateTime, end: DateTime
    ) -> Generator[Fact, None, None]:
        params = {
            "sort_by": "popularity.desc",
            "include_adult": False,
            "with_release_type": "3",
            "release_date.gte": start,
            "release_date.lte": end,
        }
        resp = await tmdb_client.fetch_url("/discover/movie", params=params)
        result_page = TMDBResultPage(**resp.json())

        for result in result_page.results[:3]:
            content = CinemaOnTheaters(
                **{"title": result.title, "summary": result.overview}
            )
            if result.poster_path:
                accessory_content = TMDBCinemaOnTheatersAccessory(
                    **{"poster_image_link": result.poster_path}
                ).dict()
            else:
                accessory_content = None

            for dt in pendulum.period(start, end):
                dt = self.repository.fetch_date(dt.year, dt.month, dt.day)
                if dt is not None:
                    data = {
                        "content": content.dict(),
                        "accessory_content": accessory_content,
                        "source_url": result.link,
                        "arquivo_url": result.link,
                        "screenshot_url": None,
                        "canonical_url": result.link,
                        "version": self.version,
                        "date_id": dt.id,
                        "category_id": CategoryID.cinema_on_theaters,
                        "source_id": SourceID.tmdb,
                        "extractor_id": self.extractor_dim.id,
                    }
                    yield Fact(**data)

    async def extract(self) -> Generator[Fact, None, None]:
        tmdb_key = os.environ.get("TMDB_KEY")
        if tmdb_key is None:
            logger.warning(
                f"No TMDB_KEY set on environment, skipping extractor {self.extractor_dim.id}"
            )
            return

        day = self.params.day
        month = self.params.month

        async with TMDBClient(api_key=tmdb_key) as tmdb_client:
            for year in range(self.params.start_year, self.params.end_year + 1):

                for start, end in intervals_for(year, month, day):
                    movie_releases_facts = self.extract_movie_releases(
                        tmdb_client, start=start, end=end
                    )
                    async for fact in movie_releases_facts:
                        yield fact

    def extractor_specification(self) -> ExtractorDim:
        return ExtractorDim(
            **{"id": f"tmdb_{self.version}", "name": "The Movie Database"}
        )
