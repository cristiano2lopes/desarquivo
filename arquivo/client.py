import asyncio
import time
from typing import Self, Optional

import httpx
from enum import StrEnum
import logging
import chardet
from sqlite_utils import Database

logger = logging.getLogger(__name__)


async def log_request(request: httpx.Request):
    pass


async def log_response(response: httpx.Response):
    pass


class ArquivoApiPath(StrEnum):
    TEXT_SEARCH = ("/textsearch",)
    BASE_URL = ("https://arquivo.pt",)
    NO_FRAME_REPLAY = "/noFrame/replay"


def autodetect(content):
    return chardet.detect(content).get("encoding")


class ArquivoClient:

    DEFAULT_WAIT = 20

    client: httpx.AsyncClient
    wait_until: Optional[int]
    http_cache_db: Database

    def __init__(self, _http_cache_db: Database = None):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(120),
            base_url=ArquivoApiPath.BASE_URL,
            follow_redirects=True,
            event_hooks={"request": [log_request], "response": [log_response]},
            default_encoding=autodetect,
        )
        self.__semaphore = asyncio.Semaphore(20)
        self.wait_until_lock = asyncio.Lock()
        self.http_cache_db = _http_cache_db

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def close(self):
        await self.client.aclose()

    def cache_response(self, resp: httpx.Response):
        if self.http_cache_db:
            self.http_cache_db["requests"].insert(
                {"url": str(resp.url), "content": resp.text},
                pk="url",
                ignore=True
            )

    async def handle_resp(self, resp: httpx.Response):
        try:
            resp.raise_for_status()
            self.cache_response(resp)
            return resp
        except httpx.HTTPStatusError as status_error:
            if status_error.response.status_code == httpx.codes.TOO_MANY_REQUESTS:
                logger.error(f"Too Many Requests: {resp.headers}")
                async with self.wait_until_lock:
                    self.wait_until = time.time_ns() + self.DEFAULT_WAIT
                    logger.info(f"Too Many Requests: wait until {self.wait_until}")
            raise status_error

    """Fetches any url passed, useful for pagination with already built links,
    from previous responses"""

    async def fetch_url(self, url: str) -> httpx.Response:
        async with self.__semaphore:
            return await self.handle_resp(await self.client.get(url))

    async def fetch_archived_url(self, url: str, ts: str) -> httpx.Response:
        request_path = f"{ArquivoApiPath.NO_FRAME_REPLAY}/{ts}/{url}"
        async with self.__semaphore:
            return await self.handle_resp(await self.client.get(request_path))

    async def fetch_url_versions(
        self,
        url: str,
        since: str,
        until: str,
        page_size: int = 2000,
        fields: tuple[str, ...] = (),
    ) -> httpx.Response:
        params = {
            "versionHistory": url,
            "from": since,
            "to": until,
            "maxItems": page_size,
        }
        if fields:
            params["fields"] = (",".join(fields),)
        async with self.__semaphore:
            return await self.handle_resp(
                await self.client.get(ArquivoApiPath.TEXT_SEARCH, params=params)
            )
