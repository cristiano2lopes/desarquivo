import itertools
import operator
from typing import Iterable

from .client import *
from .models import *


def pipe(data, *args):
    for fn in args:
        data = fn(data)
    return data


def remove_error_status_codes(data: Iterable[VersionEntry]) -> Iterable[VersionEntry]:
    return filter(lambda e: 200 <= e.statusCode < 400, data)


def remove_redirects_status_codes(
    data: Iterable[VersionEntry],
) -> Iterable[VersionEntry]:
    return filter(lambda e: 200 <= e.statusCode < 300, data)


def remove_entries_close_in_time(
    data: Iterable[VersionEntry],
) -> Iterable[VersionEntry]:
    return map(
        next,
        map(operator.itemgetter(1), itertools.groupby(data, lambda e: e.tstamp[:-4])),
    )


class Arquivo:
    def __init__(self, arquivo_client: ArquivoClient):
        self.__client = arquivo_client

    async def fetched_archived_url(self, version: VersionEntry) -> ArchivedURL:
        try:
            resp = await self.__client.fetch_archived_url(
                version.originalURL, version.tstamp
            )
            archived_url = ArchivedURL(
                **{
                    "headers": list(resp.headers.items()),
                    "content": resp.text,
                }
            )
            return archived_url
        except:
            logger.exception("Fetch archived version entry")

    async def fetch_url_versions(
        self, url: str, since: str, until: str, retries: int = 3
    ) -> Iterable[VersionEntry]:
        all_version_responses = []
        try:
            resp = await self.__client.fetch_url_versions(url, since, until)
            json_content = resp.json()
            if "response_items" in json_content:
                version_response = VersionsResponse(**resp.json())
            elif retries > 0:
                logger.warning(f"Retry ({retries}): {since} {until} {resp.request.url}")
                return await self.fetch_url_versions(url, since, until, retries - 1)
            else:
                logger.error(
                    f"Fetch url versions, no response items: {since} {until} {resp.request.url}"
                )
                return []

            while version_response.has_more_data():
                all_version_responses.append(version_response)
                resp = await self.__client.fetch_url(version_response.next_page)
                version_response = VersionsResponse(**resp.json())
        except Exception as e:
            logger.exception(f"Fetch url versions {url} {since}-{until}")

        all_data = itertools.chain(
            *[resp.response_items for resp in all_version_responses]
        )
        return pipe(
            all_data,
            remove_error_status_codes,
            remove_redirects_status_codes,
            remove_entries_close_in_time,
        )
