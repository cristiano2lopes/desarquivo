from typing import List, Optional

import pendulum
from pydantic import BaseModel


class VersionEntry(BaseModel):
    title: str
    originalURL: str
    linkToArchive: str
    tstamp: str
    contentLength: int
    digest: str
    mimeType: str
    encoding: Optional[str]
    linkToScreenshot: str
    linkToNoFrame: str
    linkToExtractedText: Optional[str]
    linkToMetadata: str
    linkToOriginalFile: str
    fileName: str
    collection: str
    offset: int
    statusCode: int

    @property
    def dt(self):
        dt = pendulum.from_format(self.tstamp, "YYYYMMDDHHmmss")
        return dt

    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))


class VersionsResponse(BaseModel):
    serviceName: str
    linkToService: str
    next_page: Optional[str]
    estimated_nr_results: int
    response_items: List[VersionEntry]

    def has_more_data(self):
        return self.next_page and self.response_items

    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))


class ArchivedURL(BaseModel):
    headers: List[tuple[str, str]]
    content: str

    def __hash__(self):
        return hash((type(self),) + tuple(self.__dict__.values()))
