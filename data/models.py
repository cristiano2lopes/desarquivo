from pydantic import BaseModel, validator

from enum import Enum


class CategoryID(str, Enum):
    music_high_rotation = "music_high_rotation"
    cinema_on_theaters = "cinema_on_theaters"
    news_highlight = "news_highlight"


class SourceID(str, Enum):
    desarquivo = "desarquivo"
    manual = "manual"


class LocationDim(BaseModel):
    id: str
    name: str


class SourceDim(BaseModel):
    id: str
    name: str


class CategoryDim(BaseModel):
    id: str
    name: str


class DateDim(BaseModel):
    id: int
    year: int
    month: int
    day: int
    day_of_week: int


class Fact(BaseModel):
    id: str | None = None
    content: dict
    source_url: str
    arquivo_url: str
    canonical_url: str
    screenshot_url: str | None
    version: str
    date_id: int
    category_id: str
    source_id: str
    location_id: str | None = None


def trim(content: str) -> str:
    return content.strip()


class HighRotationMusic(BaseModel):

    artist: str
    song: str

    _normalize_artist = validator('artist', allow_reuse=True)(trim)
    _normalize_song = validator('song', allow_reuse=True)(trim)


class NewsHighlight(BaseModel):

    title: str
    summary: str
    more_link: str

    _normalize_title = validator('title', allow_reuse=True)(trim)
    _normalize_summary = validator('summary', allow_reuse=True)(trim)
