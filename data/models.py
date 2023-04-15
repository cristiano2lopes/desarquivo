import httpx
from pydantic import BaseModel, validator

from enum import Enum

from arquivo import ArquivoApiPath


class CategoryID(str, Enum):
    music_high_rotation = "music_high_rotation"
    cinema_on_theaters = "cinema_on_theaters"
    news_highlight = "news_highlight"


class SourceID(str, Enum):
    desarquivo = "desarquivo"
    tmdb = "tmdb"
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


class ExtractorDim(BaseModel):
    id: str
    name: str


class Fact(BaseModel):
    id: str | None = None
    content: dict
    accessory_content: dict | None
    source_url: str
    arquivo_url: str
    canonical_url: str
    screenshot_url: str | None
    version: str
    date_id: int
    category_id: str
    source_id: str
    extractor_id: str
    location_id: str | None = None


def trim(content: str) -> str:
    return content.strip()


def make_absolute_url(path_or_url: str):
    if path_or_url.startswith("/"):
        url = httpx.URL(ArquivoApiPath.BASE_URL)
        return str(url.join(path_or_url))
    else:
        return path_or_url


class HighRotationMusic(BaseModel):
    artist: str
    song: str

    _normalize_artist = validator("artist", allow_reuse=True)(trim)
    _normalize_song = validator("song", allow_reuse=True)(trim)


class NewsHighlight(BaseModel):
    title: str
    summary: str

    _normalize_title = validator("title", allow_reuse=True)(trim)
    _normalize_summary = validator("summary", allow_reuse=True)(trim)


class NewsHighlightAccessory(BaseModel):
    more_link: str

    _make_absolute_url = validator("more_link", allow_reuse=True)(make_absolute_url)


class CinemaOnTheaters(BaseModel):
    title: str
    summary: str

    _normalize_title = validator("title", allow_reuse=True)(trim)
    _normalize_summary = validator("summary", allow_reuse=True)(trim)


class CinemaOnTheatersAccessory(BaseModel):
    poster_image_link: str
