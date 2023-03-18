from pydantic import BaseModel

from enum import Enum


class CategoryID(str, Enum):
    music_high_rotation = "music_high_rotation"
    cinema_on_theaters = "cinema_on_theaters"


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
    canonical_url: str
    version: str
    date_id: int
    category_id: str
    source_id: str
    location_id: str | None = None


