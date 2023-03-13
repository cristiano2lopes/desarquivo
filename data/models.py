from pydantic import BaseModel


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
