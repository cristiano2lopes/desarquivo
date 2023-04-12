import logging
from pathlib import Path
from typing import Iterable

from sqlite_utils import Database

from data.models import *

logger = logging.getLogger(__name__)


class DesarquivoDb:
    def __init__(self, recreate_db: bool):
        self.recreate_db = recreate_db

    def __enter__(self):
        def __tracer(sql, params):
            logger.debug("SQL: %s - params: %s", sql, params)

        self.db = Database(
            f"db_files/facts.db", tracer=__tracer, recreate=self.recreate_db
        )
        if self.recreate_db:
            self.db.enable_wal()
            self.db.execute("PRAGMA foreign_keys = ON;")
            self.db.execute("PRAGMA auto_vacuum = FULL;")
            for p in sorted(Path("./data/sql/").glob("*.sql")):
                with open(p, "r") as file:
                    self.db.executescript(file.read())
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()


class HttpCacheDb:
    def __init__(self, recreate_db: bool):
        self.recreate_db = recreate_db

    def __enter__(self):
        def __tracer(sql, params):
            logger.debug("SQL: %s - params: %s", sql, params)

        self.db = Database(
            f"db_files/http_requests.db", tracer=__tracer, recreate=self.recreate_db
        )
        if self.recreate_db:
            self.db.enable_wal()
            self.db.execute("PRAGMA foreign_keys = ON;")
            self.db.execute("PRAGMA auto_vacuum = FULL;")

        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db.close()


class Repository:
    def __init__(self, db: Database):
        self.db = db

    def fetch_date(self, year: int, month: int, day: int) -> DateDim | None:
        records = self.db.table("date_dim").rows_where(
            "year = :year AND month = :month AND day = :day",
            {"year": year, "month": month, "day": day},
            limit=1,
        )

        if record := next(records, None):
            return DateDim(**record)
        else:
            return None

    def fetch_category(self, _id: str) -> CategoryDim | None:
        records = self.db.table("category_dim").rows_where(
            "id = :id", {"id": _id}, limit=1
        )

        if record := next(records, None):
            return CategoryDim(**record)
        else:
            return None

    def fetch_source(self, _id: str) -> SourceDim | None:
        records = self.db.table("source_dim").rows_where(
            "id = :id", {"id": _id}, limit=1
        )

        if record := next(records, None):
            return SourceDim(**record)
        else:
            return None

    def fetch_extractor(self, _id: str, name: str) -> ExtractorDim:
        records = self.db.table("extractor_dim").rows_where(
            "id = :id", {"id": _id}, limit=1
        )

        if record := next(records, None):
            return ExtractorDim(**record)
        else:
            new_record = {"id": _id, "name": name}
            self.db.table("extractor_dim").insert({"id": _id, "name": name})
            return ExtractorDim(**new_record)

    def fetch_location(self, _id: str) -> LocationDim | None:
        records = self.db.table("location_dim").rows_where(
            "id = :id", {"id": _id}, limit=1
        )

        if record := next(records, None):
            return LocationDim(**record)
        else:
            return None

    def insert_fact(self, fact: Fact) -> str:
        data = fact.dict(exclude={"id"})
        last_pk = (
            self.db.table("fact")
            .insert(data, hash_id="id", hash_id_columns=("content",), replace=True)
            .last_pk
        )
        return last_pk

    def insert_facts(self, facts: Iterable[Fact]) -> int:
        previous_count = self.db.table("fact").count
        data = (fact.dict(exclude={"id"}) for fact in facts)
        self.db.table("fact").insert_all(
            data, hash_id="id", hash_id_columns=("content", "date_id"), replace=True
        )
        return self.db.table("fact").count - previous_count
