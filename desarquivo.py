import datetime

import click

from arquivo import *
from core import *
from data import *
from extractor import setup_extractors
from extractor.core import *

setup_logs()

logger = logging.getLogger(__name__)


async def run(params: ExtractionParams, _db):
    async with ArquivoClient() as arquivo_client:
        arquivo = Arquivo(arquivo_client=arquivo_client)
        repository = Repository(_db)
        await ExtractionJob(arquivo, repository, params).run()


@click.command()
@click.option("-dm", "--day-month", required=True, callback=validate_day_month)
@click.option("-sy", "--start-year", default=lambda: datetime.date.today().year - 15)
@click.option("-ey", "--end-year", default=lambda: datetime.date.today().year - 1)
@click.option("--recreate-db/--no-recreate-db'", default=False)
def cli(day_month, start_year, end_year, recreate_db):
    """Extracts facts for past days from arquivo.pt and other sources
    saving them on a facts database."""

    with DbContextManager(recreate_db) as _db:
        extractors = setup_extractors()
        month, day = day_month
        params = ExtractionParams(
            month, day, start_year, end_year, extractors
        )
        asyncio.run(run(params, _db))


if __name__ == "__main__":
    cli()
