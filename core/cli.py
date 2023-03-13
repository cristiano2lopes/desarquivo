import logging
import logging.config
from datetime import datetime as dt

import click


def validate_day_month(ctx, param, value):
    try:
        # Using 2024 as a year that has february with 29 days
        parsed_date = dt.strptime(f"2024-{value}", "%Y-%m-%d")
        return parsed_date.month, parsed_date.day
    except ValueError:
        raise click.BadParameter(
            f"""Got value '{value}': format should be mm-dd: e.g 01-30 (30 January), 02-29 (29 February)"""
        )


def setup_logs(level=logging.DEBUG):
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "desarquivo": {
                "format": "[%(levelname).1s] [%(asctime)s] [%(module)s] [%(filename)s:%(lineno)s] %(message)s"
            }
        },
        "handlers": {
            "null": {"level": "DEBUG", "class": "logging.NullHandler"},
            "console": {
                "level": level,
                "class": "logging.StreamHandler",
                "formatter": "desarquivo",
            },
        },
        "loggers": {
            "": {
                "handlers": ["console"],
                "level": "INFO",
                "propagate": False,
            },
            "httpx": {
                "handlers": ["console"],
                "level": "DEBUG",
                "propagate": False,
            },
        },
    }
    logging.config.dictConfig(log_config)
