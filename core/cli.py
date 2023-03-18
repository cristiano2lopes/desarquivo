import logging
import logging.config
import pendulum

import click


def validate_day_month(day, month):
    try:
        # Using 2024 as a year that has february with 29 days
        parsed_date = pendulum.from_format(f"2024-{month}-{day}", "YYYY-M-D")
        return parsed_date.month, parsed_date.day
    except ValueError:
        raise click.BadOptionUsage(
            "-d", f"Option -d {day} invalid for provided -m {month}."
        )


def validate_day(ctx, param, value: int):
    if value is not None and not 0 < value < 32:
        raise click.BadParameter(f"""Got value '{value}': Invalid day of month""")
    return value


def validate_month(ctx, param, value: int):
    if not 0 < value < 13:
        raise click.BadParameter(f"""Got value '{value}': Invalid month""")
    return value


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
