"""Structured JSON logging.

Every log line is one JSON object on stdout, so `docker compose logs` output
can be filtered with jq and shipped to any log store without regex parsing.

Usage:
    configure_logging("vitals-simulator")
    log = logging.getLogger(__name__)
    log.info("event_produced", extra={"patient_id": "P001", "topic": "vitals.raw"})

The message is treated as a short snake_case event name; details go in
`extra`, which become top-level JSON fields.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

from ward_common.config import log_level

# Attributes present on every LogRecord; anything else was passed via `extra`.
_STANDARD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {
    "message",
    "asctime",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(
    service: str, level: str | None = None, logger_name: str | None = None
) -> logging.Logger:
    """Attach a JSON stdout handler.

    By default the root logger is configured. Pass `logger_name` inside hosts
    that own the root logger (Airflow tasks, the Spark driver) so their own
    log handling is left untouched.
    """
    logger = logging.getLogger(logger_name)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    logger.addHandler(handler)
    logger.setLevel(level or log_level())
    if logger_name is not None:
        logger.propagate = False
    return logger
