"""Environment-driven configuration.

Each component loads only the sections it needs (e.g. the API never needs
Kafka settings), so a missing variable fails fast in the component that
actually depends on it instead of everywhere.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date


class ConfigError(RuntimeError):
    """Raised when a required environment variable is missing or invalid."""


def _env(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        if required:
            raise ConfigError(f"Missing required environment variable: {name}")
        return default
    return value.strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number, got {raw!r}") from exc


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic_vitals: str
    topic_labs: str
    topic_alerts: str
    topic_deadletter: str

    @classmethod
    def from_env(cls) -> KafkaSettings:
        return cls(
            bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            topic_vitals=_env("TOPIC_VITALS", "vitals.raw"),
            topic_labs=_env("TOPIC_LABS", "labs.raw"),
            topic_alerts=_env("TOPIC_ALERTS", "alerts.patient"),
            topic_deadletter=_env("TOPIC_DEADLETTER", "deadletter"),
        )


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    dbname: str
    user: str
    # repr=False keeps the password out of logs and tracebacks.
    password: str = field(repr=False)

    @classmethod
    def from_env(cls) -> PostgresSettings:
        return cls(
            host=_env("WARD_DB_HOST", "postgres"),
            port=_env_int("WARD_DB_PORT", 5432),
            dbname=_env("WARD_DB_NAME", "ward"),
            user=_env("WARD_DB_USER", "ward"),
            password=_env("WARD_DB_PASSWORD", required=True),
        )

    def connect_kwargs(self) -> dict:
        """Keyword arguments accepted by both psycopg (3) and psycopg2."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }


@dataclass(frozen=True)
class ClockSettings:
    sim_day_seconds: float
    sim_start_date: date
    refresh_seconds: float

    @classmethod
    def from_env(cls) -> ClockSettings:
        day_seconds = _env_float("SIM_DAY_SECONDS", 300.0)
        if day_seconds <= 0:
            raise ConfigError("SIM_DAY_SECONDS must be positive")
        raw_start = _env("SIM_START_DATE", "2026-01-01")
        try:
            start = date.fromisoformat(raw_start)
        except ValueError as exc:
            raise ConfigError(f"SIM_START_DATE must be YYYY-MM-DD, got {raw_start!r}") from exc
        return cls(
            sim_day_seconds=day_seconds,
            sim_start_date=start,
            refresh_seconds=_env_float("SIM_CLOCK_REFRESH_SECONDS", 30.0),
        )


def log_level() -> str:
    return (_env("LOG_LEVEL", "INFO") or "INFO").upper()
