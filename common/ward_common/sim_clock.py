"""Simulated clock shared by every component.

    sim_time = anchor_sim + (real_time - anchor_real) * speedup
    speedup  = 86400 / SIM_DAY_SECONDS          (288 by default: 1 sim day = 5 real min)

The anchor lives in the Postgres table `sim_clock` (single row), so every
container computes the same simulated time. Rules:

* `init` on an empty table anchors SIM_START_DATE 00:00 to "now".
* `init` on an existing row re-anchors at the *current* simulated time, so the
  clock never jumps, even if SIM_DAY_SECONDS changed between runs.
* `pause` freezes the clock (used by `make down`), so a stopped stack does not
  "lose" simulated days while it is offline.

Run as a CLI:  python -m ward_common.sim_clock {init|pause|show}
"""

from __future__ import annotations

import json
import logging
import sys
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

SECONDS_PER_DAY = 86_400

log = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _require_aware(name: str, value: datetime | None) -> None:
    if value is not None and value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware (UTC)")


@dataclass(frozen=True)
class SimClock:
    anchor_real: datetime
    anchor_sim: datetime
    sim_day_seconds: float
    paused_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.sim_day_seconds <= 0:
            raise ValueError("sim_day_seconds must be positive")
        _require_aware("anchor_real", self.anchor_real)
        _require_aware("anchor_sim", self.anchor_sim)
        _require_aware("paused_at", self.paused_at)

    @classmethod
    def starting(cls, start_date: date, sim_day_seconds: float, real_now: datetime) -> SimClock:
        start = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
        return cls(anchor_real=real_now, anchor_sim=start, sim_day_seconds=sim_day_seconds)

    @property
    def speedup(self) -> float:
        return SECONDS_PER_DAY / self.sim_day_seconds

    @property
    def is_paused(self) -> bool:
        return self.paused_at is not None

    # --- conversions -------------------------------------------------------

    def to_sim(self, real: datetime) -> datetime:
        _require_aware("real", real)
        if self.paused_at is not None and real > self.paused_at:
            real = self.paused_at
        return self.anchor_sim + (real - self.anchor_real) * self.speedup

    def to_real(self, sim: datetime) -> datetime:
        _require_aware("sim", sim)
        return self.anchor_real + (sim - self.anchor_sim) / self.speedup

    def sim_delta(self, real_seconds: float) -> timedelta:
        """Simulated duration that elapses during `real_seconds` of wall time."""
        return timedelta(seconds=real_seconds * self.speedup)

    def real_seconds(self, sim_duration: timedelta) -> float:
        """Wall-clock seconds needed for `sim_duration` of simulated time."""
        return sim_duration.total_seconds() / self.speedup

    def now(self) -> datetime:
        return self.to_sim(utcnow())

    def today(self) -> date:
        return self.now().date()

    # --- state transitions -------------------------------------------------

    def reanchored(self, real_now: datetime, sim_day_seconds: float | None = None) -> SimClock:
        """Continue from the current simulated time, un-paused, optionally at a new speed."""
        return SimClock(
            anchor_real=real_now,
            anchor_sim=self.to_sim(real_now),
            sim_day_seconds=sim_day_seconds or self.sim_day_seconds,
            paused_at=None,
        )

    def paused(self, real_now: datetime) -> SimClock:
        return self if self.is_paused else replace(self, paused_at=real_now)


# --- persistence (DB-API 2.0: works with psycopg 3 and psycopg2) -------------

_SELECT_SQL = (
    "SELECT anchor_real, anchor_sim, sim_day_seconds, paused_at "
    "FROM sim_clock WHERE id = 1"
)
_UPSERT_SQL = """
    INSERT INTO sim_clock (id, anchor_real, anchor_sim, sim_day_seconds, paused_at, updated_at)
    VALUES (1, %s, %s, %s, %s, now())
    ON CONFLICT (id) DO UPDATE SET
        anchor_real = EXCLUDED.anchor_real,
        anchor_sim = EXCLUDED.anchor_sim,
        sim_day_seconds = EXCLUDED.sim_day_seconds,
        paused_at = EXCLUDED.paused_at,
        updated_at = now()
"""


def _row_to_clock(row: tuple) -> SimClock:
    anchor_real, anchor_sim, day_seconds, paused_at = row
    return SimClock(
        anchor_real=anchor_real,
        anchor_sim=anchor_sim,
        sim_day_seconds=float(day_seconds),
        paused_at=paused_at,
    )


def fetch_clock(conn: Any, *, for_update: bool = False) -> SimClock | None:
    with conn.cursor() as cur:
        cur.execute(_SELECT_SQL + (" FOR UPDATE" if for_update else ""))
        row = cur.fetchone()
    return _row_to_clock(row) if row else None


def _save(conn: Any, clock: SimClock) -> None:
    with conn.cursor() as cur:
        cur.execute(
            _UPSERT_SQL,
            (clock.anchor_real, clock.anchor_sim, clock.sim_day_seconds, clock.paused_at),
        )


def init_clock(conn: Any, start_date: date, sim_day_seconds: float) -> SimClock:
    """Create the clock, or resume an existing one without a time jump."""
    real_now = utcnow()
    existing = fetch_clock(conn, for_update=True)
    if existing is None:
        clock = SimClock.starting(start_date, sim_day_seconds, real_now)
    else:
        clock = existing.reanchored(real_now, sim_day_seconds)
    _save(conn, clock)
    conn.commit()
    return clock


def pause_clock(conn: Any) -> SimClock | None:
    existing = fetch_clock(conn, for_update=True)
    if existing is None:
        conn.rollback()
        return None
    clock = existing.paused(utcnow())
    _save(conn, clock)
    conn.commit()
    return clock


class ClockReader:
    """Cached, periodically refreshed view of the shared clock.

    Long-running components (simulators, Spark driver, API) read the anchor
    once and re-read it every `refresh_seconds`, so a re-anchor by `init` is
    picked up without a restart and without a DB round-trip per event.
    """

    def __init__(
        self,
        connect: Callable[[], Any],
        refresh_seconds: float = 30.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._connect = connect
        self._refresh_seconds = refresh_seconds
        self._monotonic = monotonic
        self._clock: SimClock | None = None
        self._loaded_at = float("-inf")

    def _load(self) -> SimClock | None:
        conn = self._connect()
        try:
            return fetch_clock(conn)
        finally:
            conn.close()

    def clock(self) -> SimClock:
        if self._monotonic() - self._loaded_at >= self._refresh_seconds:
            try:
                loaded = self._load()
                if loaded is not None:
                    self._clock = loaded
                    self._loaded_at = self._monotonic()
            except Exception:  # keep serving the last known clock during a DB blip
                if self._clock is None:
                    raise
                log.warning("sim_clock_refresh_failed", exc_info=True)
        if self._clock is None:
            raise RuntimeError("sim_clock table is empty; run `python -m ward_common.sim_clock init`")
        return self._clock

    def now(self) -> datetime:
        return self.clock().now()

    def wait_until_ready(self, timeout_seconds: float = 120.0, poll_seconds: float = 2.0) -> SimClock:
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                return self.clock()
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                log.info("waiting_for_sim_clock")
                self._loaded_at = float("-inf")
                time.sleep(poll_seconds)


def describe(clock: SimClock) -> dict:
    return {
        "sim_now": clock.now().isoformat(),
        "sim_date": clock.today().isoformat(),
        "speedup": clock.speedup,
        "sim_day_seconds": clock.sim_day_seconds,
        "anchor_real": clock.anchor_real.isoformat(),
        "anchor_sim": clock.anchor_sim.isoformat(),
        "paused_at": clock.paused_at.isoformat() if clock.paused_at else None,
    }


def main(argv: list[str] | None = None) -> int:
    import psycopg  # imported here so the pure clock maths has no dependencies

    from ward_common.config import ClockSettings, PostgresSettings
    from ward_common.log import configure_logging

    configure_logging("sim-clock")
    args = argv if argv is not None else sys.argv[1:]
    command = args[0] if args else "show"
    pg = PostgresSettings.from_env()

    with psycopg.connect(**pg.connect_kwargs()) as conn:
        if command == "init":
            settings = ClockSettings.from_env()
            clock = init_clock(conn, settings.sim_start_date, settings.sim_day_seconds)
            log.info("sim_clock_initialised", extra=describe(clock))
        elif command == "pause":
            clock = pause_clock(conn)
            if clock is None:
                log.warning("sim_clock_missing_nothing_to_pause")
                return 0
            log.info("sim_clock_paused", extra=describe(clock))
        elif command == "show":
            clock = fetch_clock(conn)
            if clock is None:
                log.error("sim_clock_missing")
                return 1
            print(json.dumps(describe(clock), indent=2))
        else:
            print(f"unknown command {command!r}; use init | pause | show", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
