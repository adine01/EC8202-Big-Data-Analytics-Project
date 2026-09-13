"""Pathology simulator: drops one CSV of lab results per simulated day.

    python -m ward_sim.lab_generator                        # service: follow the sim clock
    python -m ward_sim.lab_generator --day 2026-01-03       # write one day now, then exit
    python -m ward_sim.lab_generator --day 2026-01-03 --dry-run   # print CSV only

The file for day D (results collected on D) is uploaded at LAB_UPLOAD_HOUR on
D+1, i.e. "yesterday's labs arrive this morning". Some days it is late, and
occasionally it never arrives, so the missing/late-file alert has something
real to detect.
"""

from __future__ import annotations

import argparse
import calendar
import logging
import os
import signal
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from ward_common.config import ClockSettings, PostgresSettings
from ward_common.log import configure_logging
from ward_common.schemas import lab_file_name
from ward_common.sim_clock import ClockReader

from ward_sim.labs import LabDayGenerator, inject_bad_rows, render_csv, upload_plan, write_lab_file
from ward_sim.patients import build_roster

log = logging.getLogger("ward_sim.lab_generator")


def _env(name: str, default, cast=float):
    raw = os.environ.get(name)
    return cast(raw) if raw not in (None, "") else default


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    # Must match the vitals simulator so both describe the same patients.
    p.add_argument("--patients", type=int, default=_env("SIM_PATIENTS", 20, int))
    p.add_argument("--seed", type=int, default=_env("SIM_SEED", 42, int))
    p.add_argument("--deteriorating-fraction", type=float, default=_env("SIM_DETERIORATING_FRACTION", 0.5))

    p.add_argument("--data-dir", type=Path, default=Path(os.environ.get("DATA_DIR", "/data")),
                   help="contains landing/, archive/ and quarantine/")
    p.add_argument("--upload-hour", type=float, default=_env("LAB_UPLOAD_HOUR", 6.0),
                   help="simulated hour on D+1 when day D's file is normally uploaded")
    p.add_argument("--late-rate", type=float, default=_env("LAB_LATE_RATE", 0.10))
    p.add_argument("--late-max-hours", type=float, default=_env("LAB_LATE_MAX_HOURS", 8.0))
    p.add_argument("--missing-rate", type=float, default=_env("LAB_MISSING_RATE", 0.05))
    p.add_argument("--bad-row-rate", type=float, default=_env("LAB_BAD_ROW_RATE", 0.03))
    p.add_argument("--max-backfill-days", type=int, default=_env("LAB_MAX_BACKFILL_DAYS", 2, int),
                   help="on start-up, also deliver up to this many overdue past days")
    p.add_argument("--poll-seconds", type=float, default=2.0)
    p.add_argument("--metrics-port", type=int, default=_env("LAB_METRICS_PORT", 8002, int))

    p.add_argument("--day", type=date.fromisoformat, help="generate this simulated day once and exit")
    p.add_argument("--force", action="store_true", help="with --day: overwrite an existing file")
    p.add_argument("--dry-run", action="store_true", help="with --day: print the CSV instead of writing it")
    args = p.parse_args(argv)
    if args.dry_run and args.day is None:
        p.error("--dry-run requires --day")
    return args


class Metrics:
    def __init__(self, enabled: bool, port: int) -> None:
        from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

        r = self.registry = CollectorRegistry()
        self.files = Counter("lab_files_written_total", "Lab files dropped into landing/", registry=r)
        self.rows = Counter("lab_rows_written_total", "Rows written, by injected quality", ["quality"], registry=r)
        self.withheld = Counter("lab_files_withheld_total", "Days whose file was never uploaded", registry=r)
        self.last_day = Gauge("lab_last_file_sim_day_seconds", "Simulated day of the newest file (epoch s)",
                              registry=r)
        self.last_written = Gauge("lab_last_file_written_timestamp_seconds", "Real time the newest file was written",
                                  registry=r)
        self.last_delay = Gauge("lab_last_file_upload_delay_sim_hours", "Hours after the scheduled upload time",
                                registry=r)
        if enabled:
            start_http_server(port, registry=r)


class LabSimulator:
    def __init__(self, args: argparse.Namespace, metrics: Metrics) -> None:
        self.args = args
        self.metrics = metrics
        self.sim_start = ClockSettings.from_env().sim_start_date
        roster = build_roster(args.patients, args.seed, self.sim_start, args.deteriorating_fraction)
        self.patient_ids = [p.patient_id for p in roster]
        self.generator = LabDayGenerator(roster)
        self.landing = args.data_dir / "landing"
        self.seen_dirs = [self.landing, args.data_dir / "archive", args.data_dir / "quarantine"]
        self.withheld: set[date] = set()
        self.running = True

    def stop(self, *_: object) -> None:
        self.running = False

    def already_delivered(self, day: date) -> bool:
        """A file counts as delivered once it is in landing/, or Airflow has archived/quarantined it."""
        stem = lab_file_name(day)[: -len(".csv")]
        return any(any(d.glob(f"{stem}*.csv")) for d in self.seen_dirs if d.exists())

    def build_file(self, day: date) -> tuple[list[dict], list[str]]:
        rows = self.generator.rows_for_day(day)
        return inject_bad_rows(rows, day, self.args.seed, self.args.bad_row_rate)

    def deliver(self, day: date, delay_hours: float = 0.0) -> Path:
        rows, faults = self.build_file(day)
        path = write_lab_file(self.landing, day, rows)
        self.metrics.files.inc()
        self.metrics.rows.labels(quality="good").inc(len(rows) - len(faults))
        self.metrics.rows.labels(quality="bad").inc(len(faults))
        self.metrics.last_day.set(calendar.timegm(day.timetuple()))
        self.metrics.last_written.set(time.time())
        self.metrics.last_delay.set(delay_hours)
        log.info(
            "lab_file_written",
            extra={"file": path.name, "sim_day": day.isoformat(), "rows": len(rows),
                   "bad_rows": len(faults), "fault_kinds": sorted(set(faults)),
                   "upload_delay_sim_hours": round(delay_hours, 2)},
        )
        return path

    def tick(self, sim_now) -> None:
        yesterday = sim_now.date() - timedelta(days=1)
        first = max(self.sim_start, yesterday - timedelta(days=self.args.max_backfill_days))
        day = first
        while day <= yesterday:
            if day not in self.withheld and not self.already_delivered(day):
                plan = upload_plan(day, self.args.seed, self.args.upload_hour, self.args.late_rate,
                                   self.args.missing_rate, self.args.late_max_hours)
                if plan.missing:
                    self.withheld.add(day)
                    self.metrics.withheld.inc()
                    log.warning("lab_file_withheld", extra={"sim_day": day.isoformat(),
                                                            "reason": "simulated missing upload"})
                elif sim_now >= plan.upload_at:
                    self.deliver(day, plan.delay_hours)
            day += timedelta(days=1)

    def run(self, clock_reader: ClockReader) -> None:
        log.info("lab_simulator_started",
                 extra={"patients": len(self.patient_ids), "landing": str(self.landing),
                        "upload_hour": self.args.upload_hour, "late_rate": self.args.late_rate,
                        "missing_rate": self.args.missing_rate, "bad_row_rate": self.args.bad_row_rate})
        while self.running:
            self.tick(clock_reader.now())
            time.sleep(self.args.poll_seconds)
        log.info("lab_simulator_stopped")


def _warn_if_roster_differs(connect, patient_ids: list[str]) -> None:
    """Both simulators rebuild the roster from the same seed; flag config drift early."""
    with connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT patient_id FROM patients WHERE active ORDER BY patient_id")
        in_db = [row[0] for row in cur.fetchall()]
    if in_db and in_db != patient_ids:
        log.warning("roster_mismatch_with_vitals_simulator",
                    extra={"db_patients": len(in_db), "lab_patients": len(patient_ids)})


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging("lab-simulator")

    if args.day is not None:
        # One-shot mode (demo / manual re-delivery): ignores the upload plan.
        sim = LabSimulator(args, Metrics(enabled=False, port=0))
        if args.dry_run:
            rows, _ = sim.build_file(args.day)
            sys.stdout.write(render_csv(rows))
            return 0
        if sim.already_delivered(args.day) and not args.force:
            log.warning("lab_file_exists", extra={"sim_day": args.day.isoformat(), "hint": "use --force"})
            return 1
        sim.deliver(args.day)
        return 0

    import psycopg

    pg = PostgresSettings.from_env()
    connect = lambda: psycopg.connect(**pg.connect_kwargs(), connect_timeout=5)  # noqa: E731
    reader = ClockReader(connect, ClockSettings.from_env().refresh_seconds)
    reader.wait_until_ready()

    sim = LabSimulator(args, Metrics(enabled=True, port=args.metrics_port))
    try:
        _warn_if_roster_differs(connect, sim.patient_ids)
    except Exception:
        log.warning("roster_check_skipped", exc_info=True)

    signal.signal(signal.SIGTERM, sim.stop)
    signal.signal(signal.SIGINT, sim.stop)
    sim.run(reader)
    return 0


if __name__ == "__main__":
    sys.exit(main())
