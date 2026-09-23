"""
### Lab ingestion (Kappa batch adapter)

Runs once per **simulated day** (every `SIM_DAY_SECONDS` real seconds).

1. **resolve_target_day** - yesterday in simulated time; its file is due at
   `LAB_UPLOAD_HOUR` today (sim time).
2. **wait_for_lab_file** - sensor (reschedule mode, frees the worker slot between
   pokes). Succeeds when the file is in `landing/` (or was already loaded).
   If the simulated SLA deadline (`LAB_UPLOAD_HOUR + LAB_SLA_HOURS`) passes first,
   the day is recorded as **missing** in `lab_file_loads` and the task fails
   without retries - the visible signal behind the missing/late-file alert.
3. **load_landing_files** - runs even if the sensor failed (`all_done`), and
   processes *every* file waiting in `landing/`, so a late file or a manual
   re-delivery is picked up by the next run:
   checksum → header check → row DQ (shared validator) → decide →
   valid rows to `labs.raw`, rejected rows to `deadletter` → ledger → archive/quarantine.

Kappa: nothing here writes lab results to Postgres. The Spark `labs` query consumes
`labs.raw` and upserts `lab_results`, so a replay of the topic rebuilds the table.

Idempotency: an identical re-delivery (same sha256) is detected in `lab_file_loads`
and not published again; a corrected re-delivery is republished and Spark's upsert
on (sample_id, test_type) overwrites the old values.
"""

from __future__ import annotations

import logging
import os
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.sdk import PokeReturnValue, TriggerRule, dag, get_current_context, task

from ward_common.config import KafkaSettings
from ward_common.log import configure_logging
from ward_common.schemas import lab_file_name
from ward_ingest import audit
from ward_ingest.assets import LABS_PUBLISHED
from ward_ingest.callbacks import on_failure, on_success
from ward_ingest.kafka_publish import publish
from ward_ingest.lab_files import check_file, classify_arrival, deadletter_event, decide, expected_upload, lab_event

SIM_DAY_SECONDS = float(os.environ.get("SIM_DAY_SECONDS", "300"))
DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
UPLOAD_HOUR = float(os.environ.get("LAB_UPLOAD_HOUR", "6"))
SLA_HOURS = float(os.environ.get("LAB_SLA_HOURS", "4"))
MAX_REJECT_RATE = float(os.environ.get("LAB_MAX_REJECT_RATE", "0.2"))
POKE_SECONDS = float(os.environ.get("LAB_POKE_SECONDS", "10"))

LANDING, ARCHIVE, QUARANTINE = DATA_DIR / "landing", DATA_DIR / "archive", DATA_DIR / "quarantine"

log = logging.getLogger("ward_ingest.lab_ingest")


def _logger() -> logging.Logger:
    # Attach our JSON handler to our own logger only; Airflow owns the root logger.
    configure_logging("airflow-lab-ingest", logger_name="ward_ingest")
    return log


def _move(src: Path, dest_dir: Path) -> Path:
    """Move without overwriting: a second version of a day is kept next to the first."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    version = 2
    while dest.exists():
        dest = dest_dir / f"{src.stem}.v{version}{src.suffix}"
        version += 1
    shutil.move(src, dest)
    return dest


@dag(
    dag_id="lab_ingest",
    description="Sense, validate and publish the daily lab file into Kafka",
    schedule=timedelta(seconds=SIM_DAY_SECONDS),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    tags=["ward", "batch", "kappa"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
        "on_success_callback": on_success,
        "on_failure_callback": on_failure,
    },
)
def lab_ingest():

    @task
    def resolve_target_day() -> str:
        clock = audit.current_clock()
        target = clock.today() - timedelta(days=1)
        _logger().info("target_day_resolved", extra={"sim_now": clock.now().isoformat(), "target_day": target.isoformat()})
        return target.isoformat()

    @task.sensor(poke_interval=POKE_SECONDS, timeout=2 * SIM_DAY_SECONDS, mode="reschedule",
                 retries=0)
    def wait_for_lab_file(target_day: str) -> PokeReturnValue:
        day = date.fromisoformat(target_day)
        name = lab_file_name(day)
        if (LANDING / name).exists():
            return PokeReturnValue(is_done=True, xcom_value=name)

        conn = audit.connect()
        try:
            loaded = audit.load_row(conn, day)
            if loaded and loaded["status"] != "missing":
                return PokeReturnValue(is_done=True, xcom_value=name)  # handled by an earlier run
            clock = audit.current_clock()
            deadline = expected_upload(day, UPLOAD_HOUR) + timedelta(hours=SLA_HOURS)
            if clock.now() > deadline:
                audit.record_missing(conn, day, name, expected_upload(day, UPLOAD_HOUR),
                                     get_current_context()["run_id"])
                _logger().error("lab_file_missing", extra={"file": name, "sim_deadline": deadline.isoformat(),
                                                           "sim_now": clock.now().isoformat()})
                raise AirflowFailException(f"{name} not received by simulated deadline {deadline:%Y-%m-%d %H:%M}")
        finally:
            conn.close()
        return PokeReturnValue(is_done=False)

    @task(trigger_rule=TriggerRule.ALL_DONE, outlets=[LABS_PUBLISHED])
    def load_landing_files() -> list[dict]:
        logger = _logger()
        run_id = get_current_context()["run_id"]
        kafka = KafkaSettings.from_env()
        clock = audit.current_clock()
        summaries = []

        conn = audit.connect()
        try:
            roster = audit.known_patients(conn)
            for path in sorted(LANDING.glob("labs_*.csv")):
                check = check_file(path, roster)
                if check.file_day is None:
                    dest = _move(path, QUARANTINE)
                    logger.error("lab_file_unrecognised_name", extra={"file": path.name, "moved_to": str(dest)})
                    continue

                previous = audit.load_row(conn, check.file_day)
                if previous and previous["checksum"] == check.checksum and previous["status"] == "loaded":
                    audit.record_duplicate(conn, check.file_day)
                    dest = _move(path, ARCHIVE)
                    logger.info("lab_file_duplicate_skipped", extra={"file": path.name, "checksum": check.checksum[:12]})
                    summaries.append({"file": path.name, "outcome": "duplicate"})
                    continue

                expected_at = expected_upload(check.file_day, UPLOAD_HOUR)
                arrived_at = clock.to_sim(datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc))
                arrival = classify_arrival(arrived_at, expected_at)
                outcome = decide(check, MAX_REJECT_RATE)

                if outcome == "quarantine" and previous and previous["status"] == "loaded":
                    # A bad re-delivery must not discard a day that already loaded cleanly.
                    dest = _move(path, QUARANTINE)
                    logger.warning("lab_file_redelivery_quarantined",
                                   extra={"file": path.name, "reject_rate": round(check.reject_rate, 4),
                                          "moved_to": str(dest.relative_to(DATA_DIR))})
                    summaries.append({"file": path.name, "outcome": "redelivery_quarantined"})
                    continue

                if outcome == "publish":
                    now = datetime.now(timezone.utc)
                    messages = [(kafka.topic_labs, r["patient_id"].strip(), lab_event(r, check)) for r in check.valid]
                    messages += [(kafka.topic_deadletter, "labs", deadletter_event(r, check, now)) for r in check.rejected]
                    publish(kafka.bootstrap_servers, messages)
                    status, dest_dir = "loaded", ARCHIVE
                else:
                    status, dest_dir = "quarantined", QUARANTINE

                # Ledger after publishing, move last: a crash in between re-runs the
                # publish (harmless, consumers upsert) instead of losing the file.
                audit.record_result(conn, check, status, arrival, expected_at, arrived_at, run_id)
                dest = _move(path, dest_dir)
                summary = {
                    "file": path.name, "outcome": status, "arrival": arrival, "rows": check.total,
                    "valid": len(check.valid), "rejected": len(check.rejected),
                    "reject_rate": round(check.reject_rate, 4), "reasons": check.reason_counts,
                    "moved_to": str(dest.relative_to(DATA_DIR)),
                }
                (logger.warning if status == "quarantined" else logger.info)("lab_file_processed", extra=summary)
                summaries.append(summary)
        finally:
            conn.close()

        if not summaries:
            logger.info("no_lab_files_waiting")
        return summaries

    target = resolve_target_day()
    wait_for_lab_file(target) >> load_landing_files()


lab_ingest()
