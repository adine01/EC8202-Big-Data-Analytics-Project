"""
### Daily consolidated risk report

Triggered by the `LABS_PUBLISHED` asset, i.e. every time `lab_ingest` finishes, so it
runs once per simulated day right after that day's lab file has been handled.

1. **resolve_report_day** - yesterday in simulated time (the day whose vitals are complete
   and whose lab file has just arrived).
2. **wait_for_lab_results** - Kappa means labs reach Postgres asynchronously (Airflow → Kafka →
   Spark → `lab_results`). The sensor waits until the rows Airflow published for the day are
   all in `lab_results`, or the ledger says the file is missing/quarantined. If Spark is slow it
   gives up after a short budget (`soft_fail`: skipped, not failed) and the report is still
   produced, marking the lab status.
3. **build_risk_report** - joins the day's vitals windows, trends and alerts with the latest labs
   (48 h), scores every patient with the shared rules, upserts `daily_risk_report` and writes
   `reports/risk_report_<day>.html` and `.csv`.

Idempotent: re-running a day overwrites the same table rows and files.
ILLUSTRATIVE ONLY - NEWS2-style scoring on synthetic data, not a clinical tool.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import PokeReturnValue, TriggerRule, dag, get_current_context, task

from ward_common.log import configure_logging
from ward_ingest import audit, risk_report
from ward_ingest.assets import LABS_PUBLISHED, RISK_REPORT
from ward_ingest.callbacks import on_failure, on_success

REPORTS_DIR = Path(os.environ.get("REPORTS_DIR", "/opt/airflow/reports"))
POKE_SECONDS = float(os.environ.get("LAB_POKE_SECONDS", "10"))
LABS_WAIT_SECONDS = float(os.environ.get("REPORT_LABS_WAIT_SECONDS", "120"))

log = logging.getLogger("ward_ingest.risk_report")


def _logger() -> logging.Logger:
    configure_logging("airflow-risk-report", logger_name="ward_ingest")
    return log


@dag(
    dag_id="daily_risk_report",
    description="Join vitals trends with the latest labs into a daily per-patient risk report",
    schedule=[LABS_PUBLISHED],
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    tags=["ward", "batch", "report"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
        "on_success_callback": on_success,
        "on_failure_callback": on_failure,
    },
)
def daily_risk_report():

    @task
    def resolve_report_day() -> str:
        return (audit.current_clock().today() - timedelta(days=1)).isoformat()

    @task.sensor(poke_interval=POKE_SECONDS, timeout=LABS_WAIT_SECONDS, mode="reschedule",
                 soft_fail=True, retries=0)
    def wait_for_lab_results(report_day: str) -> PokeReturnValue:
        day = date.fromisoformat(report_day)
        conn = audit.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT status, rows_valid FROM lab_file_loads WHERE file_day = %s", (day,))
                ledger = cur.fetchone()
                if ledger is None:
                    return PokeReturnValue(is_done=False)
                status, rows_valid = ledger
                if status != "loaded":
                    return PokeReturnValue(is_done=True, xcom_value=status)  # nothing to wait for
                cur.execute("SELECT count(*) FROM lab_results WHERE file_day = %s", (day,))
                in_table = cur.fetchone()[0]
        finally:
            conn.close()
        return PokeReturnValue(is_done=in_table >= rows_valid, xcom_value="loaded")

    @task(trigger_rule=TriggerRule.NONE_FAILED, outlets=[RISK_REPORT])
    def build_risk_report(report_day: str) -> dict:
        logger = _logger()
        day = date.fromisoformat(report_day)
        clock = audit.current_clock()
        conn = audit.connect()
        try:
            inputs = risk_report.fetch_inputs(conn, day)
            rows = risk_report.build_report(day, inputs)
            risk_report.upsert_report(conn, rows)
        finally:
            conn.close()

        lab_file = inputs["lab_file"] or {}
        meta = {
            "generated_at_sim": clock.now().strftime("%Y-%m-%d %H:%M"),
            "generated_at_real": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "labs_status": rows[0]["labs_status"] if rows else "not_received",
            "lab_rows_valid": lab_file.get("rows_valid", 0) or 0,
            "lab_rows_rejected": lab_file.get("rows_rejected", 0) or 0,
            "run_id": get_current_context()["run_id"],
        }
        scenarios = {p["patient_id"]: p["scenario"] for p in inputs["patients"]}
        html = risk_report.render_html(day, rows, meta, scenarios)
        paths = risk_report.write_outputs(REPORTS_DIR, day, html, risk_report.render_csv(rows))

        summary = {"report_day": report_day, **risk_report.summarise(rows),
                   "files": [p.name for p in paths], "labs_status": meta["labs_status"]}
        logger.info("risk_report_written", extra=summary)
        return summary

    day = resolve_report_day()
    wait_for_lab_results(day) >> build_risk_report(day)


daily_risk_report()
