"""Task callbacks that record every task outcome in `pipeline_runs`.

Airflow tasks are short-lived, so Prometheus cannot scrape them directly.
Recording outcomes in Postgres lets the API expose DAG success/failure and
duration as metrics (see docs/architecture_decision.md, section 10).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from ward_ingest.audit import record_task

log = logging.getLogger("ward_ingest.callbacks")


def _record(context, state: str) -> None:
    try:
        ti = context["ti"]
        exception = context.get("exception")
        record_task(
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            try_number=ti.try_number,
            state=state,
            started_at=getattr(ti, "start_date", None),
            ended_at=datetime.now(timezone.utc),
            error=f"{type(exception).__name__}: {exception}"[:500] if exception else None,
        )
    except Exception:  # a monitoring hiccup must never change the task's outcome
        log.warning("pipeline_run_not_recorded", exc_info=True)


def on_success(context) -> None:
    _record(context, "success")


def on_failure(context) -> None:
    _record(context, "failed")
