"""Database access for the ingestion DAG: roster, clock, load ledger and task outcomes.

Uses psycopg2, which already ships in the Airflow image (no extra dependency).
"""

from __future__ import annotations

import json
from datetime import date, datetime

import psycopg2

from ward_common.config import PostgresSettings
from ward_common.sim_clock import SimClock, fetch_clock

from ward_ingest.lab_files import FileCheck


def connect():
    return psycopg2.connect(**PostgresSettings.from_env().connect_kwargs(), connect_timeout=10)


def current_clock() -> SimClock:
    conn = connect()
    try:
        clock = fetch_clock(conn)
    finally:
        conn.close()
    if clock is None:
        raise RuntimeError("sim_clock is not initialised")
    return clock


def known_patients(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT patient_id FROM patients WHERE active")
        return {row[0] for row in cur.fetchall()}


def load_row(conn, day: date) -> dict | None:
    with conn.cursor() as cur:
        cur.execute("SELECT status, checksum, arrival FROM lab_file_loads WHERE file_day = %s", (day,))
        row = cur.fetchone()
    return None if row is None else {"status": row[0], "checksum": row[1], "arrival": row[2]}


def record_missing(conn, day: date, file_name: str, expected_at: datetime, run_id: str) -> None:
    """Mark a day missing, but never downgrade a day that has already been loaded."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO lab_file_loads (file_day, file_name, status, arrival, expected_at, dag_run_id)
            VALUES (%s, %s, 'missing', 'missing', %s, %s)
            ON CONFLICT (file_day) DO UPDATE
               SET dag_run_id = EXCLUDED.dag_run_id, updated_at = now()
             WHERE lab_file_loads.status = 'missing'
            """,
            (day, file_name, expected_at, run_id),
        )
    conn.commit()


def record_duplicate(conn, day: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE lab_file_loads SET duplicate_deliveries = duplicate_deliveries + 1, updated_at = now() "
            "WHERE file_day = %s",
            (day,),
        )
    conn.commit()


def record_result(conn, check: FileCheck, status: str, arrival: str, expected_at: datetime,
                  arrived_at: datetime, run_id: str) -> None:
    published = 1 if status == "loaded" else 0
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO lab_file_loads (
                file_day, file_name, status, arrival, expected_at, arrived_at, checksum,
                rows_total, rows_valid, rows_rejected, reject_rate, reject_reasons,
                deliveries, dag_run_id, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (file_day) DO UPDATE SET
                file_name = EXCLUDED.file_name, status = EXCLUDED.status, arrival = EXCLUDED.arrival,
                arrived_at = EXCLUDED.arrived_at, checksum = EXCLUDED.checksum,
                rows_total = EXCLUDED.rows_total, rows_valid = EXCLUDED.rows_valid,
                rows_rejected = EXCLUDED.rows_rejected, reject_rate = EXCLUDED.reject_rate,
                reject_reasons = EXCLUDED.reject_reasons,
                deliveries = lab_file_loads.deliveries + EXCLUDED.deliveries,
                dag_run_id = EXCLUDED.dag_run_id, loaded_at = now(), updated_at = now()
            """,
            (check.file_day, check.path.name, status, arrival, expected_at, arrived_at, check.checksum,
             check.total, len(check.valid), len(check.rejected), round(check.reject_rate, 4),
             json.dumps(check.reason_counts), published, run_id),
        )
    conn.commit()


def record_task(dag_id: str, run_id: str, task_id: str, try_number: int, state: str,
                started_at: datetime | None, ended_at: datetime, error: str | None) -> None:
    duration = (ended_at - started_at).total_seconds() if started_at else None
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (dag_id, run_id, task_id, try_number, state, started_at, ended_at, duration_s, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dag_id, run_id, task_id, try_number) DO UPDATE
                   SET state = EXCLUDED.state, ended_at = EXCLUDED.ended_at,
                       duration_s = EXCLUDED.duration_s, error = EXCLUDED.error, recorded_at = now()
                """,
                (dag_id, run_id, task_id, try_number, state, started_at, ended_at, duration, error),
            )
        conn.commit()
    finally:
        conn.close()
