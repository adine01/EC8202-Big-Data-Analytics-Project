"""Persist the simulated roster into the `patients` dimension table."""

from __future__ import annotations

from typing import Any

from ward_sim.patients import Patient

_UPSERT = """
    INSERT INTO patients (
        patient_id, bed, age, sex,
        baseline_heart_rate, baseline_spo2, baseline_systolic, baseline_diastolic, baseline_temp,
        scenario, active, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, true, now())
    ON CONFLICT (patient_id) DO UPDATE SET
        bed = EXCLUDED.bed,
        age = EXCLUDED.age,
        sex = EXCLUDED.sex,
        baseline_heart_rate = EXCLUDED.baseline_heart_rate,
        baseline_spo2 = EXCLUDED.baseline_spo2,
        baseline_systolic = EXCLUDED.baseline_systolic,
        baseline_diastolic = EXCLUDED.baseline_diastolic,
        baseline_temp = EXCLUDED.baseline_temp,
        scenario = EXCLUDED.scenario,
        active = true,
        updated_at = now()
"""


def sync_roster(conn: Any, roster: list[Patient]) -> None:
    """Upsert the current roster and deactivate patients no longer simulated (one transaction)."""
    rows = [
        (
            p.patient_id, p.bed, p.age, p.sex,
            p.baseline["heart_rate"], p.baseline["spo2"], p.baseline["systolic_bp"],
            p.baseline["diastolic_bp"], p.baseline["temperature"],
            p.scenario,
        )
        for p in roster
    ]
    with conn.cursor() as cur:
        cur.executemany(_UPSERT, rows)
        cur.execute(
            "UPDATE patients SET active = false, updated_at = now() "
            "WHERE active AND NOT (patient_id = ANY(%s))",
            ([p.patient_id for p in roster],),
        )
    conn.commit()
