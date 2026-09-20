"""All SQL used by the API, behind one class.

Routers depend on `Repository` through FastAPI dependency injection, so tests
swap in an in-memory fake and never need a database.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class Repository:
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def _all(self, sql: str, params: dict | None = None) -> list[dict[str, Any]]:
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()

    def _one(self, sql: str, params: dict | None = None) -> dict[str, Any] | None:
        rows = self._all(sql, params)
        return rows[0] if rows else None

    # --- health -----------------------------------------------------------------

    def ping(self) -> None:
        self._one("SELECT 1 AS ok")

    # --- ward -------------------------------------------------------------------

    def ward_summary(self, sim_now: datetime, active_since: datetime) -> dict[str, Any]:
        tiers = {r["risk_tier"]: r["n"] for r in self._all(
            "SELECT risk_tier, count(*) AS n FROM patient_live_status s "
            "JOIN patients p USING (patient_id) WHERE p.active GROUP BY 1")}
        alerts = {r["severity"]: r["n"] for r in self._all(
            "SELECT severity, count(*) AS n FROM alerts "
            "WHERE NOT acknowledged AND window_end >= %(since)s GROUP BY 1", {"since": active_since})}
        vitals = self._one(
            "SELECT count(*) AS patients_reporting, max(last_event_time) AS last_event_time, "
            "round(avg(hr_avg)::numeric, 1)::float AS hr_avg, round(avg(spo2_avg)::numeric, 1)::float AS spo2_avg, "
            "round(avg(sbp_avg)::numeric, 1)::float AS sbp_avg, round(avg(temp_avg)::numeric, 2)::float AS temp_avg, "
            "count(*) FILTER (WHERE trend = 'deteriorating') AS deteriorating "
            "FROM patient_live_status s JOIN patients p USING (patient_id) WHERE p.active") or {}
        lab_file = self._one(
            "SELECT file_day, status, arrival, rows_valid, rows_rejected FROM lab_file_loads "
            "ORDER BY file_day DESC LIMIT 1")
        report = self._one("SELECT max(report_date) AS report_date FROM daily_risk_report")
        patients = self._one("SELECT count(*) AS n FROM patients WHERE active") or {"n": 0}
        return {
            "sim_now": sim_now, "patients": patients["n"], "tiers": tiers, "active_alerts": alerts,
            "vitals": vitals, "latest_lab_file": lab_file,
            "latest_report_date": report["report_date"] if report else None,
        }

    # --- patients ---------------------------------------------------------------

    _PATIENT_STATUS = """
        SELECT p.patient_id, p.bed, p.age, p.sex,
               s.window_start, s.window_end, s.last_event_time,
               s.hr_avg, s.spo2_avg, s.sbp_avg, s.dbp_avg, s.temp_avg,
               s.ews_score, s.ews_red_flag, s.lab_adjustment, s.lab_flags, s.trend, s.trend_adjustment,
               s.total_score, s.risk_tier
        FROM patients p LEFT JOIN patient_live_status s USING (patient_id)
        WHERE p.active
    """

    def list_patients(self, tier: str | None = None) -> list[dict[str, Any]]:
        sql = self._PATIENT_STATUS + (" AND s.risk_tier = %(tier)s" if tier else "")
        sql += " ORDER BY s.total_score DESC NULLS LAST, p.patient_id"
        return self._all(sql, {"tier": tier})

    def patient(self, patient_id: str) -> dict[str, Any] | None:
        return self._one(self._PATIENT_STATUS + " AND p.patient_id = %(pid)s", {"pid": patient_id})

    def latest_labs(self, patient_id: str) -> list[dict[str, Any]]:
        return self._all(
            "SELECT test_type, result_value, unit, reference_range, abnormal_flag, collected_at "
            "FROM lab_latest WHERE patient_id = %(pid)s ORDER BY test_type", {"pid": patient_id})

    def vitals_windows(self, patient_id: str, since: datetime) -> list[dict[str, Any]]:
        return self._all(
            "SELECT window_start, window_end, n_readings, hr_avg, hr_min, hr_max, spo2_avg, spo2_min, spo2_max, "
            "sbp_avg, sbp_min, sbp_max, dbp_avg, temp_avg, temp_min, temp_max, ews_score, ews_red_flag, "
            "lab_adjustment, trend, total_score, risk_tier "
            "FROM vitals_window_1h WHERE patient_id = %(pid)s AND window_start >= %(since)s ORDER BY window_start",
            {"pid": patient_id, "since": since})

    def trend_windows(self, patient_id: str, since: datetime) -> list[dict[str, Any]]:
        return self._all(
            "SELECT window_start, window_end, n_readings, hr_slope, spo2_slope, sbp_slope, temp_slope, "
            "deterioration_index, improvement_index, trend "
            "FROM vitals_trend_4h WHERE patient_id = %(pid)s AND window_start >= %(since)s ORDER BY window_start",
            {"pid": patient_id, "since": since})

    # --- alerts -----------------------------------------------------------------

    def alerts(self, since: datetime, severity: str | None, patient_id: str | None,
               include_acknowledged: bool, limit: int) -> list[dict[str, Any]]:
        sql = ("SELECT alert_id, patient_id, alert_type, severity, window_start, window_end, observed_value, "
               "threshold, message, first_seen_at, last_seen_at, acknowledged FROM alerts "
               "WHERE window_end >= %(since)s")
        if severity:
            sql += " AND severity = %(severity)s"
        if patient_id:
            sql += " AND patient_id = %(pid)s"
        if not include_acknowledged:
            sql += " AND NOT acknowledged"
        sql += (" ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END, "
                "window_end DESC LIMIT %(limit)s")
        return self._all(sql, {"since": since, "severity": severity, "pid": patient_id, "limit": limit})

    def acknowledge_alert(self, alert_id: str) -> dict[str, Any] | None:
        with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("UPDATE alerts SET acknowledged = true WHERE alert_id = %(id)s "
                        "RETURNING alert_id, patient_id, alert_type, severity, acknowledged", {"id": alert_id})
            return cur.fetchone()

    # --- reports ----------------------------------------------------------------

    def latest_report_date(self) -> date | None:
        row = self._one("SELECT max(report_date) AS d FROM daily_risk_report")
        return row["d"] if row else None

    def report(self, report_date: date) -> list[dict[str, Any]]:
        return self._all(
            "SELECT report_date, patient_id, bed, ews_end, ews_peak, hours_high, trend_last, deteriorating_windows, "
            "alerts_critical, alerts_high, alerts_medium, labs, lab_adjustment, lab_flags, labs_status, red_flag, "
            "vitals_score, total_score, vitals_only_tier, risk_tier, tier_change, generated_at "
            "FROM daily_risk_report WHERE report_date = %(d)s "
            "ORDER BY CASE risk_tier WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1 WHEN 'LOW' THEN 2 ELSE 3 END, "
            "total_score DESC NULLS LAST, patient_id", {"d": report_date})

    # --- pipeline state for /metrics (Step 8) --------------------------------------

    def pipeline_state(self) -> dict[str, Any]:
        return {
            "lab_files": self._all("SELECT status, count(*) AS n FROM lab_file_loads GROUP BY 1"),
            "lab_last_loaded": self._one(
                "SELECT file_day, extract(epoch FROM loaded_at) AS loaded_epoch FROM lab_file_loads "
                "WHERE status = 'loaded' ORDER BY file_day DESC LIMIT 1"),
            "lab_ledger": self._all("SELECT file_day, status FROM lab_file_loads "
                                    "WHERE file_day >= (SELECT max(file_day) FROM lab_file_loads) - 3"),
            "lab_rows": self._one("SELECT coalesce(sum(rows_valid), 0) AS valid, coalesce(sum(rows_rejected), 0) AS rejected "
                                  "FROM lab_file_loads"),
            "task_runs": self._all("SELECT dag_id, task_id, state, count(*) AS n FROM pipeline_runs GROUP BY 1, 2, 3"),
            "task_last_success": self._all(
                "SELECT dag_id, extract(epoch FROM max(ended_at)) AS epoch FROM pipeline_runs "
                "WHERE state = 'success' GROUP BY 1"),
            "task_last_duration": self._all(
                "SELECT DISTINCT ON (dag_id, task_id) dag_id, task_id, duration_s FROM pipeline_runs "
                "WHERE duration_s IS NOT NULL ORDER BY dag_id, task_id, recorded_at DESC"),
            "table_rows": self._all(
                "SELECT relname AS table_name, n_live_tup AS n FROM pg_stat_user_tables "
                "WHERE relname IN ('vitals_window_1h','vitals_trend_4h','alerts','lab_results','dead_letter',"
                "'daily_risk_report','patient_live_status')"),
            "db_size": self._one("SELECT pg_database_size(current_database()) AS bytes"),
            "live": self._one("SELECT max(last_event_time) AS last_event_time FROM patient_live_status"),
            "tiers": self._all("SELECT risk_tier, count(*) AS n FROM patient_live_status GROUP BY 1"),
        }


def since(now: datetime, hours: float) -> datetime:
    return now - timedelta(hours=hours)
