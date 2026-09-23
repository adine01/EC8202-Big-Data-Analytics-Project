"""Daily consolidated risk report: joins a day of vitals trends with the latest labs.

ILLUSTRATIVE ONLY - NEWS2-style scoring on synthetic data, not a clinical tool.

For report day D (simulated):
  vitals   end-of-day state = reading-weighted averages over D's last 4 hours of
           1 h windows (one noisy hour cannot decide a daily report), plus the
           day's peak, time spent at HIGH/MEDIUM, and extremes;
  trends   last trend of the day and how many 4 h windows were deteriorating;
  alerts   raised during D, by severity;
  labs     latest result per test collected in the 48 h up to the end of D
           ("yesterday's labs").

Scoring reuses ward_common.scoring_rules, the same rules the stream job applies,
so the daily tier and the live tier can only differ because of the inputs
(4 h vs 1 h averages), never because of different logic. The report shows the
tier from vitals alone next to the tier after labs, which is the business
question: how do yesterday's labs change the risk picture going forward?
"""

from __future__ import annotations

import csv
import io
import json
import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from ward_common.scoring_rules import (
    TREND_ADJUSTMENT,
    early_warning_score,
    lab_adjustment,
    risk_tier,
)

TAIL_HOURS = 4
LAB_LOOKBACK_HOURS = 48
TIER_RANK = {"NO_DATA": -1, "LOW": 0, "MEDIUM": 1, "HIGH": 2}

REPORT_COLUMNS = [
    "report_date", "patient_id", "bed", "windows", "readings", "ews_end", "ews_peak",
    "hours_high", "hours_medium", "hr_max", "spo2_min", "sbp_min", "temp_max",
    "trend_last", "deteriorating_windows", "max_deterioration_index",
    "alerts_critical", "alerts_high", "alerts_medium",
    "labs", "lab_adjustment", "lab_flags", "labs_status",
    "red_flag", "trend_adjustment", "vitals_score", "total_score",
    "vitals_only_tier", "risk_tier", "tier_change",
]


def day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time(0), tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


# --------------------------------------------------------------------------
# Batch side of the join: feature extraction in SQL
# --------------------------------------------------------------------------

_VITALS_SQL = """
SELECT patient_id,
       count(*)                                         AS windows,
       sum(n_readings)                                  AS readings,
       max(ews_score)                                   AS ews_peak,
       count(*) FILTER (WHERE risk_tier = 'HIGH')       AS hours_high,
       count(*) FILTER (WHERE risk_tier = 'MEDIUM')     AS hours_medium,
       max(hr_max) AS hr_max, min(spo2_min) AS spo2_min, min(sbp_min) AS sbp_min, max(temp_max) AS temp_max,
       sum(hr_avg * n_readings)   FILTER (WHERE window_start >= %(tail)s)
         / nullif(sum(n_readings) FILTER (WHERE window_start >= %(tail)s), 0) AS hr_tail,
       sum(spo2_avg * n_readings) FILTER (WHERE window_start >= %(tail)s)
         / nullif(sum(n_readings) FILTER (WHERE window_start >= %(tail)s), 0) AS spo2_tail,
       sum(sbp_avg * n_readings)  FILTER (WHERE window_start >= %(tail)s)
         / nullif(sum(n_readings) FILTER (WHERE window_start >= %(tail)s), 0) AS sbp_tail,
       sum(temp_avg * n_readings) FILTER (WHERE window_start >= %(tail)s)
         / nullif(sum(n_readings) FILTER (WHERE window_start >= %(tail)s), 0) AS temp_tail
FROM vitals_window_1h
WHERE window_start >= %(d0)s AND window_start < %(d1)s
GROUP BY patient_id
"""

_TRENDS_SQL = """
SELECT patient_id,
       count(*) FILTER (WHERE trend = 'deteriorating') AS deteriorating_windows,
       max(deterioration_index)                       AS max_deterioration_index,
       (array_agg(trend ORDER BY window_end DESC) FILTER (WHERE trend <> 'insufficient_data'))[1] AS trend_last
FROM vitals_trend_4h
WHERE window_end > %(d0)s AND window_end <= %(d1)s
GROUP BY patient_id
"""

_ALERTS_SQL = """
SELECT patient_id,
       count(*) FILTER (WHERE severity = 'critical') AS alerts_critical,
       count(*) FILTER (WHERE severity = 'high')     AS alerts_high,
       count(*) FILTER (WHERE severity = 'medium')   AS alerts_medium
FROM alerts
WHERE window_start >= %(d0)s AND window_start < %(d1)s
GROUP BY patient_id
"""

_LABS_SQL = """
SELECT DISTINCT ON (patient_id, test_type)
       patient_id, test_type, result_value, unit, reference_range, abnormal_flag, collected_at
FROM lab_results
WHERE collected_at > %(d1)s - make_interval(hours => %(lookback)s) AND collected_at <= %(d1)s
ORDER BY patient_id, test_type, collected_at DESC
"""


def _rows(conn, sql: str, params: dict) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        names = [c[0] for c in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]


def fetch_inputs(conn, day: date) -> dict:
    d0, d1 = day_bounds(day)
    params = {"d0": d0, "d1": d1, "tail": d1 - timedelta(hours=TAIL_HOURS), "lookback": LAB_LOOKBACK_HOURS}
    labs: dict[str, dict] = {}
    for r in _rows(conn, _LABS_SQL, params):
        labs.setdefault(r["patient_id"], {})[r["test_type"]] = {
            "value": float(r["result_value"]), "unit": r["unit"], "reference_range": r["reference_range"],
            "flag": r["abnormal_flag"], "collected_at": r["collected_at"].isoformat(),
        }
    ledger = _rows(conn, "SELECT status, arrival, rows_valid, rows_rejected FROM lab_file_loads WHERE file_day = %(day)s",
                   {"day": day})
    return {
        "patients": _rows(conn, "SELECT patient_id, bed, scenario FROM patients WHERE active ORDER BY patient_id", {}),
        "vitals": {r["patient_id"]: r for r in _rows(conn, _VITALS_SQL, params)},
        "trends": {r["patient_id"]: r for r in _rows(conn, _TRENDS_SQL, params)},
        "alerts": {r["patient_id"]: r for r in _rows(conn, _ALERTS_SQL, params)},
        "labs": labs,
        "lab_file": ledger[0] if ledger else None,
    }


# --------------------------------------------------------------------------
# Scoring (pure)
# --------------------------------------------------------------------------

def _num(value):
    return None if value is None else float(value)


def score_patient(vitals: dict | None, trends: dict | None, labs: dict[str, dict]) -> dict:
    lab_points, lab_flags = lab_adjustment({test: v["value"] for test, v in labs.items()})
    if not vitals or not vitals.get("windows"):
        return {"ews_end": None, "red_flag": False, "trend_adjustment": 0, "vitals_score": None,
                "total_score": None, "lab_adjustment": lab_points, "lab_flags": lab_flags,
                "vitals_only_tier": "NO_DATA", "risk_tier": "NO_DATA", "tier_change": "no_data"}

    tail = {"heart_rate": _num(vitals.get("hr_tail")), "spo2": _num(vitals.get("spo2_tail")),
            "systolic_bp": _num(vitals.get("sbp_tail")), "temperature": _num(vitals.get("temp_tail"))}
    ews, red_flag = early_warning_score(tail)
    trend_last = (trends or {}).get("trend_last") or "unknown"
    trend_points = TREND_ADJUSTMENT if trend_last == "deteriorating" else 0

    vitals_score = ews + trend_points
    total = vitals_score + lab_points
    vitals_only, final = risk_tier(vitals_score, red_flag), risk_tier(total, red_flag)
    return {
        "ews_end": ews, "red_flag": red_flag, "trend_adjustment": trend_points,
        "vitals_score": vitals_score, "total_score": total,
        "lab_adjustment": lab_points, "lab_flags": lab_flags,
        "vitals_only_tier": vitals_only, "risk_tier": final,
        "tier_change": "escalated" if TIER_RANK[final] > TIER_RANK[vitals_only] else "unchanged",
    }


def build_report(day: date, inputs: dict) -> list[dict]:
    """One row per active patient, sorted by risk (highest first)."""
    lab_file = inputs["lab_file"]
    labs_status = f"{lab_file['status']}/{lab_file['arrival']}" if lab_file else "not_received"
    rows = []
    for patient in inputs["patients"]:
        pid = patient["patient_id"]
        vitals = inputs["vitals"].get(pid) or {}
        trends = inputs["trends"].get(pid) or {}
        alerts = inputs["alerts"].get(pid) or {}
        labs = inputs["labs"].get(pid, {})
        row = {
            "report_date": day, "patient_id": pid, "bed": patient["bed"],
            "windows": int(vitals.get("windows") or 0), "readings": int(vitals.get("readings") or 0),
            "ews_peak": vitals.get("ews_peak"),
            "hours_high": int(vitals.get("hours_high") or 0), "hours_medium": int(vitals.get("hours_medium") or 0),
            "hr_max": _num(vitals.get("hr_max")), "spo2_min": _num(vitals.get("spo2_min")),
            "sbp_min": _num(vitals.get("sbp_min")), "temp_max": _num(vitals.get("temp_max")),
            "trend_last": trends.get("trend_last") or "unknown",
            "deteriorating_windows": int(trends.get("deteriorating_windows") or 0),
            "max_deterioration_index": _num(trends.get("max_deterioration_index")),
            "alerts_critical": int(alerts.get("alerts_critical") or 0),
            "alerts_high": int(alerts.get("alerts_high") or 0),
            "alerts_medium": int(alerts.get("alerts_medium") or 0),
            "labs": labs, "labs_status": labs_status,
        }
        row.update(score_patient(vitals, trends, labs))
        rows.append(row)
    rows.sort(key=lambda r: (-TIER_RANK[r["risk_tier"]], -(r["total_score"] or -1), r["patient_id"]))
    return rows


def summarise(rows: list[dict]) -> dict:
    tiers = {t: sum(r["risk_tier"] == t for r in rows) for t in ("HIGH", "MEDIUM", "LOW", "NO_DATA")}
    return {
        "patients": len(rows),
        "tiers": tiers,
        "escalated_by_labs": sum(r["tier_change"] == "escalated" for r in rows),
        "alerts": sum(r["alerts_critical"] + r["alerts_high"] + r["alerts_medium"] for r in rows),
        "deteriorating": sum(r["trend_last"] == "deteriorating" for r in rows),
    }


# --------------------------------------------------------------------------
# Persistence and rendering
# --------------------------------------------------------------------------

def upsert_report(conn, rows: list[dict]) -> None:
    columns = ", ".join(REPORT_COLUMNS)
    placeholders = ", ".join(["%s"] * len(REPORT_COLUMNS))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in REPORT_COLUMNS if c not in ("report_date", "patient_id"))
    sql = (f"INSERT INTO daily_risk_report ({columns}) VALUES ({placeholders}) "
           f"ON CONFLICT (report_date, patient_id) DO UPDATE SET {updates}, generated_at = now()")
    with conn.cursor() as cur:
        for row in rows:
            values = [json.dumps(row[c]) if c == "labs" else row[c] for c in REPORT_COLUMNS]
            cur.execute(sql, values)
    conn.commit()


def _labs_text(labs: dict[str, dict]) -> str:
    return "; ".join(f"{t}={v['value']:g}{'' if v['flag'] == 'N' else '(' + v['flag'] + ')'}" for t, v in sorted(labs.items()))


CSV_COLUMNS = [c for c in REPORT_COLUMNS if c != "labs"] + ["labs_latest"]


def render_csv(rows: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        out = {c: row[c] for c in CSV_COLUMNS if c in row}
        out["lab_flags"] = "|".join(row["lab_flags"])
        out["labs_latest"] = _labs_text(row["labs"])
        writer.writerow(out)
    return buffer.getvalue()


def render_html(day: date, rows: list[dict], meta: dict, scenarios: dict[str, str]) -> str:
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    env = Environment(
        loader=FileSystemLoader(Path(__file__).parent / "templates"),
        autoescape=select_autoescape(["html", "j2"]),
    )
    evaluation = {}
    for row in rows:
        storyline = scenarios.get(row["patient_id"], "unknown")
        evaluation.setdefault(storyline, {t: 0 for t in ("HIGH", "MEDIUM", "LOW", "NO_DATA")})
        evaluation[storyline][row["risk_tier"]] += 1
    return env.get_template("risk_report.html.j2").render(
        day=day, rows=rows, summary=summarise(rows), meta=meta, evaluation=dict(sorted(evaluation.items())),
    )


def write_outputs(reports_dir: Path, day: date, html: str, csv_text: str) -> list[Path]:
    """Atomic writes (temp file + rename): the API never serves a half-written report."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for suffix, content in ((".html", html), (".csv", csv_text)):
        final = reports_dir / f"risk_report_{day.isoformat()}{suffix}"
        tmp = reports_dir / f".{final.name}.tmp"
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, final)
        written.append(final)
    return written
