"""HTTP endpoints for real-time ward monitoring, alerts and daily reports."""

from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Path as PathParam, Query
from fastapi.responses import FileResponse

from app import models
from app.deps import get_repository, get_sim_now
from app.repository import Repository, since
from ward_common.scoring_rules import LAB_LOOKBACK_HOURS

# How long an unacknowledged alert counts as "active" (simulated hours).
ALERT_ACTIVE_HOURS = float(os.environ.get("ALERT_ACTIVE_HOURS", "2"))
REPORTS_DIR = Path(os.environ.get("REPORTS_DIR", "/srv/reports"))

PatientId = PathParam(pattern=r"^P\d{3}$", description="Patient id, e.g. P001")
Tier = Literal["LOW", "MEDIUM", "HIGH"]
Severity = Literal["medium", "high", "critical"]

router = APIRouter()


# --- ward ----------------------------------------------------------------------

@router.get("/ward/summary", response_model=models.WardSummary, tags=["ward"],
            summary="Real-time ward figures: risk tiers, active alerts, vitals averages, data freshness")
def ward_summary(repo: Repository = Depends(get_repository), sim_now: datetime = Depends(get_sim_now)):
    data = repo.ward_summary(sim_now, since(sim_now, ALERT_ACTIVE_HOURS))
    vitals = dict(data["vitals"])
    last = vitals.get("last_event_time")
    vitals["data_lag_sim_minutes"] = round((sim_now - last).total_seconds() / 60, 1) if last else None
    data["vitals"] = vitals
    return data


@router.get("/patients", response_model=list[models.PatientStatus], tags=["patients"],
            summary="All patients with their live score, highest risk first")
def list_patients(tier: Tier | None = None, repo: Repository = Depends(get_repository)):
    return repo.list_patients(tier)


@router.get("/patients/{patient_id}", response_model=models.PatientDetail, tags=["patients"],
            summary="One patient: live status, latest labs and active alert count")
def patient_detail(patient_id: str = PatientId, repo: Repository = Depends(get_repository),
                   sim_now: datetime = Depends(get_sim_now)):
    patient = repo.patient(patient_id)
    if patient is None:
        raise HTTPException(status_code=404, detail=f"patient {patient_id} not found")
    active = repo.alerts(since(sim_now, ALERT_ACTIVE_HOURS), None, patient_id, False, 500)
    cutoff = since(sim_now, LAB_LOOKBACK_HOURS)
    labs = [{**lab, "counts_toward_score": lab["collected_at"] > cutoff} for lab in repo.latest_labs(patient_id)]
    return {**patient, "latest_labs": labs, "active_alerts": len(active)}


@router.get("/patients/{patient_id}/vitals", response_model=list[models.VitalsWindow], tags=["patients"],
            summary="1 h window aggregates and scores over the last N simulated hours")
def patient_vitals(patient_id: str = PatientId, hours: float = Query(24, gt=0, le=24 * 14),
                   repo: Repository = Depends(get_repository), sim_now: datetime = Depends(get_sim_now)):
    if repo.patient(patient_id) is None:
        raise HTTPException(status_code=404, detail=f"patient {patient_id} not found")
    return repo.vitals_windows(patient_id, since(sim_now, hours))


@router.get("/patients/{patient_id}/trends", response_model=list[models.TrendWindow], tags=["patients"],
            summary="4 h sliding-window slopes and trend classification over the last N simulated hours")
def patient_trends(patient_id: str = PatientId, hours: float = Query(24, gt=0, le=24 * 14),
                   repo: Repository = Depends(get_repository), sim_now: datetime = Depends(get_sim_now)):
    if repo.patient(patient_id) is None:
        raise HTTPException(status_code=404, detail=f"patient {patient_id} not found")
    return repo.trend_windows(patient_id, since(sim_now, hours))


# --- alerts --------------------------------------------------------------------

@router.get("/alerts", response_model=list[models.Alert], tags=["alerts"],
            summary="Alerts, most severe and most recent first (default: active = unacknowledged, last 2 sim hours)")
def list_alerts(
    severity: Severity | None = None,
    patient_id: str | None = Query(None, pattern=r"^P\d{3}$"),
    hours: float = Query(ALERT_ACTIVE_HOURS, gt=0, le=24 * 14, description="Look-back in simulated hours"),
    include_acknowledged: bool = False,
    limit: int = Query(100, ge=1, le=500),
    repo: Repository = Depends(get_repository),
    sim_now: datetime = Depends(get_sim_now),
):
    return repo.alerts(since(sim_now, hours), severity, patient_id, include_acknowledged, limit)


@router.post("/alerts/{alert_id}/acknowledge", response_model=models.AcknowledgedAlert, tags=["alerts"],
             summary="Acknowledge an alert so it no longer counts as active")
def acknowledge_alert(alert_id: str = PathParam(pattern=r"^[0-9a-f]{32}$"), repo: Repository = Depends(get_repository)):
    alert = repo.acknowledge_alert(alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="alert not found")
    return alert


# --- reports -------------------------------------------------------------------

def _report(report_date: date, repo: Repository) -> dict:
    rows = repo.report(report_date)
    if not rows:
        raise HTTPException(status_code=404, detail=f"no report for {report_date}")
    tiers: dict[str, int] = {}
    for row in rows:
        tiers[row["risk_tier"]] = tiers.get(row["risk_tier"], 0) + 1
    return {
        "report_date": report_date,
        "generated_at": max(r["generated_at"] for r in rows),
        "tiers": tiers,
        "escalated_by_labs": sum(r["tier_change"] == "escalated" for r in rows),
        "html_url": f"/reports/{report_date}/html",
        "csv_url": f"/reports/{report_date}/csv",
        "patients": rows,
    }


@router.get("/reports/latest", response_model=models.RiskReport, tags=["reports"],
            summary="Latest daily consolidated risk report (vitals trends joined with the latest labs)")
def latest_report(repo: Repository = Depends(get_repository)):
    latest = repo.latest_report_date()
    if latest is None:
        raise HTTPException(status_code=404, detail="no report generated yet")
    return _report(latest, repo)


@router.get("/reports/{report_date}", response_model=models.RiskReport, tags=["reports"],
            summary="Daily risk report for a simulated day")
def report_for_day(report_date: date, repo: Repository = Depends(get_repository)):
    return _report(report_date, repo)


@router.get("/reports/{report_date}/{fmt}", tags=["reports"], summary="Download the rendered report file",
            response_class=FileResponse)
def report_file(report_date: date, fmt: Literal["html", "csv"]):
    # `report_date` is parsed as a date, so the file name cannot contain path separators.
    path = REPORTS_DIR / f"risk_report_{report_date.isoformat()}.{fmt}"
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"{path.name} not found")
    media = "text/html" if fmt == "html" else "text/csv"
    return FileResponse(path, media_type=media, filename=path.name if fmt == "csv" else None)
