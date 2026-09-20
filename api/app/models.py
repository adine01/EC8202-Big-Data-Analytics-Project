"""Response models: typed, validated and documented in the OpenAPI schema (/docs).

All timestamps are simulated time unless the field name says otherwise.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field

DISCLAIMER = "Synthetic data. Risk scores are an illustrative NEWS2-style score, not a clinical tool."


class Health(BaseModel):
    status: str
    checks: dict[str, str]
    sim_now: datetime | None


class VitalsAverages(BaseModel):
    patients_reporting: int = 0
    last_event_time: datetime | None = None
    data_lag_sim_minutes: float | None = Field(None, description="Simulated minutes since the newest reading")
    hr_avg: float | None = None
    spo2_avg: float | None = None
    sbp_avg: float | None = None
    temp_avg: float | None = None
    deteriorating: int = 0


class LabFileStatus(BaseModel):
    file_day: date
    status: str
    arrival: str | None
    rows_valid: int | None
    rows_rejected: int | None


class WardSummary(BaseModel):
    disclaimer: str = DISCLAIMER
    sim_now: datetime
    patients: int
    tiers: dict[str, int] = Field(description="Patients per live risk tier")
    active_alerts: dict[str, int] = Field(description="Unacknowledged alerts in the active window, by severity")
    vitals: VitalsAverages
    latest_lab_file: LabFileStatus | None
    latest_report_date: date | None


class PatientStatus(BaseModel):
    patient_id: str
    bed: str
    age: int
    sex: str
    window_start: datetime | None = None
    window_end: datetime | None = None
    last_event_time: datetime | None = None
    hr_avg: float | None = None
    spo2_avg: float | None = None
    sbp_avg: float | None = None
    dbp_avg: float | None = None
    temp_avg: float | None = None
    ews_score: int | None = None
    ews_red_flag: bool | None = None
    lab_adjustment: int | None = None
    lab_flags: list[str] | None = None
    trend: str | None = None
    trend_adjustment: int | None = None
    total_score: int | None = None
    risk_tier: str | None = None


class LabResult(BaseModel):
    test_type: str
    result_value: float
    unit: str
    reference_range: str
    abnormal_flag: str
    collected_at: datetime
    counts_toward_score: bool = Field(
        True, description="Collected within the 48 simulated hours the live score looks back over")


class PatientDetail(PatientStatus):
    disclaimer: str = DISCLAIMER
    latest_labs: list[LabResult] = []
    active_alerts: int = 0


class VitalsWindow(BaseModel):
    window_start: datetime
    window_end: datetime
    n_readings: int
    hr_avg: float | None
    hr_min: float | None
    hr_max: float | None
    spo2_avg: float | None
    spo2_min: float | None
    spo2_max: float | None
    sbp_avg: float | None
    sbp_min: float | None
    sbp_max: float | None
    dbp_avg: float | None
    temp_avg: float | None
    temp_min: float | None
    temp_max: float | None
    ews_score: int
    ews_red_flag: bool
    lab_adjustment: int
    trend: str
    total_score: int
    risk_tier: str


class TrendWindow(BaseModel):
    window_start: datetime
    window_end: datetime
    n_readings: int
    hr_slope: float | None = Field(None, description="bpm per simulated hour")
    spo2_slope: float | None = Field(None, description="% per simulated hour")
    sbp_slope: float | None = Field(None, description="mmHg per simulated hour")
    temp_slope: float | None = Field(None, description="°C per simulated hour")
    deterioration_index: float
    improvement_index: float
    trend: str


class Alert(BaseModel):
    alert_id: str
    patient_id: str
    alert_type: str
    severity: str
    window_start: datetime
    window_end: datetime
    observed_value: float | None
    threshold: float | None
    message: str
    first_seen_at: datetime = Field(description="Real time the alert was first raised")
    last_seen_at: datetime = Field(description="Real time the alert was last refreshed")
    acknowledged: bool


class AcknowledgedAlert(BaseModel):
    alert_id: str
    patient_id: str
    alert_type: str
    severity: str
    acknowledged: bool


class ReportRow(BaseModel):
    patient_id: str
    bed: str | None
    ews_end: int | None
    ews_peak: int | None
    hours_high: int
    trend_last: str
    deteriorating_windows: int
    alerts_critical: int
    alerts_high: int
    alerts_medium: int
    labs: dict[str, Any]
    lab_adjustment: int
    lab_flags: list[str]
    labs_status: str
    red_flag: bool
    vitals_score: int | None
    total_score: int | None
    vitals_only_tier: str
    risk_tier: str
    tier_change: str


class RiskReport(BaseModel):
    disclaimer: str = DISCLAIMER
    report_date: date
    generated_at: datetime | None = Field(None, description="Real time the report was generated")
    tiers: dict[str, int]
    escalated_by_labs: int
    html_url: str
    csv_url: str
    patients: list[ReportRow]
