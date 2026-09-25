"""API tests against an in-memory repository (no database needed)."""

from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.metrics import lab_file_overdue

NOW = datetime(2026, 1, 12, 9, 0, tzinfo=timezone.utc)
ALERT_ID = "a" * 32


def patient(pid, score, tier, **extra):
    return {"patient_id": pid, "bed": "B" + pid[-2:], "age": 70, "sex": "F", "window_start": NOW - timedelta(hours=1),
            "window_end": NOW, "last_event_time": NOW - timedelta(minutes=5), "hr_avg": 80.0, "spo2_avg": 96.0,
            "sbp_avg": 120.0, "dbp_avg": 75.0, "temp_avg": 37.0, "ews_score": score, "ews_red_flag": False,
            "lab_adjustment": 0, "lab_flags": [], "trend": "stable", "trend_adjustment": 0,
            "total_score": score, "risk_tier": tier, **extra}


class FakeRepository:
    def __init__(self):
        self.patients = {"P001": patient("P001", 8, "HIGH"), "P002": patient("P002", 1, "LOW")}
        self.acknowledged = set()
        self.calls = []
        self.fail_ping = False

    def ping(self):
        if self.fail_ping:
            raise ConnectionError("db down")

    def ward_summary(self, sim_now, active_since):
        self.calls.append(("ward_summary", active_since))
        return {"sim_now": sim_now, "patients": 2, "tiers": {"HIGH": 1, "LOW": 1}, "active_alerts": {"critical": 1},
                "vitals": {"patients_reporting": 2, "last_event_time": NOW - timedelta(minutes=12), "hr_avg": 80.0,
                           "spo2_avg": 96.0, "sbp_avg": 120.0, "temp_avg": 37.0, "deteriorating": 1},
                "latest_lab_file": {"file_day": date(2026, 1, 11), "status": "loaded", "arrival": "on_time",
                                    "rows_valid": 110, "rows_rejected": 3},
                "latest_report_date": date(2026, 1, 11)}

    def list_patients(self, tier=None):
        rows = sorted(self.patients.values(), key=lambda p: -p["total_score"])
        return [p for p in rows if tier is None or p["risk_tier"] == tier]

    def patient(self, pid):
        return self.patients.get(pid)

    def latest_labs(self, pid):
        return [{"test_type": "CRP", "result_value": 150.0, "unit": "mg/L", "reference_range": "<5",
                 "abnormal_flag": "H", "collected_at": NOW - timedelta(hours=20)}]

    def vitals_windows(self, pid, since):
        self.calls.append(("vitals", since))
        return [{"window_start": NOW - timedelta(hours=1), "window_end": NOW, "n_readings": 12, "hr_avg": 80.0,
                 "hr_min": 75.0, "hr_max": 88.0, "spo2_avg": 96.0, "spo2_min": 95.0, "spo2_max": 98.0,
                 "sbp_avg": 120.0, "sbp_min": 115.0, "sbp_max": 126.0, "dbp_avg": 75.0, "temp_avg": 37.0,
                 "temp_min": 36.9, "temp_max": 37.1, "ews_score": 0, "ews_red_flag": False, "lab_adjustment": 0,
                 "trend": "stable", "total_score": 0, "risk_tier": "LOW"}]

    def trend_windows(self, pid, since):
        return [{"window_start": NOW - timedelta(hours=4), "window_end": NOW, "n_readings": 48, "hr_slope": 2.5,
                 "spo2_slope": -0.4, "sbp_slope": -3.1, "temp_slope": 0.1, "deterioration_index": 6.2,
                 "improvement_index": 0.0, "trend": "deteriorating"}]

    def alerts(self, since, severity, patient_id, include_acknowledged, limit):
        self.calls.append(("alerts", since, severity, patient_id, include_acknowledged, limit))
        alert = {"alert_id": ALERT_ID, "patient_id": "P001", "alert_type": "SPO2_LOW", "severity": "critical",
                 "window_start": NOW - timedelta(hours=1), "window_end": NOW, "observed_value": 86.0,
                 "threshold": 90.0, "message": "spo2 < 90", "first_seen_at": NOW, "last_seen_at": NOW,
                 "acknowledged": ALERT_ID in self.acknowledged}
        if (severity and severity != "critical") or (patient_id and patient_id != "P001"):
            return []
        if alert["acknowledged"] and not include_acknowledged:
            return []
        return [alert]

    def acknowledge_alert(self, alert_id):
        if alert_id != ALERT_ID:
            return None
        self.acknowledged.add(alert_id)
        return {"alert_id": alert_id, "patient_id": "P001", "alert_type": "SPO2_LOW", "severity": "critical",
                "acknowledged": True}

    def latest_report_date(self):
        return date(2026, 1, 11)

    def report(self, report_date):
        if report_date != date(2026, 1, 11):
            return []
        base = {"report_date": report_date, "bed": "B01", "ews_end": 1, "ews_peak": 9, "hours_high": 3,
                "trend_last": "improving", "deteriorating_windows": 2, "alerts_critical": 1, "alerts_high": 0,
                "alerts_medium": 2, "labs": {"CRP": {"value": 150.0, "flag": "H"}}, "lab_adjustment": 4,
                "lab_flags": ["crp_very_high"], "labs_status": "loaded/on_time", "red_flag": False,
                "vitals_score": 1, "total_score": 5, "generated_at": NOW}
        return [{**base, "patient_id": "P014", "vitals_only_tier": "LOW", "risk_tier": "MEDIUM", "tier_change": "escalated"},
                {**base, "patient_id": "P001", "vitals_only_tier": "LOW", "risk_tier": "LOW", "tier_change": "unchanged",
                 "lab_adjustment": 0, "total_score": 1}]

    def pipeline_state(self):
        return {
            "lab_files": [{"status": "loaded", "n": 10}, {"status": "missing", "n": 1}],
            "lab_last_loaded": {"file_day": date(2026, 1, 11), "loaded_epoch": 1790000000},
            "lab_ledger": [{"file_day": date(2026, 1, 11), "status": "loaded"}],
            "lab_rows": {"valid": 1100, "rejected": 33},
            "task_runs": [{"dag_id": "lab_ingest", "task_id": "wait_for_lab_file", "state": "failed", "n": 2}],
            "task_last_success": [{"dag_id": "lab_ingest", "epoch": 1790000100}],
            "task_last_duration": [{"dag_id": "lab_ingest", "task_id": "load_landing_files", "duration_s": 8.3}],
            "table_rows": [{"table_name": "alerts", "n": 900}],
            "db_size": {"bytes": 12_000_000},
            "live": {"last_event_time": NOW - timedelta(minutes=10)},
            "tiers": [{"risk_tier": "HIGH", "n": 1}, {"risk_tier": "LOW", "n": 1}],
        }


@pytest.fixture
def repo():
    return FakeRepository()


@pytest.fixture
def client(repo, tmp_path, monkeypatch):
    monkeypatch.setattr("app.routers.REPORTS_DIR", tmp_path)
    (tmp_path / "risk_report_2026-01-11.html").write_text("<html>report</html>")
    with TestClient(create_app(repository=repo, sim_now=lambda: NOW)) as c:
        yield c


def test_health_ok_and_degraded(client, repo):
    assert client.get("/health").json()["status"] == "ok"
    repo.fail_ping = True
    response = client.get("/health")
    assert response.status_code == 503 and response.json()["checks"]["database"].startswith("error")


def test_ward_summary_includes_freshness_and_disclaimer(client, repo):
    body = client.get("/ward/summary").json()
    assert body["tiers"] == {"HIGH": 1, "LOW": 1}
    assert body["vitals"]["data_lag_sim_minutes"] == 12.0
    assert "not a clinical tool" in body["disclaimer"]
    assert repo.calls[0] == ("ward_summary", NOW - timedelta(hours=2))  # active window = 2 sim hours


def test_patients_sorted_and_filterable(client):
    assert [p["patient_id"] for p in client.get("/patients").json()] == ["P001", "P002"]
    assert [p["patient_id"] for p in client.get("/patients", params={"tier": "LOW"}).json()] == ["P002"]
    assert client.get("/patients", params={"tier": "SEVERE"}).status_code == 422


def test_patient_detail_vitals_and_trends(client, repo):
    detail = client.get("/patients/P001").json()
    assert detail["risk_tier"] == "HIGH" and detail["latest_labs"][0]["test_type"] == "CRP"
    assert detail["latest_labs"][0]["counts_toward_score"] is True  # collected 20 sim-hours ago (< 48 h)
    assert detail["active_alerts"] == 1
    assert client.get("/patients/P001/vitals", params={"hours": 6}).json()[0]["n_readings"] == 12
    assert ("vitals", NOW - timedelta(hours=6)) in repo.calls
    assert client.get("/patients/P001/trends").json()[0]["trend"] == "deteriorating"


@pytest.mark.parametrize("path", ["/patients/P999", "/patients/P999/vitals", "/patients/P999/trends"])
def test_unknown_patient_is_404(client, path):
    assert client.get(path).status_code == 404


@pytest.mark.parametrize("path", ["/patients/bed7", "/patients/P1", "/patients/..%2Fetc"])
def test_malformed_patient_id_is_rejected(client, path):
    assert client.get(path).status_code in (404, 422)


def test_alerts_filters_and_acknowledge(client, repo):
    assert len(client.get("/alerts").json()) == 1
    assert client.get("/alerts", params={"severity": "high"}).json() == []
    assert client.get("/alerts", params={"limit": 1000}).status_code == 422
    assert client.post(f"/alerts/{ALERT_ID}/acknowledge").json()["acknowledged"] is True
    assert client.get("/alerts").json() == []  # no longer active
    assert len(client.get("/alerts", params={"include_acknowledged": True}).json()) == 1
    assert client.post(f"/alerts/{'b' * 32}/acknowledge").status_code == 404


def test_reports(client):
    latest = client.get("/reports/latest").json()
    assert latest["report_date"] == "2026-01-11"
    assert latest["escalated_by_labs"] == 1 and latest["tiers"] == {"MEDIUM": 1, "LOW": 1}
    assert latest["patients"][0]["patient_id"] == "P014"
    assert client.get("/reports/2026-01-05").status_code == 404
    assert client.get("/reports/not-a-date").status_code == 422
    html = client.get("/reports/2026-01-11/html")
    assert html.status_code == 200 and "report" in html.text
    assert client.get("/reports/2026-01-11/csv").status_code == 404  # file not written in this test
    assert client.get("/reports/2026-01-11/pdf").status_code == 422


def test_metrics_expose_http_and_pipeline_state(client):
    client.get("/patients/P001")
    text = client.get("/metrics").text
    assert 'ward_api_requests_total{method="GET",route="/patients/{patient_id}",status="200"} 1.0' in text
    assert "ward_db_up 1.0" in text
    assert 'ward_lab_files{status="missing"} 1.0' in text
    assert 'ward_pipeline_task_runs_total{dag_id="lab_ingest",state="failed",task_id="wait_for_lab_file"} 2.0' in text
    assert "ward_vitals_data_lag_sim_seconds 600.0" in text
    assert "ward_lab_file_overdue 0.0" in text


def test_metrics_survive_database_outage(client, repo):
    repo.pipeline_state = lambda: (_ for _ in ()).throw(ConnectionError("down"))
    text = client.get("/metrics").text
    assert "ward_db_up 0.0" in text


@pytest.mark.parametrize(
    "sim_now,ledger,expected",
    [
        (datetime(2026, 1, 12, 7, 0, tzinfo=timezone.utc), {}, (0, 1.0)),                         # due 06:00, 1 h late
        (datetime(2026, 1, 12, 11, 0, tzinfo=timezone.utc), {}, (1, 5.0)),                        # past 4 h SLA
        (datetime(2026, 1, 12, 11, 0, tzinfo=timezone.utc), {date(2026, 1, 11): "loaded"}, (0, 0.0)),
        (datetime(2026, 1, 12, 11, 0, tzinfo=timezone.utc), {date(2026, 1, 11): "missing"}, (1, 5.0)),
        (datetime(2026, 1, 12, 3, 0, tzinfo=timezone.utc), {}, (0, 0.0)),                         # not due yet
    ],
)
def test_lab_file_overdue(sim_now, ledger, expected):
    assert lab_file_overdue(sim_now, ledger) == expected
