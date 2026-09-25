import csv
import io
from datetime import date

from ward_common.scoring_rules import early_warning_score, lab_adjustment, risk_tier
from ward_ingest import risk_report
from ward_ingest.risk_report import build_report, render_csv, render_html, score_patient, write_outputs

DAY = date(2026, 1, 10)
CALM = {"windows": 24, "readings": 280, "hr_tail": 76.0, "spo2_tail": 97.5, "sbp_tail": 122.0, "temp_tail": 36.9,
        "ews_peak": 1, "hours_high": 0, "hours_medium": 0}
SICK = {**CALM, "hr_tail": 118.0, "spo2_tail": 93.0, "sbp_tail": 98.0, "temp_tail": 38.7, "ews_peak": 8}


def lab(value, flag="N"):
    return {"value": value, "unit": "u", "reference_range": "x", "flag": flag, "collected_at": "2026-01-10T07:00:00+00:00"}


def test_scores_use_the_shared_rules():
    result = score_patient(SICK, {"trend_last": "stable"}, {})
    ews, red = early_warning_score({"heart_rate": 118, "spo2": 93, "systolic_bp": 98, "temperature": 38.7})
    assert (result["ews_end"], result["red_flag"]) == (ews, red)
    assert result["risk_tier"] == risk_tier(ews, red)


def test_labs_can_escalate_a_patient_whose_vitals_look_fine():
    labs = {"LACTATE": lab(4.6, "H"), "CRP": lab(180.0, "H"), "WBC": lab(15.2, "H")}
    result = score_patient(CALM, {"trend_last": "stable"}, labs)
    assert result["vitals_only_tier"] == "LOW"
    assert result["lab_adjustment"] == lab_adjustment({"LACTATE": 4.6, "CRP": 180.0, "WBC": 15.2})[0] == 4
    assert result["risk_tier"] == "LOW"  # 0 + 4 < 5: labs alone stay below MEDIUM...
    borderline = {**CALM, "hr_tail": 95.0, "temp_tail": 38.3}  # EWS 2
    escalated = score_patient(borderline, {"trend_last": "stable"}, labs)
    assert (escalated["vitals_only_tier"], escalated["risk_tier"], escalated["tier_change"]) == ("LOW", "MEDIUM", "escalated")


def test_deteriorating_trend_adds_its_adjustment():
    stable = score_patient(CALM, {"trend_last": "stable"}, {})
    worse = score_patient(CALM, {"trend_last": "deteriorating"}, {})
    assert worse["total_score"] == stable["total_score"] + 1


def test_patient_without_vitals_is_no_data_not_low():
    result = score_patient(None, None, {"CRP": lab(3.0)})
    assert result["risk_tier"] == "NO_DATA" and result["tier_change"] == "no_data"


def inputs():
    return {
        "patients": [{"patient_id": "P001", "bed": "B01", "scenario": "stable"},
                     {"patient_id": "P002", "bed": "B02", "scenario": "sepsis"},
                     {"patient_id": "P003", "bed": "B03", "scenario": "stable"}],
        "vitals": {"P001": CALM, "P002": SICK},
        "trends": {"P002": {"trend_last": "deteriorating", "deteriorating_windows": 5, "max_deterioration_index": 9.1}},
        "alerts": {"P002": {"alerts_critical": 1, "alerts_high": 2, "alerts_medium": 3}},
        "labs": {"P002": {"LACTATE": lab(3.1, "H"), "CRP": lab(150.0, "H")}},
        "lab_file": {"status": "loaded", "arrival": "on_time", "rows_valid": 110, "rows_rejected": 3},
    }


def test_report_rows_sorted_by_risk_and_complete():
    rows = build_report(DAY, inputs())
    assert [r["patient_id"] for r in rows] == ["P002", "P001", "P003"]
    assert rows[0]["risk_tier"] == "HIGH" and rows[-1]["risk_tier"] == "NO_DATA"
    assert all(set(risk_report.REPORT_COLUMNS) <= set(r) for r in rows)
    assert rows[0]["labs_status"] == "loaded/on_time"


def test_csv_and_html_outputs(tmp_path):
    rows = build_report(DAY, inputs())
    parsed = list(csv.DictReader(io.StringIO(render_csv(rows))))
    assert parsed[0]["patient_id"] == "P002" and "LACTATE=3.1(H)" in parsed[0]["labs_latest"]

    meta = {"generated_at_sim": "2026-01-11 06:40", "generated_at_real": "now", "labs_status": "loaded/on_time",
            "lab_rows_valid": 110, "lab_rows_rejected": 3, "run_id": "test"}
    html = render_html(DAY, rows, meta, {"P001": "stable", "P002": "sepsis", "P003": "stable"})
    assert "not clinical" in html and "P002" in html and "Ward risk report - 2026-01-10" in html

    paths = write_outputs(tmp_path, DAY, html, render_csv(rows))
    assert sorted(p.name for p in tmp_path.iterdir()) == ["risk_report_2026-01-10.csv", "risk_report_2026-01-10.html"]
    assert paths[0].read_text() == html


def test_html_escapes_untrusted_text():
    data = inputs()
    data["patients"][0]["bed"] = "<script>alert(1)</script>"
    rows = build_report(DAY, data)
    html = render_html(DAY, rows, {"generated_at_sim": "", "generated_at_real": "", "labs_status": "",
                                   "lab_rows_valid": 0, "lab_rows_rejected": 0, "run_id": ""}, {})
    assert "<script>alert(1)</script>" not in html
