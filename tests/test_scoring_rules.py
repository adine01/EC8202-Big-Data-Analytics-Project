import pytest

from ward_common.scoring_rules import (
    classify_trend,
    early_warning_score,
    lab_adjustment,
    news2_points,
    risk_tier,
)


@pytest.mark.parametrize(
    "vital,value,points",
    [
        ("heart_rate", 40, 3), ("heart_rate", 45, 1), ("heart_rate", 75, 0), ("heart_rate", 100, 1),
        ("heart_rate", 120, 2), ("heart_rate", 131, 3),
        ("spo2", 91, 3), ("spo2", 92.5, 2), ("spo2", 95, 1), ("spo2", 97, 0),
        ("systolic_bp", 88, 3), ("systolic_bp", 95, 2), ("systolic_bp", 108, 1), ("systolic_bp", 120, 0),
        ("systolic_bp", 225, 3),
        ("temperature", 34.9, 3), ("temperature", 35.8, 1), ("temperature", 37.0, 0),
        ("temperature", 38.6, 1), ("temperature", 39.4, 2),
    ],
)
def test_news2_bands(vital, value, points):
    assert news2_points(vital, value) == points


def test_early_warning_score_and_red_flag():
    assert early_warning_score({"heart_rate": 75, "spo2": 97, "systolic_bp": 120, "temperature": 37}) == (0, False)
    assert early_warning_score({"heart_rate": 120, "spo2": 89, "systolic_bp": 95, "temperature": 38.5}) == (8, True)


@pytest.mark.parametrize(
    "score,red,tier",
    [(0, False, "LOW"), (4, False, "LOW"), (3, True, "MEDIUM"), (5, False, "MEDIUM"), (7, False, "HIGH")],
)
def test_risk_tiers(score, red, tier):
    assert risk_tier(score, red) == tier


def test_lab_adjustment_takes_strongest_rule_per_test_and_caps():
    assert lab_adjustment({"LACTATE": 4.5}) == (2, ["lactate_very_high"])
    assert lab_adjustment({"LACTATE": 2.5, "CRP": 150}) == (2, ["crp_very_high", "lactate_high"])
    everything = {"LACTATE": 5, "CRP": 200, "WBC": 20, "CREATININE": 300, "TROPONIN": 50}
    assert lab_adjustment(everything)[0] == 4
    assert lab_adjustment({"WBC": 7.0, "POTASSIUM": 4.1}) == (0, [])


def test_trend_classification():
    assert classify_trend({"heart_rate": 6, "systolic_bp": -6, "temperature": 0.1, "spo2": 0}) == "deteriorating"
    assert classify_trend({"heart_rate": -6, "systolic_bp": 6, "temperature": -0.1, "spo2": 0.3}) == "improving"
    assert classify_trend({"heart_rate": 1, "systolic_bp": -1, "temperature": 0.02, "spo2": 0}) == "stable"
