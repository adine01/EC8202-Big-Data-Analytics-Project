"""Single source of truth for scoring, alerting and trend rules.

ILLUSTRATIVE ONLY - NOT A CLINICAL TOOL. The early-warning score below is a
simplified subset of NEWS2 (heart rate, SpO2 scale 1, systolic BP,
temperature). Respiratory rate, consciousness level and supplemental oxygen
are not in the synthetic feed, so the score is partial.

The rules are plain data. The Spark job turns them into column expressions,
and the Airflow report / API use the Python functions below, so every
component scores a patient identically (the Kappa consistency argument in
docs/architecture_decision.md, enforced in code and by a parity test).
"""

from __future__ import annotations

import math

# --------------------------------------------------------------------------
# NEWS2-style points per vital: ordered (inclusive upper bound, points).
# e.g. heart rate 95 -> first bound >= 95 is 110 -> 1 point.
# --------------------------------------------------------------------------
INF = math.inf

NEWS2_BANDS: dict[str, list[tuple[float, int]]] = {
    "heart_rate": [(40, 3), (50, 1), (90, 0), (110, 1), (130, 2), (INF, 3)],
    "spo2": [(91, 3), (93, 2), (95, 1), (INF, 0)],
    "systolic_bp": [(90, 3), (100, 2), (110, 1), (219, 0), (INF, 3)],
    "temperature": [(35.0, 3), (36.0, 1), (38.0, 0), (39.0, 1), (INF, 2)],
}

# Window averages are continuous (e.g. HR 90.4), so bands use "<= bound".
# Integer bands like "91-110" therefore become (90, 110]: 90.4 scores 1.


def news2_points(vital: str, value: float | None) -> int:
    if value is None:
        return 0
    for bound, points in NEWS2_BANDS[vital]:
        if value <= bound:
            return points
    return 0  # unreachable: last bound is +inf


def early_warning_score(values: dict[str, float | None]) -> tuple[int, bool]:
    """(total points, red_flag) where red_flag = any single parameter scoring 3."""
    points = [news2_points(v, values.get(v)) for v in NEWS2_BANDS]
    return sum(points), any(p == 3 for p in points)


# --------------------------------------------------------------------------
# Lab adjustment: latest result per test within LAB_LOOKBACK_HOURS.
# Per test, the highest-scoring matching rule applies; total capped.
# --------------------------------------------------------------------------
LAB_LOOKBACK_HOURS = 48
LAB_ADJUSTMENT_CAP = 4

# (test, operator, threshold, points, flag)
LAB_RULES: list[tuple[str, str, float, int, str]] = [
    ("LACTATE", ">", 4.0, 2, "lactate_very_high"),
    ("LACTATE", ">", 2.0, 1, "lactate_high"),
    ("CRP", ">", 100.0, 1, "crp_very_high"),
    ("WBC", ">", 12.0, 1, "wbc_high"),
    ("WBC", "<", 4.0, 1, "wbc_low"),
    ("CREATININE", ">", 150.0, 1, "creatinine_high"),
    ("POTASSIUM", ">", 6.0, 1, "potassium_high"),
    ("POTASSIUM", "<", 3.0, 1, "potassium_low"),
    ("HAEMOGLOBIN", "<", 90.0, 1, "haemoglobin_low"),
    ("TROPONIN", ">", 14.0, 1, "troponin_raised"),
]


def _matches(op: str, value: float, threshold: float) -> bool:
    return value > threshold if op == ">" else value < threshold


def lab_adjustment(latest: dict[str, float]) -> tuple[int, list[str]]:
    """latest: {test_type: value}. Returns (capped points, triggered flags)."""
    total, flags = 0, []
    for test, value in latest.items():
        best = None
        for rule_test, op, threshold, points, flag in LAB_RULES:
            if rule_test == test and _matches(op, value, threshold):
                if best is None or points > best[0]:
                    best = (points, flag)
        if best:
            total += best[0]
            flags.append(best[1])
    return min(total, LAB_ADJUSTMENT_CAP), sorted(flags)


# --------------------------------------------------------------------------
# Trend: least-squares slope per vital over a 4 h sliding window, scaled by
# the slope noise seen in stable patients, summed in the "worsening"
# direction only. Calibrated on the simulator: index >= 5 catches ~76% of
# worsening windows while flagging ~1% of stable ones.
# --------------------------------------------------------------------------
TREND_WINDOW = "4 hours"
TREND_SLIDE = "1 hour"
# vital: (direction that means "worse", typical stable slope noise per hour)
TREND_SIGNALS: dict[str, tuple[int, float]] = {
    "heart_rate": (+1, 1.5),
    "spo2": (-1, 0.27),
    "systolic_bp": (-1, 1.8),
    "temperature": (+1, 0.055),
}
TREND_INDEX_THRESHOLD = 5.0
TREND_ADJUSTMENT = 1  # added to the score while "deteriorating"


def trend_indices(slopes: dict[str, float | None]) -> tuple[float, float]:
    """(deterioration_index, improvement_index) from per-hour slopes."""
    worse = better = 0.0
    for vital, (direction, scale) in TREND_SIGNALS.items():
        slope = slopes.get(vital)
        if slope is None:
            continue
        z = direction * slope / scale
        worse += max(z, 0.0)
        better += max(-z, 0.0)
    return worse, better


def classify_trend(slopes: dict[str, float | None]) -> str:
    worse, better = trend_indices(slopes)
    if worse >= TREND_INDEX_THRESHOLD and worse > better:
        return "deteriorating"
    if better >= TREND_INDEX_THRESHOLD and better > worse:
        return "improving"
    return "stable"


# --------------------------------------------------------------------------
# Threshold alerts on the 1 h tumbling window. A rule fires when at least
# `min_readings` readings in the window breach it, so a single artefact
# reading does not page anyone, but a genuine short spike still does.
# --------------------------------------------------------------------------
# (alert_type, vital, operator, threshold, min_readings, severity)
THRESHOLD_ALERTS: list[tuple[str, str, str, float, int, str]] = [
    ("HR_HIGH", "heart_rate", ">", 130, 2, "high"),
    ("HR_LOW", "heart_rate", "<", 40, 2, "high"),
    ("SPO2_LOW", "spo2", "<", 90, 2, "critical"),
    ("SBP_LOW", "systolic_bp", "<", 90, 2, "high"),
    ("SBP_HIGH", "systolic_bp", ">", 200, 2, "medium"),
    ("TEMP_HIGH", "temperature", ">", 39.0, 2, "medium"),
    ("TEMP_LOW", "temperature", "<", 35.0, 2, "medium"),
]

# --------------------------------------------------------------------------
# Risk tiers (NEWS2-style clinical response bands).
# --------------------------------------------------------------------------
TIER_HIGH = 7
TIER_MEDIUM = 5


def risk_tier(total_score: int, red_flag: bool) -> str:
    if total_score >= TIER_HIGH:
        return "HIGH"
    if total_score >= TIER_MEDIUM or red_flag:
        return "MEDIUM"
    return "LOW"
