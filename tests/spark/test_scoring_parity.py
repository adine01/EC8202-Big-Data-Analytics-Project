"""The Spark expressions must score exactly like the reference Python rules."""

import random
from datetime import datetime, timedelta

from pyspark.sql import functions as F

from ward_common import scoring_rules as rules
from ward_stream.scoring import lab_adjustments, news2_points, risk_tier, with_early_warning_score, with_trend

VALUES = {
    "heart_rate": [30, 40, 40.5, 50, 55, 90, 90.4, 110, 111, 130, 131, 180],
    "spo2": [85, 91, 91.5, 93, 94, 95, 95.5, 96, 100],
    "systolic_bp": [70, 90, 95, 100, 105, 110, 150, 219, 219.5, 240],
    "temperature": [34.0, 35.0, 35.5, 36.0, 36.5, 38.0, 38.5, 39.0, 39.5],
}


def test_news2_points_match(spark):
    for vital, values in VALUES.items():
        df = spark.createDataFrame([(float(v),) for v in values], "x double")
        got = [r[0] for r in df.select(news2_points(vital, F.col("x"))).collect()]
        assert got == [rules.news2_points(vital, v) for v in values], vital


def test_early_warning_score_and_tier_match(spark):
    rng = random.Random(4)
    rows = [tuple(float(rng.choice(VALUES[v])) for v in rules.NEWS2_BANDS) for _ in range(300)]
    df = spark.createDataFrame(rows, "hr double, spo2 double, sbp double, temp double")
    scored = with_early_warning_score(df, {"heart_rate": "hr", "spo2": "spo2", "systolic_bp": "sbp", "temperature": "temp"})
    scored = scored.withColumn("tier", risk_tier(F.col("ews_score"), F.col("ews_red_flag")))
    for row, result in zip(rows, scored.collect()):
        score, red = rules.early_warning_score(dict(zip(rules.NEWS2_BANDS, row)))
        assert (result["ews_score"], result["ews_red_flag"]) == (score, red)
        assert result["tier"] == rules.risk_tier(score, red)


def test_trend_classification_matches(spark):
    rng = random.Random(5)
    rows = [(rng.gauss(0, 4), rng.gauss(0, 0.7), rng.gauss(0, 5), rng.gauss(0, 0.15), 40) for _ in range(500)]
    df = spark.createDataFrame(rows, "hr double, spo2 double, sbp double, temp double, n_readings int")
    cols = {"heart_rate": "hr", "spo2": "spo2", "systolic_bp": "sbp", "temperature": "temp"}
    got = [r["trend"] for r in with_trend(df, cols, min_readings=20).collect()]
    expected = [rules.classify_trend(dict(zip(cols, row[:4]))) for row in rows]
    assert got == expected
    assert {"deteriorating", "improving", "stable"} <= set(expected)  # the sample covers all classes


def test_trend_needs_enough_readings(spark):
    df = spark.createDataFrame([(20.0, -3.0, -20.0, 1.0, 5)], "hr double, spo2 double, sbp double, temp double, n_readings int")
    cols = {"heart_rate": "hr", "spo2": "spo2", "systolic_bp": "sbp", "temperature": "temp"}
    assert with_trend(df, cols, min_readings=20).first()["trend"] == "insufficient_data"


def test_lab_adjustment_matches_and_respects_lookback(spark):
    rng = random.Random(6)
    end = datetime(2026, 1, 5, 12, 0)
    windows, labs, expected = [], [], {}
    candidates = {
        "LACTATE": [1.0, 2.5, 4.5], "CRP": [3, 120], "WBC": [2.5, 7, 15], "CREATININE": [80, 200],
        "POTASSIUM": [2.8, 4.2, 6.4], "HAEMOGLOBIN": [80, 130], "TROPONIN": [5, 40],
    }
    for i in range(1, 41):
        pid = f"P{i:03d}"
        windows.append((pid, end - timedelta(hours=1), end))
        latest = {}
        for test in rng.sample(sorted(candidates), rng.randint(0, 7)):
            old, new = rng.choice(candidates[test]), rng.choice(candidates[test])
            labs.append((pid, test, float(old), end - timedelta(hours=30)))
            labs.append((pid, test, float(new), end - timedelta(hours=2)))       # newest wins
            labs.append((pid, test, 999.0, end - timedelta(hours=60)))           # outside 48 h: ignored
            labs.append((pid, test, 999.0, end + timedelta(hours=1)))            # after window: ignored
            latest[test] = float(new)
        expected[pid] = rules.lab_adjustment(latest)

    w = spark.createDataFrame(windows, "patient_id string, window_start timestamp, window_end timestamp")
    lab = spark.createDataFrame(labs, "patient_id string, test_type string, result_value double, collected_at timestamp")
    got = {r["patient_id"]: (r["lab_adjustment"], list(r["lab_flags"])) for r in lab_adjustments(w, lab).collect()}
    for pid, (points, flags) in expected.items():
        assert got.get(pid, (0, [])) == (points, flags), pid
