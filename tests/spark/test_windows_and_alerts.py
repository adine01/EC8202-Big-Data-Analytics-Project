from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from ward_stream.alerts import trend_alerts, window_alerts
from ward_stream.scoring import risk_tier, with_early_warning_score, with_trend
from ward_stream.windows import sliding_trend, tumbling_1h

T0 = datetime(2026, 1, 3, 8, 0)
SCHEMA = ("event_id string, patient_id string, heart_rate double, spo2 double, systolic_bp double, "
          "diastolic_bp double, temperature double, event_time timestamp, produced_at timestamp")


def readings(spark, patient="P001", minutes=5, count=48, hr=lambda i: 80.0, spo2=lambda i: 97.0,
             sbp=lambda i: 120.0, temp=lambda i: 37.0, start=T0):
    rows = []
    for i in range(count):
        t = start + timedelta(minutes=minutes * i)
        rows.append((f"{patient}-{i}", patient, float(hr(i)), float(spo2(i)), float(sbp(i)), 75.0,
                     float(temp(i)), t, t))
    return spark.createDataFrame(rows, SCHEMA)


def test_tumbling_window_counts_and_breaches(spark):
    # 12 readings per sim hour; two SpO2 readings below 90 in the first hour.
    df = readings(spark, count=24, spo2=lambda i: 86.0 if i in (3, 4) else 97.0)
    rows = {r["window_start"]: r for r in tumbling_1h(df).collect()}
    first, second = rows[T0], rows[T0 + timedelta(hours=1)]
    assert first["n_readings"] == 12 and second["n_readings"] == 12
    assert first["spo2_min"] == 86.0 and first["breach_SPO2_LOW"] == 2
    assert second["breach_SPO2_LOW"] == 0


def test_sliding_window_slope_is_units_per_hour(spark):
    # Heart rate rising 1 bpm every 5 minutes = 12 bpm per hour.
    df = readings(spark, count=48, hr=lambda i: 70 + i)
    rows = sliding_trend(df).where(F.col("n_readings") >= 40).collect()
    assert rows and all(r["hr_slope"] == pytest.approx(12.0) for r in rows)
    assert all(r["spo2_slope"] == pytest.approx(0.0) for r in rows)


def test_single_reading_window_has_null_slope_not_error(spark):
    df = readings(spark, count=1)
    assert sliding_trend(df).first()["hr_slope"] is None


def score(df):
    df = with_early_warning_score(df, {"heart_rate": "hr_avg", "spo2": "spo2_avg",
                                       "systolic_bp": "sbp_avg", "temperature": "temp_avg"})
    return (df.withColumn("lab_adjustment", F.lit(0)).withColumn("trend_adjustment", F.lit(0))
              .withColumn("total_score", F.col("ews_score"))
              .withColumn("risk_tier", risk_tier(F.col("total_score"), F.col("ews_red_flag"))))


def test_window_alerts_fire_on_repeated_breach_and_high_risk(spark):
    sick = readings(spark, count=12, hr=lambda i: 140, spo2=lambda i: 85, sbp=lambda i: 85, temp=lambda i: 39.5)
    one_blip = readings(spark, patient="P002", count=12, spo2=lambda i: 85 if i == 0 else 97)
    alerts = window_alerts(score(tumbling_1h(sick.unionByName(one_blip)))).collect()
    types = {(a["patient_id"], a["alert_type"]) for a in alerts}
    assert {("P001", "HR_HIGH"), ("P001", "SPO2_LOW"), ("P001", "SBP_LOW"),
            ("P001", "TEMP_HIGH"), ("P001", "RISK_HIGH")} <= types
    assert not any(pid == "P002" for pid, _ in types)  # a single-reading artefact does not alert


def test_alert_ids_are_deterministic(spark):
    df = score(tumbling_1h(readings(spark, count=12, spo2=lambda i: 85)))
    first = sorted(a["alert_id"] for a in window_alerts(df).collect())
    second = sorted(a["alert_id"] for a in window_alerts(df).collect())
    assert first == second and len(first[0]) == 32


def test_deteriorating_trend_raises_alert(spark):
    df = readings(spark, count=48, hr=lambda i: 70 + i * 0.5, temp=lambda i: 37 + i * 0.02,
                  sbp=lambda i: 130 - i * 0.4)
    trends = with_trend(sliding_trend(df), {"heart_rate": "hr_slope", "spo2": "spo2_slope",
                                            "systolic_bp": "sbp_slope", "temperature": "temp_slope"}, 20)
    alerts = trend_alerts(trends).collect()
    assert alerts and {a["alert_type"] for a in alerts} == {"TREND_DETERIORATING"}
