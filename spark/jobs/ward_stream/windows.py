"""Event-time window aggregations over validated, de-duplicated vitals.

All windows are in *simulated* time (the event `timestamp`), so their clinical
meaning does not change with SIM_DAY_SECONDS. At the default x288:
    1 h tumbling window    = 12.5 real seconds
    4 h / 1 h sliding      = 50 s long, a new window every 12.5 s
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ward_common.scoring_rules import THRESHOLD_ALERTS, TREND_SLIDE, TREND_WINDOW

# vital -> short column prefix used in Postgres
SHORT = {
    "heart_rate": "hr",
    "spo2": "spo2",
    "systolic_bp": "sbp",
    "diastolic_bp": "dbp",
    "temperature": "temp",
}
TREND_VITALS = ("heart_rate", "spo2", "systolic_bp", "temperature")


def tumbling_1h(vitals: DataFrame) -> DataFrame:
    aggs = [F.count(F.lit(1)).alias("n_readings")]
    for vital, short in SHORT.items():
        aggs += [
            F.avg(vital).alias(f"{short}_avg"),
            F.min(vital).alias(f"{short}_min"),
            F.max(vital).alias(f"{short}_max"),
        ]
    aggs += [
        F.max("event_time").alias("last_event_time"),
        F.max("produced_at").alias("last_produced_at"),
    ]
    # Count readings breaching each alert rule, so the alert can require >= N readings.
    for alert_type, vital, op, threshold, _min_n, _sev in THRESHOLD_ALERTS:
        breach = (F.col(vital) > threshold) if op == ">" else (F.col(vital) < threshold)
        aggs.append(F.sum(F.when(breach, 1).otherwise(0)).alias(f"breach_{alert_type}"))

    return (
        vitals.groupBy("patient_id", F.window("event_time", "1 hour"))
        .agg(*aggs)
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "*")
        .drop("window")
    )


def sliding_trend(vitals: DataFrame) -> DataFrame:
    """Least-squares slope of each vital against time, in units per simulated hour.

    slope = cov(value, t) / var(t). try_divide returns NULL instead of failing
    (ANSI mode) when a window holds a single reading and var(t) = 0.
    """
    hours = F.unix_micros("event_time") / F.lit(3.6e9)
    aggs = [F.count(F.lit(1)).alias("n_readings")]
    for vital in TREND_VITALS:
        aggs.append(F.try_divide(F.covar_pop(vital, hours), F.var_pop(hours)).alias(f"{SHORT[vital]}_slope"))
    return (
        vitals.groupBy("patient_id", F.window("event_time", TREND_WINDOW, TREND_SLIDE))
        .agg(*aggs)
        .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"), "*")
        .drop("window")
    )
