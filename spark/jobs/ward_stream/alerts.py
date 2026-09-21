"""Derive per-patient alerts from scored windows.

Alert id = hash(patient, type, window_start): processing the same window
again (a later micro-batch updating a still-open window, or a replay after a
crash) refreshes the same alert instead of raising a duplicate.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ward_common.scoring_rules import THRESHOLD_ALERTS, TIER_HIGH, TREND_INDEX_THRESHOLD

from ward_stream.windows import SHORT

ALERT_COLUMNS = [
    "alert_id", "patient_id", "alert_type", "severity", "window_start", "window_end",
    "observed_value", "threshold", "message",
]


def _alert(condition: Column, alert_type: str, severity: str, observed: Column, threshold: float,
           message: Column) -> Column:
    return F.when(
        condition,
        F.struct(
            F.lit(alert_type).alias("alert_type"),
            F.lit(severity).alias("severity"),
            observed.cast("double").alias("observed_value"),
            F.lit(float(threshold)).alias("threshold"),
            message.alias("message"),
        ),
    )


def _explode(df: DataFrame, candidates: list[Column]) -> DataFrame:
    exploded = df.select(
        "patient_id", "window_start", "window_end",
        F.explode(F.array_compact(F.array(*candidates))).alias("a"),
    ).select("patient_id", "window_start", "window_end", "a.*")
    alert_id = F.substring(
        F.sha2(F.concat_ws("|", "patient_id", "alert_type", F.col("window_start").cast("string")), 256), 1, 32
    )
    return exploded.withColumn("alert_id", alert_id).select(*ALERT_COLUMNS)


def window_alerts(scored: DataFrame) -> DataFrame:
    """Threshold breaches (>= N readings in the 1 h window) and high overall risk."""
    candidates = []
    for alert_type, vital, op, threshold, min_n, severity in THRESHOLD_ALERTS:
        worst = F.col(f"{SHORT[vital]}_max" if op == ">" else f"{SHORT[vital]}_min")
        breaches = F.col(f"breach_{alert_type}")
        candidates.append(_alert(
            breaches >= min_n, alert_type, severity, worst, threshold,
            F.format_string(f"{vital} {op} {threshold}: worst %.1f, %d readings in window", worst, breaches),
        ))
    candidates.append(_alert(
        F.col("risk_tier") == "HIGH", "RISK_HIGH", "critical", F.col("total_score"), TIER_HIGH,
        F.format_string("risk score %d (EWS %d + labs %d + trend %d)",
                        "total_score", "ews_score", "lab_adjustment", "trend_adjustment"),
    ))
    return _explode(scored, candidates)


def trend_alerts(trends: DataFrame) -> DataFrame:
    candidates = [_alert(
        F.col("trend") == "deteriorating", "TREND_DETERIORATING", "medium",
        F.col("deterioration_index"), TREND_INDEX_THRESHOLD,
        F.format_string("sustained deterioration over 4 h (index %.1f; HR %+.1f/h, SpO2 %+.2f/h, SBP %+.1f/h, T %+.2f/h)",
                        "deterioration_index", "hr_slope", "spo2_slope", "sbp_slope", "temp_slope"),
    )]
    return _explode(trends, candidates)
