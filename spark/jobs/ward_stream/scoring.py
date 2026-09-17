"""Spark column expressions generated from ward_common.scoring_rules.

Nothing here hard-codes a threshold: every band, weight and cut-off is read
from the shared rules module, so the stream job, the daily report and the
API cannot drift apart. tests/spark/test_scoring_parity.py checks that these
expressions give the same answers as the reference Python functions.
"""

from __future__ import annotations

import math

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ward_common.scoring_rules import (
    LAB_ADJUSTMENT_CAP,
    LAB_LOOKBACK_HOURS,
    LAB_RULES,
    NEWS2_BANDS,
    TIER_HIGH,
    TIER_MEDIUM,
    TREND_INDEX_THRESHOLD,
    TREND_SIGNALS,
)


def news2_points(vital: str, value: Column) -> Column:
    bands = NEWS2_BANDS[vital]
    expr = None
    for bound, points in bands:
        if math.isinf(bound):
            return expr.otherwise(F.lit(points)) if expr is not None else F.lit(points)
        expr = F.when(value <= bound, F.lit(points)) if expr is None else expr.when(value <= bound, F.lit(points))
    return expr.otherwise(F.lit(0))


def with_early_warning_score(df: DataFrame, value_columns: dict[str, str]) -> DataFrame:
    """value_columns maps vital -> column holding its value (e.g. window average)."""
    points = [F.coalesce(news2_points(vital, F.col(col)), F.lit(0)) for vital, col in value_columns.items()]
    total = points[0]
    for p in points[1:]:
        total = total + p
    red_flag = F.greatest(*points) >= 3
    return df.withColumn("ews_score", total.cast("int")).withColumn("ews_red_flag", red_flag)


def risk_tier(total: Column, red_flag: Column) -> Column:
    return (
        F.when(total >= TIER_HIGH, F.lit("HIGH"))
        .when((total >= TIER_MEDIUM) | red_flag, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW"))
    )


def with_trend(df: DataFrame, slope_columns: dict[str, str], min_readings: int) -> DataFrame:
    """Adds deterioration_index, improvement_index and trend from per-vital slopes."""
    worse, better = F.lit(0.0), F.lit(0.0)
    for vital, (direction, scale) in TREND_SIGNALS.items():
        z = F.coalesce(F.col(slope_columns[vital]), F.lit(0.0)) * F.lit(direction / scale)
        worse = worse + F.greatest(z, F.lit(0.0))
        better = better + F.greatest(-z, F.lit(0.0))
    trend = (
        F.when(F.col("n_readings") < min_readings, F.lit("insufficient_data"))
        .when((worse >= TREND_INDEX_THRESHOLD) & (worse > better), F.lit("deteriorating"))
        .when((better >= TREND_INDEX_THRESHOLD) & (better > worse), F.lit("improving"))
        .otherwise(F.lit("stable"))
    )
    return df.withColumn("deterioration_index", worse).withColumn("improvement_index", better).withColumn("trend", trend)


def lab_adjustments(windows: DataFrame, labs: DataFrame) -> DataFrame:
    """Stream/batch join: each vitals window x lab results collected in the 48 h before it ended.

    windows: patient_id, window_start, window_end
    labs:    patient_id, test_type, result_value, collected_at
    returns: patient_id, window_start, lab_adjustment, lab_flags
    """
    w, lab = windows.alias("w"), labs.alias("l")
    joined = w.join(
        lab,
        (F.col("w.patient_id") == F.col("l.patient_id"))
        & (F.col("l.collected_at") <= F.col("w.window_end"))
        & (F.col("l.collected_at") > F.col("w.window_end") - F.expr(f"INTERVAL {LAB_LOOKBACK_HOURS} HOURS")),
    )
    latest = joined.groupBy("w.patient_id", "w.window_start", "l.test_type").agg(
        F.max_by("l.result_value", "l.collected_at").alias("value")
    )

    # Every matching rule becomes a (points, flag) struct; the highest one per test wins.
    candidates = [
        F.when(
            (F.col("test_type") == test) & ((F.col("value") > threshold) if op == ">" else (F.col("value") < threshold)),
            F.struct(F.lit(points).alias("points"), F.lit(flag).alias("flag")),
        )
        for test, op, threshold, points, flag in LAB_RULES
    ]
    best = F.array_max(F.array_compact(F.array(*candidates)))
    per_test = latest.withColumn("best", best)

    return per_test.groupBy("patient_id", "window_start").agg(
        F.least(F.coalesce(F.sum("best.points"), F.lit(0)), F.lit(LAB_ADJUSTMENT_CAP)).cast("int").alias("lab_adjustment"),
        F.sort_array(F.collect_list("best.flag")).alias("lab_flags"),
    )
