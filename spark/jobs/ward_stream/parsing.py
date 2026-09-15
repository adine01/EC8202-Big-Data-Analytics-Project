"""Parse and validate Kafka payloads with native Spark expressions (no Python UDFs).

Payloads are parsed into Spark 4's VARIANT type first. Unlike parsing
straight into a typed schema, VARIANT keeps the JSON type of every field, so
we can tell a missing field from `null`, a number from the string "78", and
reject `true` as a heart rate - the same verdicts as the Python validator in
ward_common.schemas, producing the same error codes.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ward_common.schemas import LAB_EVENT_FIELDS, LAB_TESTS, VITAL_RANGES, VITALS_FIELDS

TS_WITH_ZONE = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
PATIENT_ID_REGEX = r"^P[0-9]{3}$"
NUMERIC_VARIANT = r"^(TINYINT|SMALLINT|INT|BIGINT|FLOAT|DOUBLE|DECIMAL)"
REFERENCE_RANGE = r"^\s*(?:(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)|<\s*(\d+(?:\.\d+)?))\s*$"


# --- VARIANT helpers ---------------------------------------------------------

def _vtype(field: str) -> Column:
    """JSON type of a field ('STRING', 'BIGINT', 'DECIMAL(3,1)', 'VOID' for JSON null, ...); SQL NULL if absent."""
    return F.expr(f"schema_of_variant(variant_get(v, '$.{field}'))")


def _vget(field: str, spark_type: str) -> Column:
    return F.expr(f"try_variant_get(v, '$.{field}', '{spark_type}')")


def _present(field: str) -> Column:
    t = _vtype(field)
    return t.isNotNull() & (t != "VOID")


def _present_nonblank(field: str) -> Column:
    """For fields that come from CSV cells, where an empty cell means 'missing'."""
    blank = _is_string(field) & (F.trim(_vget(field, "string")) == "")
    return _present(field) & ~F.coalesce(blank, F.lit(False))


def _is_numeric(field: str) -> Column:
    return _vtype(field).rlike(NUMERIC_VARIANT)


def _is_string(field: str) -> Column:
    return _vtype(field) == "STRING"


def _code(condition: Column, code: str) -> Column:
    return F.when(condition, F.lit(code))


def _kafka_columns(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_payload"),
        "topic",
        "partition",
        "offset",
        F.concat_ws(":", "topic", "partition", "offset").alias("origin"),
    ).withColumn("v", F.expr("try_parse_json(raw_payload)"))


def _object_guard(errors: list[Column]) -> Column:
    """Field-level checks only make sense for a JSON object."""
    is_object = F.expr("schema_of_variant(v)").startswith("OBJECT")
    return F.when(F.col("v").isNull(), F.array(F.lit("invalid_json"))).when(
        ~is_object, F.array(F.lit("not_an_object"))
    ).otherwise(F.array_compact(F.array(*errors)))


# --- vitals ----------------------------------------------------------------------

def parse_vitals(raw: DataFrame, known_patients: DataFrame) -> DataFrame:
    """Kafka rows -> one row per message with typed fields and `error_reasons` (empty = valid).

    `known_patients` (patient_id) is a small static table; joining it to the
    stream is a stream-static join, re-read every micro-batch so roster
    changes are picked up.
    """
    df = _kafka_columns(raw)
    errors: list[Column] = [_code(~_present(f), f"missing:{f}") for f in VITALS_FIELDS]

    pid = _vget("patient_id", "string")
    pid_ok = _is_string("patient_id") & pid.rlike(PATIENT_ID_REGEX)
    errors.append(_code(_present("patient_id") & ~pid_ok, "bad_patient_id"))

    for vital, (low, high) in VITAL_RANGES.items():
        value = _vget(vital, "double")
        errors.append(_code(_present(vital) & ~_is_numeric(vital), f"type:{vital}"))
        errors.append(_code(_is_numeric(vital) & ~value.between(low, high), f"range:{vital}"))

    both_bp = _is_numeric("systolic_bp") & _is_numeric("diastolic_bp")
    errors.append(_code(both_bp & (_vget("diastolic_bp", "double") >= _vget("systolic_bp", "double")),
                        "bp_inverted"))

    for ts in ("timestamp", "produced_at"):
        text = _vget(ts, "string")
        ok = _is_string(ts) & text.rlike(TS_WITH_ZONE) & F.try_to_timestamp(text).isNotNull()
        errors.append(_code(_present(ts) & ~ok, f"bad_timestamp:{ts}"))

    df = df.select(
        "*",
        _object_guard(errors).alias("_errors"),
        pid.alias("patient_id"),
        _vget("event_id", "string").alias("event_id"),
        *[_vget(v, "double").alias(v) for v in VITAL_RANGES],
        F.try_to_timestamp(_vget("timestamp", "string")).alias("event_time"),
        F.try_to_timestamp(_vget("produced_at", "string")).alias("produced_at"),
    )

    known = known_patients.select(F.col("patient_id").alias("_known_id"))
    df = df.join(F.broadcast(known), df.patient_id == known._known_id, "left")
    unknown = pid_ok & F.col("_known_id").isNull()
    return df.withColumn(
        "error_reasons",
        F.when(unknown, F.array_union("_errors", F.array(F.lit("unknown_patient")))).otherwise(F.col("_errors")),
    ).drop("_errors", "_known_id", "v")


VALID_VITALS_COLUMNS = ["event_id", "patient_id", *VITAL_RANGES, "event_time", "produced_at"]


def valid_vitals(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.size("error_reasons") == 0).select(*VALID_VITALS_COLUMNS)


# --- labs ---------------------------------------------------------------------------

def parse_labs(raw: DataFrame, known_patients: DataFrame) -> DataFrame:
    """labs.raw rows (published by Airflow after its own DQ check) -> typed rows + error_reasons.

    Validated again here on purpose: the stream job must not trust any producer.
    """
    df = _kafka_columns(raw)
    present = _present_nonblank
    errors: list[Column] = [_code(~present(f), f"missing:{f}") for f in LAB_EVENT_FIELDS]

    pid = _vget("patient_id", "string")
    pid_ok = _is_string("patient_id") & pid.rlike(PATIENT_ID_REGEX)
    errors.append(_code(present("patient_id") & ~pid_ok, "bad_patient_id"))

    test = F.upper(F.trim(_vget("test_type", "string")))
    errors.append(_code(present("test_type") & ~test.isin(*LAB_TESTS), "unknown_test"))

    value = _vget("result_value", "double")
    errors.append(_code(present("result_value") & ~_is_numeric("result_value"), "non_numeric_result"))
    plausible = F.lit(False)
    for name, spec in LAB_TESTS.items():
        plausible = plausible | ((test == name) & value.between(*spec.plausible))
    errors.append(_code(_is_numeric("result_value") & test.isin(*LAB_TESTS) & ~plausible, "implausible_result"))

    ref_text = _vget("reference_range", "string")
    matched = ref_text.rlike(REFERENCE_RANGE)
    upper_only = F.regexp_extract(ref_text, REFERENCE_RANGE, 3)
    ref_low = F.when(upper_only != "", F.lit(0.0)).otherwise(F.regexp_extract(ref_text, REFERENCE_RANGE, 1).try_cast("double"))
    ref_high = F.when(upper_only != "", upper_only.try_cast("double")).otherwise(
        F.regexp_extract(ref_text, REFERENCE_RANGE, 2).try_cast("double"))
    errors.append(_code(present("reference_range") & ~(matched & (ref_low < ref_high)), "bad_reference_range"))

    collected_text = _vget("collected_at", "string")
    collected = F.try_to_timestamp(collected_text)
    errors.append(_code(present("collected_at") & ~(collected_text.rlike(TS_WITH_ZONE) & collected.isNotNull()),
                        "bad_collected_at"))

    file_day = F.try_to_date(_vget("file_day", "string"))
    errors.append(_code(collected.isNotNull() & file_day.isNotNull() & (F.to_date(collected) != file_day),
                        "collected_outside_file_day"))

    known = known_patients.select(F.col("patient_id").alias("_known_id"))
    df = df.join(F.broadcast(known), pid == known._known_id, "left")
    errors.append(_code(pid_ok & F.col("_known_id").isNull(), "unknown_patient"))

    return df.select(
        "raw_payload",
        "origin",
        _object_guard(errors).alias("error_reasons"),
        _vget("sample_id", "string").alias("sample_id"),
        test.alias("test_type"),
        pid.alias("patient_id"),
        value.alias("result_value"),
        _vget("unit", "string").alias("unit"),
        ref_text.alias("reference_range"),
        ref_low.alias("ref_low"),
        ref_high.alias("ref_high"),
        F.when(value < ref_low, "L").when(value > ref_high, "H").otherwise("N").alias("abnormal_flag"),
        collected.alias("collected_at"),
        file_day.alias("file_day"),
        _vget("source_file", "string").alias("source_file"),
    )


# --- dead letters -------------------------------------------------------------------

def deadletter_messages(rejected: DataFrame, source: str) -> DataFrame:
    """Rows with error_reasons -> Kafka (key, value) rows for the deadletter topic."""
    return rejected.select(
        F.lit(source).alias("key"),
        F.to_json(
            F.struct(
                F.lit(source).alias("source"),
                F.col("origin"),
                F.col("error_reasons"),
                F.col("raw_payload"),
                F.lit("spark").alias("detected_by"),
                F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("detected_at"),
            )
        ).alias("value"),
    )
