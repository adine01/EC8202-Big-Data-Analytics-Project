"""Event contracts shared by producers and consumers.

The vitals simulator produces events that follow `VITALS_FIELDS`; the Spark
job (Step 4) validates them against the same `VITAL_RANGES`, so producer and
consumer can never disagree about what "valid" means.

The ranges are *physiological plausibility* limits (what a real sensor could
report for a living patient), not clinical normal ranges. A value outside them
is treated as a sensor/transmission fault and dead-lettered; a value inside
them but abnormal (e.g. SpO2 85) is a genuine clinical signal and must reach
the alerting logic.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

VITALS_SCHEMA_VERSION = 1

# (min, max) inclusive plausibility limits.
VITAL_RANGES: dict[str, tuple[float, float]] = {
    "heart_rate": (20, 250),     # beats/min
    "spo2": (50, 100),           # %
    "systolic_bp": (50, 260),    # mmHg
    "diastolic_bp": (20, 160),   # mmHg
    "temperature": (30.0, 43.0), # °C
}

VITALS_FIELDS: tuple[str, ...] = (
    "schema_version",
    "event_id",
    "patient_id",
    "heart_rate",
    "spo2",
    "systolic_bp",
    "diastolic_bp",
    "temperature",
    "timestamp",    # simulated event time (ISO-8601, UTC) - used for windows
    "produced_at",  # real wall-clock time the monitor took the reading - for latency metrics
)

PATIENT_ID_PATTERN = re.compile(r"^P\d{3}$")


def patient_id(index: int) -> str:
    """1 -> 'P001'."""
    return f"P{index:03d}"


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _is_number(value: Any) -> bool:
    # bool is a subclass of int in Python; a JSON `true` is not a heart rate.
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_vitals_event(event: Any, known_patients: set[str] | None = None) -> list[str]:
    """Return a list of error codes; an empty list means the event is valid.

    Error codes are stable strings because they are stored as the dead-letter
    `error_reason` and counted in metrics.
    """
    if not isinstance(event, dict):
        return ["not_an_object"]

    errors: list[str] = []
    for name in VITALS_FIELDS:
        if event.get(name) is None:
            errors.append(f"missing:{name}")

    pid = event.get("patient_id")
    if pid is not None:
        if not isinstance(pid, str) or not PATIENT_ID_PATTERN.match(pid):
            errors.append("bad_patient_id")
        elif known_patients is not None and pid not in known_patients:
            errors.append("unknown_patient")

    for name, (low, high) in VITAL_RANGES.items():
        value = event.get(name)
        if value is None:
            continue
        if not _is_number(value):
            errors.append(f"type:{name}")
        elif not low <= value <= high:
            errors.append(f"range:{name}")

    sbp, dbp = event.get("systolic_bp"), event.get("diastolic_bp")
    if _is_number(sbp) and _is_number(dbp) and dbp >= sbp:
        errors.append("bp_inverted")

    for name in ("timestamp", "produced_at"):
        if event.get(name) is not None and _parse_ts(event[name]) is None:
            errors.append(f"bad_timestamp:{name}")

    return errors


# --------------------------------------------------------------------------
# Daily lab results (batch source)
# --------------------------------------------------------------------------

LAB_FILE_COLUMNS: tuple[str, ...] = (
    "sample_id",
    "patient_id",
    "test_type",
    "result_value",
    "unit",
    "reference_range",
    "collected_at",
)

LAB_FILE_PREFIX = "labs_"  # labs_YYYY-MM-DD.csv, one file per simulated day


def lab_file_name(day) -> str:
    return f"{LAB_FILE_PREFIX}{day.isoformat()}.csv"


class LabTest:
    """Catalogue entry for one test type.

    `reference` is the clinical normal range printed on the report (sex-specific
    where it differs); `plausible` is the physical limit beyond which the value
    must be a transcription/instrument error.
    """

    def __init__(self, unit: str, reference: dict[str, str], plausible: tuple[float, float]) -> None:
        self.unit = unit
        self.reference = reference
        self.plausible = plausible

    def reference_for(self, sex: str) -> str:
        return self.reference.get(sex, self.reference["*"])


LAB_TESTS: dict[str, LabTest] = {
    "WBC": LabTest("10^9/L", {"*": "4.0-11.0"}, (0.0, 200.0)),
    "CRP": LabTest("mg/L", {"*": "<5"}, (0.0, 500.0)),
    "LACTATE": LabTest("mmol/L", {"*": "0.5-2.0"}, (0.0, 30.0)),
    "CREATININE": LabTest("umol/L", {"M": "59-104", "F": "45-84", "*": "45-104"}, (10.0, 2000.0)),
    "POTASSIUM": LabTest("mmol/L", {"*": "3.5-5.3"}, (1.5, 10.0)),
    "HAEMOGLOBIN": LabTest("g/L", {"M": "130-180", "F": "115-165", "*": "115-180"}, (30.0, 250.0)),
    "TROPONIN": LabTest("ng/L", {"*": "<14"}, (0.0, 100000.0)),
}

_RANGE_PATTERN = re.compile(r"^\s*(?:(?P<low>\d+(?:\.\d+)?)\s*-\s*(?P<high>\d+(?:\.\d+)?)|<\s*(?P<upper>\d+(?:\.\d+)?))\s*$")


def parse_reference_range(text: Any) -> tuple[float, float] | None:
    """'4.0-11.0' -> (4.0, 11.0); '<5' -> (0.0, 5.0); anything else -> None."""
    if not isinstance(text, str):
        return None
    match = _RANGE_PATTERN.match(text)
    if not match:
        return None
    if match.group("upper") is not None:
        return 0.0, float(match.group("upper"))
    low, high = float(match.group("low")), float(match.group("high"))
    return (low, high) if low < high else None


def validate_lab_row(row: dict, known_patients: set[str] | None = None, file_day=None) -> list[str]:
    """Row-level data-quality check for one CSV row (all values arrive as strings)."""
    errors: list[str] = []
    for name in LAB_FILE_COLUMNS:
        value = row.get(name)
        if value is None or str(value).strip() == "":
            errors.append(f"missing:{name}")

    pid = (row.get("patient_id") or "").strip()
    if pid:
        if not PATIENT_ID_PATTERN.match(pid):
            errors.append("bad_patient_id")
        elif known_patients is not None and pid not in known_patients:
            errors.append("unknown_patient")

    test = LAB_TESTS.get((row.get("test_type") or "").strip().upper())
    if row.get("test_type") and test is None:
        errors.append("unknown_test")

    raw_value = (row.get("result_value") or "").strip()
    if raw_value:
        try:
            value = float(raw_value)
        except ValueError:
            errors.append("non_numeric_result")
        else:
            if test is not None and not test.plausible[0] <= value <= test.plausible[1]:
                errors.append("implausible_result")

    if row.get("reference_range") and parse_reference_range(row["reference_range"]) is None:
        errors.append("bad_reference_range")

    if row.get("collected_at"):
        collected = _parse_ts(row["collected_at"])
        if collected is None:
            errors.append("bad_collected_at")
        elif file_day is not None and collected.date() != file_day:
            errors.append("collected_outside_file_day")

    return errors


# --------------------------------------------------------------------------
# Kafka message contracts beyond the raw vitals
# --------------------------------------------------------------------------

# labs.raw: one message per validated CSV row, published by Airflow (Step 5).
# result_value is sent as a number; everything else as in the CSV.
LAB_EVENT_FIELDS: tuple[str, ...] = LAB_FILE_COLUMNS + (
    "schema_version",
    "source_file",
    "file_day",     # YYYY-MM-DD the file covers
    "row_number",   # 1-based data row in the source file
)
LAB_EVENT_SCHEMA_VERSION = 1

# deadletter: one message per rejected record, from any stage.
#   source         "vitals" | "labs"
#   origin         where the record came from, unique per record, e.g.
#                  "vitals.raw:3:1234" (topic:partition:offset) or
#                  "labs_2026-01-04.csv:17" (file:row)
#   error_reasons  list of stable error codes (see validators above)
#   raw_payload    the original record as text
#   detected_by    component that rejected it ("spark", "airflow")
#   detected_at    real UTC time of rejection
DEADLETTER_FIELDS: tuple[str, ...] = (
    "source", "origin", "error_reasons", "raw_payload", "detected_by", "detected_at",
)
