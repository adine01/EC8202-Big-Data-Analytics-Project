"""Lab file data-quality logic, free of Airflow imports so it is unit-testable locally.

A file goes through:
  1. checksum (sha256 of the bytes)       -> idempotency key for the whole file
  2. header check                         -> wrong layout = quarantine, nothing published
  3. row validation (shared validator)    -> per-row reasons
     + duplicate (sample_id, test_type)   -> later copies rejected as duplicate_row
  4. decision: reject rate above the limit -> quarantine; otherwise publish valid rows
     to labs.raw and rejected rows to the dead-letter topic
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from ward_common.schemas import (
    LAB_EVENT_SCHEMA_VERSION,
    LAB_FILE_COLUMNS,
    validate_lab_row,
)

FILE_NAME = re.compile(r"^labs_(\d{4}-\d{2}-\d{2})\.csv$")
ON_TIME_GRACE = timedelta(hours=1)  # the simulator uploads on-time files within 30 sim-minutes


@dataclass
class RejectedRow:
    row_number: int
    row: dict
    reasons: list[str]


@dataclass
class FileCheck:
    path: Path
    file_day: date | None
    checksum: str
    header_ok: bool
    valid: list[dict] = field(default_factory=list)       # each has "row_number"
    rejected: list[RejectedRow] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.valid) + len(self.rejected)

    @property
    def reject_rate(self) -> float:
        return len(self.rejected) / self.total if self.total else 1.0

    @property
    def reason_counts(self) -> dict[str, int]:
        # First reason per row (full code, e.g. "missing:unit"): small, stable cardinality.
        return dict(Counter(r.reasons[0] for r in self.rejected))


def file_day_from_name(name: str) -> date | None:
    match = FILE_NAME.match(name)
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def check_file(path: Path, known_patients: set[str] | None) -> FileCheck:
    data = path.read_bytes()
    check = FileCheck(
        path=path,
        file_day=file_day_from_name(path.name),
        checksum=hashlib.sha256(data).hexdigest(),
        header_ok=False,
    )
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return check
    reader = csv.DictReader(io.StringIO(text))
    if check.file_day is None or tuple(reader.fieldnames or ()) != LAB_FILE_COLUMNS:
        return check
    check.header_ok = True

    seen: set[tuple[str, str]] = set()
    for number, row in enumerate(reader, start=1):
        if None in row:  # more cells than header columns
            reasons = ["wrong_column_count"]
        else:
            reasons = validate_lab_row(row, known_patients, check.file_day)
        key = ((row.get("sample_id") or "").strip(), (row.get("test_type") or "").strip().upper())
        if not reasons and key in seen:
            reasons = ["duplicate_row"]
        if reasons:
            check.rejected.append(RejectedRow(number, {k: v for k, v in row.items() if k is not None}, reasons))
        else:
            seen.add(key)
            check.valid.append({**row, "row_number": number})
    return check


def decide(check: FileCheck, max_reject_rate: float) -> str:
    """'publish' or 'quarantine'. A file that is mostly bad is held back as a whole:
    publishing its few good rows would present a partial day as if it were complete."""
    if not check.header_ok or check.total == 0 or check.reject_rate > max_reject_rate:
        return "quarantine"
    return "publish"


def lab_event(row: dict, check: FileCheck) -> dict:
    """labs.raw message for a validated row (contract: ward_common.schemas.LAB_EVENT_FIELDS)."""
    event = {name: row[name].strip() for name in LAB_FILE_COLUMNS}
    event["test_type"] = event["test_type"].upper()
    event["result_value"] = float(event["result_value"])
    event.update(
        schema_version=LAB_EVENT_SCHEMA_VERSION,
        source_file=check.path.name,
        file_day=check.file_day.isoformat(),
        row_number=row["row_number"],
    )
    return event


def deadletter_event(rejected: RejectedRow, check: FileCheck, detected_at: datetime) -> dict:
    return {
        "source": "labs",
        "origin": f"{check.path.name}:{rejected.row_number}",
        "error_reasons": rejected.reasons,
        "raw_payload": json.dumps(rejected.row),
        "detected_by": "airflow",
        "detected_at": detected_at.isoformat(),
    }


def expected_upload(day: date, upload_hour: float) -> datetime:
    """Simulated time pathology normally uploads day D's file (upload_hour on D+1)."""
    return datetime.combine(day + timedelta(days=1), time(0), tzinfo=timezone.utc) + timedelta(hours=upload_hour)


def classify_arrival(arrived_at: datetime, expected_at: datetime) -> str:
    return "on_time" if arrived_at <= expected_at + ON_TIME_GRACE else "late"
