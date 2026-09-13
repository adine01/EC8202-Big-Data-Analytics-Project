"""Daily pathology results, driven by the same hidden patient state as the vitals.

Because the roster and episode schedules are deterministic (see patients.py),
a septic patient whose heart rate is climbing in the stream will also show a
rising CRP and white cell count here, without the two simulators talking to
each other.

Each lab marker reacts to deterioration with a clinically plausible *lag*:
lactate and haemoglobin move immediately, CRP peaks many hours later. That lag
is what makes "yesterday's labs change the risk picture going forward"
interesting: labs can confirm a trend the vitals only hinted at.

Everything for a given (seed, patient, day) is seeded, so regenerating a day
produces a byte-identical file - which lets Step 5 prove its loads are idempotent.
"""

from __future__ import annotations

import csv
import io
import os
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

from ward_common.schemas import LAB_FILE_COLUMNS, LAB_TESTS, lab_file_name

from ward_sim.patients import EpisodeSchedule, Patient

# Change from the patient's baseline at full severity (severity 1.0).
LAB_EFFECTS: dict[str, dict[str, float]] = {
    "sepsis": {"WBC": 11, "CRP": 220, "LACTATE": 3.2, "CREATININE": 70, "POTASSIUM": 0.4, "TROPONIN": 25},
    "respiratory_failure": {"WBC": 4, "CRP": 60, "LACTATE": 1.6, "POTASSIUM": 0.2, "TROPONIN": 10},
    "haemorrhage": {"HAEMOGLOBIN": -45, "LACTATE": 3.0, "CREATININE": 45, "POTASSIUM": 0.3,
                    "TROPONIN": 40, "WBC": 3},
    "recovering": {"WBC": 7, "CRP": 150, "LACTATE": 0.8, "CREATININE": 20},
    "stable": {},
}

# How many simulated hours each marker lags behind the underlying deterioration.
LAG_HOURS: dict[str, float] = {
    "WBC": 6, "CRP": 18, "LACTATE": 0, "CREATININE": 12, "POTASSIUM": 6, "HAEMOGLOBIN": 0, "TROPONIN": 3,
}

# Combined biological + analytical variation, as a coefficient of variation.
NOISE_CV: dict[str, float] = {
    "WBC": 0.08, "CRP": 0.15, "LACTATE": 0.10, "CREATININE": 0.05,
    "POTASSIUM": 0.03, "HAEMOGLOBIN": 0.03, "TROPONIN": 0.15,
}

DECIMALS: dict[str, int] = {
    "WBC": 1, "CRP": 1, "LACTATE": 1, "CREATININE": 0, "POTASSIUM": 1, "HAEMOGLOBIN": 0, "TROPONIN": 0,
}

ROUTINE_PANEL = ("WBC", "HAEMOGLOBIN", "CREATININE", "POTASSIUM", "CRP")  # "morning bloods"
REPEAT_PANEL = ("LACTATE", "POTASSIUM", "HAEMOGLOBIN")                   # evening re-check if unwell


def _at(day: date, hour: float) -> datetime:
    return datetime.combine(day, time(0), tzinfo=timezone.utc) + timedelta(hours=hour)


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="seconds").replace("+00:00", "Z")


def lab_baseline(patient: Patient) -> dict[str, float]:
    """A patient's personal 'normal' for each marker (stable across days)."""
    rng = random.Random(f"{patient.seed}:{patient.patient_id}:lab-baseline")
    male = patient.sex == "M"
    return {
        "WBC": max(rng.gauss(7.0, 1.4), 4.5),
        "CRP": rng.uniform(1.0, 4.0),
        "LACTATE": min(max(rng.gauss(1.0, 0.2), 0.6), 1.6),
        "CREATININE": rng.gauss(85 if male else 68, 10) + 0.2 * max(patient.age - 50, 0),
        "POTASSIUM": min(max(rng.gauss(4.2, 0.25), 3.7), 4.8),
        "HAEMOGLOBIN": rng.gauss(148 if male else 132, 8),
        "TROPONIN": rng.uniform(2, 9),
    }


class LabDayGenerator:
    def __init__(self, roster: list[Patient]) -> None:
        self.roster = roster
        self.schedules = {p.patient_id: EpisodeSchedule(p) for p in roster}
        self.baselines = {p.patient_id: lab_baseline(p) for p in roster}

    def _value(self, patient: Patient, test: str, at: datetime, rng: random.Random) -> str:
        severity = self.schedules[patient.patient_id].severity(at - timedelta(hours=LAG_HOURS[test]))
        effect = LAB_EFFECTS[patient.scenario].get(test, 0.0) * severity
        value = (self.baselines[patient.patient_id][test] + effect) * (1 + rng.gauss(0, NOISE_CV[test]))
        low, high = LAB_TESTS[test].plausible
        value = min(max(value, max(low, 0.1)), high)
        return f"{value:.{DECIMALS[test]}f}"

    def _sample_rows(self, patient: Patient, sample_id: str, at: datetime, tests, rng) -> list[dict]:
        return [
            {
                "sample_id": sample_id,
                "patient_id": patient.patient_id,
                "test_type": test,
                "result_value": self._value(patient, test, at, rng),
                "unit": LAB_TESTS[test].unit,
                "reference_range": LAB_TESTS[test].reference_for(patient.sex),
                "collected_at": _iso(at),
            }
            for test in tests
        ]

    def rows_for_day(self, day: date) -> list[dict]:
        """All (clean) results collected on `day`, ordered by collection time."""
        rows: list[dict] = []
        for patient in self.roster:
            rng = random.Random(f"{patient.seed}:{patient.patient_id}:{day.isoformat()}:labs")
            schedule = self.schedules[patient.patient_id]
            worst_today = max(schedule.severity(_at(day, h)) for h in range(24))

            # Morning bloods: almost everyone; always if unwell at any point today.
            if worst_today > 0.1 or rng.random() < 0.9:
                at = _at(day, 6 + rng.uniform(0, 3))
                severity_now = schedule.severity(at)
                tests = list(ROUTINE_PANEL)
                if severity_now > 0.2 or rng.random() < 0.15:
                    tests.append("LACTATE")
                if (severity_now > 0.3 and patient.scenario != "recovering") or rng.random() < 0.08:
                    tests.append("TROPONIN")
                rows += self._sample_rows(patient, f"{day:%Y%m%d}-{patient.patient_id}-AM", at, tests, rng)

            # Evening re-check for patients who were clearly unwell today.
            if worst_today > 0.4:
                at = _at(day, 16 + rng.uniform(0, 5))
                tests = list(REPEAT_PANEL) + (["TROPONIN"] if patient.scenario == "haemorrhage" else [])
                rows += self._sample_rows(patient, f"{day:%Y%m%d}-{patient.patient_id}-PM", at, tests, rng)

        rows.sort(key=lambda r: (r["collected_at"], r["patient_id"], r["test_type"]))
        return rows


# --------------------------------------------------------------- bad rows

LAB_FAULT_KINDS = (
    "missing_patient_id",
    "unknown_patient",
    "non_numeric_result",
    "implausible_result",
    "unknown_test",
    "bad_reference_range",
    "bad_collected_at",
    "wrong_day",
    "duplicate_row",
)


def inject_bad_rows(rows: list[dict], day: date, seed: int, rate: float) -> tuple[list[dict], list[str]]:
    """Corrupt roughly `rate` of the rows the way real LIS exports go wrong."""
    rng = random.Random(f"{seed}:{day.isoformat()}:lab-faults")
    out: list[dict] = []
    faults: list[str] = []
    for row in rows:
        if rng.random() >= rate:
            out.append(row)
            continue
        kind = rng.choice(LAB_FAULT_KINDS)
        bad = dict(row)
        if kind == "missing_patient_id":
            bad["patient_id"] = ""
        elif kind == "unknown_patient":
            bad["patient_id"] = f"P{rng.randint(900, 999)}"
        elif kind == "non_numeric_result":
            bad["result_value"] = rng.choice(["haemolysed", "ERR", "see comment", "clotted"])
        elif kind == "implausible_result":
            bad["result_value"] = rng.choice(["-1", "99999999"])
        elif kind == "unknown_test":
            bad["test_type"] = rng.choice(["GLU", "HBA1C", "XYZ"])
        elif kind == "bad_reference_range":
            bad["reference_range"] = rng.choice(["normal", "4.0 to 11.0", "n/a"])
        elif kind == "bad_collected_at":
            bad["collected_at"] = rng.choice(["03/01/2026 07:10", "morning", "2026-02-30T07:10:00Z"])
        elif kind == "wrong_day":
            bad["collected_at"] = _iso(_at(day - timedelta(days=1), rng.uniform(8, 20)))
        elif kind == "duplicate_row":
            out.append(row)  # the row itself is fine; it simply appears twice
        out.append(bad)
        faults.append(kind)
    return out, faults


# ----------------------------------------------------------- upload plan


@dataclass(frozen=True)
class UploadPlan:
    day: date
    missing: bool
    scheduled_at: datetime      # when pathology normally uploads (LAB_UPLOAD_HOUR on D+1)
    upload_at: datetime | None  # simulated time the file actually appears in landing/

    @property
    def delay_hours(self) -> float:
        """Lateness relative to the normal upload time (0 for on-time or missing files)."""
        if self.upload_at is None:
            return 0.0
        return max((self.upload_at - self.scheduled_at).total_seconds() / 3600, 0.0)


def upload_plan(
    day: date,
    seed: int,
    upload_hour: float = 6.0,
    late_rate: float = 0.1,
    missing_rate: float = 0.05,
    late_max_hours: float = 8.0,
) -> UploadPlan:
    """When (or whether) pathology uploads the file for `day`. Deterministic per day."""
    rng = random.Random(f"{seed}:{day.isoformat()}:upload")
    draw = rng.random()
    scheduled = _at(day + timedelta(days=1), upload_hour)
    if draw < missing_rate:
        return UploadPlan(day, True, scheduled, None)
    if draw < missing_rate + late_rate:
        return UploadPlan(day, False, scheduled, scheduled + timedelta(hours=rng.uniform(2, late_max_hours)))
    return UploadPlan(day, False, scheduled, scheduled + timedelta(minutes=rng.uniform(0, 30)))


# --------------------------------------------------------------- file I/O


def render_csv(rows: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=LAB_FILE_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def write_lab_file(landing_dir: Path, day: date, rows: list[dict]) -> Path:
    """Write atomically: a sensor watching the folder never sees a half-written file."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    final = landing_dir / lab_file_name(day)
    tmp = landing_dir / f".{final.name}.tmp"  # dot-prefix: never matches labs_*.csv
    with open(tmp, "w", encoding="utf-8", newline="") as fh:
        fh.write(render_csv(rows))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, final)  # atomic rename on the same filesystem
    return final
