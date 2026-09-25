import csv
import io
from collections import Counter
from datetime import date, datetime, timedelta, timezone

import pytest

from ward_common.schemas import LAB_TESTS, parse_reference_range, validate_lab_row
from ward_sim.lab_generator import LabSimulator, Metrics, parse_args
from ward_sim.labs import (
    LAB_FAULT_KINDS,
    LabDayGenerator,
    inject_bad_rows,
    render_csv,
    upload_plan,
    write_lab_file,
)
from ward_sim.patients import EpisodeSchedule, build_roster

UTC = timezone.utc
START = date(2026, 1, 1)
ROSTER = build_roster(20, 42, START)
IDS = {p.patient_id for p in ROSTER}


def rows_by(rows, patient_id, test):
    return [float(r["result_value"]) for r in rows if r["patient_id"] == patient_id and r["test_type"] == test]


# --- contract -------------------------------------------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [("4.0-11.0", (4.0, 11.0)), ("59-104", (59.0, 104.0)), ("<5", (0.0, 5.0)), (" < 14 ", (0.0, 14.0)),
     ("11-4", None), ("normal", None), ("4.0 to 11.0", None), (None, None)],
)
def test_parse_reference_range(text, expected):
    assert parse_reference_range(text) == expected


# --- clean data -------------------------------------------------------------------


def test_same_day_regenerates_identical_file():
    first = render_csv(LabDayGenerator(ROSTER).rows_for_day(date(2026, 1, 3)))
    second = render_csv(LabDayGenerator(build_roster(20, 42, START)).rows_for_day(date(2026, 1, 3)))
    assert first == second


def test_clean_rows_pass_validation():
    day = date(2026, 1, 3)
    rows = LabDayGenerator(ROSTER).rows_for_day(day)
    assert len(rows) >= 5 * 15  # most patients get the routine panel
    for row in rows:
        assert validate_lab_row(row, IDS, day) == [], row


def test_all_seven_test_types_appear_over_a_week():
    gen = LabDayGenerator(ROSTER)
    seen = {r["test_type"] for d in range(7) for r in gen.rows_for_day(START + timedelta(days=d))}
    assert seen == set(LAB_TESTS)


def test_stable_patients_are_mostly_within_reference():
    gen = LabDayGenerator(ROSTER)
    total = normal = 0
    for d in range(10):
        for row in gen.rows_for_day(START + timedelta(days=d)):
            if next(p for p in ROSTER if p.patient_id == row["patient_id"]).scenario != "stable":
                continue
            low, high = parse_reference_range(row["reference_range"])
            total += 1
            normal += low <= float(row["result_value"]) <= high
    assert normal / total > 0.9


def _peak_day(patient):
    ep = EpisodeSchedule(patient).episodes_until(datetime(2026, 1, 20, tzinfo=UTC))[0]
    return (ep.recovery_start + timedelta(hours=12)).date(), (ep.onset - timedelta(days=1)).date()


@pytest.mark.parametrize(
    "scenario,test,direction",
    [("sepsis", "CRP", +1), ("sepsis", "WBC", +1), ("haemorrhage", "HAEMOGLOBIN", -1),
     ("respiratory_failure", "CRP", +1)],
)
def test_labs_follow_the_hidden_deterioration(scenario, test, direction):
    gen = LabDayGenerator(ROSTER)
    patient = next(p for p in ROSTER if p.scenario == scenario)
    peak_day, before_day = _peak_day(patient)
    before = rows_by(gen.rows_for_day(max(before_day, START)), patient.patient_id, test)
    during = rows_by(gen.rows_for_day(peak_day), patient.patient_id, test)
    assert before and during
    mean = lambda xs: sum(xs) / len(xs)  # noqa: E731
    assert (mean(during) - mean(before)) * direction > 0


# --- bad rows -------------------------------------------------------------------


def test_injected_faults_are_caught_by_validation_or_duplicate_check():
    day = date(2026, 1, 3)
    clean = LabDayGenerator(ROSTER).rows_for_day(day)
    rows, faults = inject_bad_rows(clean, day, seed=42, rate=0.5)
    kinds = Counter(faults)
    assert set(kinds) == set(LAB_FAULT_KINDS)

    invalid = sum(1 for r in rows if validate_lab_row(r, IDS, day))
    keys = Counter((r["sample_id"], r["test_type"], r["patient_id"], r["result_value"]) for r in rows)
    duplicates = sum(c - 1 for c in keys.values() if c > 1)
    assert invalid == len(faults) - kinds["duplicate_row"]
    assert duplicates == kinds["duplicate_row"]


def test_bad_row_rate_zero_leaves_file_clean():
    day = date(2026, 1, 3)
    clean = LabDayGenerator(ROSTER).rows_for_day(day)
    assert inject_bad_rows(clean, day, 42, 0.0) == (clean, [])


# --- upload plan ------------------------------------------------------------------


def test_upload_plan_rates_and_delays():
    plans = [upload_plan(START + timedelta(days=i), 42, late_rate=0.1, missing_rate=0.05) for i in range(4000)]
    missing = sum(p.missing for p in plans) / len(plans)
    late = sum((not p.missing) and p.delay_hours >= 2 for p in plans) / len(plans)
    assert missing == pytest.approx(0.05, abs=0.015)
    assert late == pytest.approx(0.10, abs=0.02)
    on_time = [p for p in plans if not p.missing and p.delay_hours < 2]
    assert all(6 * 60 <= (p.upload_at.hour * 60 + p.upload_at.minute) <= 6 * 60 + 30 for p in on_time)


def test_upload_plan_is_deterministic():
    assert upload_plan(date(2026, 2, 1), 42) == upload_plan(date(2026, 2, 1), 42)


# --- files and service loop -------------------------------------------------------


def test_write_is_atomic_and_leaves_no_temp_file(tmp_path):
    rows = LabDayGenerator(ROSTER).rows_for_day(date(2026, 1, 2))
    path = write_lab_file(tmp_path, date(2026, 1, 2), rows)
    assert path.name == "labs_2026-01-02.csv"
    assert [p.name for p in tmp_path.iterdir()] == [path.name]
    parsed = list(csv.DictReader(io.StringIO(path.read_text())))
    assert parsed == rows


def make_sim(tmp_path, monkeypatch, **overrides):
    monkeypatch.setenv("SIM_START_DATE", "2026-01-01")
    argv = ["--data-dir", str(tmp_path), "--missing-rate", "0", "--late-rate", "0"]
    for key, value in overrides.items():
        argv += [f"--{key.replace('_', '-')}", str(value)]
    return LabSimulator(parse_args(argv), Metrics(enabled=False, port=0))


def test_file_appears_only_after_upload_time(tmp_path, monkeypatch):
    sim = make_sim(tmp_path, monkeypatch)
    sim.tick(datetime(2026, 1, 3, 5, 0, tzinfo=UTC))   # before 06:00 on D+1
    assert not (tmp_path / "landing" / "labs_2026-01-02.csv").exists()
    sim.tick(datetime(2026, 1, 3, 7, 0, tzinfo=UTC))
    assert (tmp_path / "landing" / "labs_2026-01-02.csv").exists()


def test_archived_file_is_not_redelivered(tmp_path, monkeypatch):
    sim = make_sim(tmp_path, monkeypatch, max_backfill_days=0)
    (tmp_path / "archive").mkdir()
    (tmp_path / "archive" / "labs_2026-01-02.csv").write_text("already loaded")
    sim.tick(datetime(2026, 1, 3, 12, 0, tzinfo=UTC))
    assert not (tmp_path / "landing").exists()


def test_missing_day_is_withheld_not_written(tmp_path, monkeypatch):
    sim = make_sim(tmp_path, monkeypatch, missing_rate=1.0, max_backfill_days=0)
    sim.tick(datetime(2026, 1, 3, 12, 0, tzinfo=UTC))
    assert date(2026, 1, 2) in sim.withheld
    assert not (tmp_path / "landing").exists()


def test_backfill_is_bounded(tmp_path, monkeypatch):
    sim = make_sim(tmp_path, monkeypatch, max_backfill_days=2)
    sim.tick(datetime(2026, 1, 20, 12, 0, tzinfo=UTC))
    names = sorted(p.name for p in (tmp_path / "landing").iterdir())
    assert names == ["labs_2026-01-17.csv", "labs_2026-01-18.csv", "labs_2026-01-19.csv"]
