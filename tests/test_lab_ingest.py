import json
from datetime import date, datetime, timedelta, timezone

from ward_common.schemas import LAB_EVENT_FIELDS
from ward_ingest.lab_files import (
    check_file,
    classify_arrival,
    deadletter_event,
    decide,
    expected_upload,
    file_day_from_name,
    lab_event,
)
from ward_sim.labs import LabDayGenerator, inject_bad_rows, render_csv, write_lab_file
from ward_sim.patients import build_roster

DAY = date(2026, 1, 4)
ROSTER = build_roster(20, 42, date(2026, 1, 1))
KNOWN = {p.patient_id for p in ROSTER}
UTC = timezone.utc


def simulated_file(tmp_path, rate=0.03, day=DAY):
    rows, faults = inject_bad_rows(LabDayGenerator(ROSTER).rows_for_day(day), day, 42, rate)
    return write_lab_file(tmp_path, day, rows), rows, faults


def test_every_injected_fault_is_rejected_and_nothing_else(tmp_path):
    path, rows, faults = simulated_file(tmp_path, rate=0.3)
    check = check_file(path, KNOWN)
    assert check.header_ok and check.file_day == DAY
    assert check.total == len(rows)
    assert len(check.rejected) == len(faults)       # duplicates included, as duplicate_row
    assert check.reason_counts.get("duplicate_row", 0) == faults.count("duplicate_row") > 0


def test_clean_file_publishes_and_checksum_is_stable(tmp_path):
    path, _, _ = simulated_file(tmp_path, rate=0.0)
    first = check_file(path, KNOWN)
    assert first.rejected == [] and decide(first, 0.2) == "publish"
    assert check_file(path, KNOWN).checksum == first.checksum


def test_mostly_bad_file_is_quarantined(tmp_path):
    path, _, _ = simulated_file(tmp_path, rate=0.6)
    check = check_file(path, KNOWN)
    assert check.reject_rate > 0.2
    assert decide(check, 0.2) == "quarantine"


def test_wrong_header_is_quarantined_without_row_checks(tmp_path):
    path = tmp_path / "labs_2026-01-04.csv"
    path.write_text("patient,test,value\nP001,CRP,3\n")
    check = check_file(path, KNOWN)
    assert not check.header_ok and check.total == 0
    assert decide(check, 0.2) == "quarantine"


def test_extra_cells_are_rejected(tmp_path):
    rows = LabDayGenerator(ROSTER).rows_for_day(DAY)[:3]
    text = render_csv(rows).splitlines()
    text[2] += ",surprise"
    path = tmp_path / "labs_2026-01-04.csv"
    path.write_text("\n".join(text) + "\n")
    check = check_file(path, KNOWN)
    assert [r.reasons for r in check.rejected] == [["wrong_column_count"]]


def test_unknown_patient_uses_roster(tmp_path):
    rows = LabDayGenerator(ROSTER).rows_for_day(DAY)[:2]
    rows[0] = {**rows[0], "patient_id": "P950"}
    path = write_lab_file(tmp_path, DAY, rows)
    assert check_file(path, KNOWN).rejected[0].reasons == ["unknown_patient"]


def test_messages_follow_the_contracts(tmp_path):
    path, _, _ = simulated_file(tmp_path, rate=0.3)
    check = check_file(path, KNOWN)
    event = lab_event(check.valid[0], check)
    assert set(event) == set(LAB_EVENT_FIELDS)
    assert isinstance(event["result_value"], float)
    assert event["file_day"] == "2026-01-04" and event["source_file"] == "labs_2026-01-04.csv"

    dl = deadletter_event(check.rejected[0], check, datetime(2026, 9, 30, tzinfo=UTC))
    assert dl["origin"] == f"labs_2026-01-04.csv:{check.rejected[0].row_number}"
    assert dl["source"] == "labs" and dl["detected_by"] == "airflow"
    assert json.loads(dl["raw_payload"])  # the original row, kept for inspection


def test_file_name_parsing():
    assert file_day_from_name("labs_2026-01-04.csv") == DAY
    assert file_day_from_name("labs_2026-02-30.csv") is None
    assert file_day_from_name(".labs_2026-01-04.csv.tmp") is None
    assert file_day_from_name("results.csv") is None


def test_arrival_classification():
    expected = expected_upload(DAY, 6)
    assert expected == datetime(2026, 1, 5, 6, 0, tzinfo=UTC)
    assert classify_arrival(expected + timedelta(minutes=25), expected) == "on_time"
    assert classify_arrival(expected + timedelta(hours=3), expected) == "late"
