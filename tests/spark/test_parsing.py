"""Spark validation must give exactly the same verdicts as the Python contract."""

import json
import random
from datetime import datetime, timezone

from ward_common.schemas import validate_lab_row, validate_vitals_event
from ward_sim.faults import FaultInjector
from ward_sim.labs import LabDayGenerator, inject_bad_rows
from ward_sim.patients import build_roster
from ward_sim.vitals import VitalsGenerator
from ward_sim.vitals_producer import build_event
from ward_stream.parsing import parse_labs, parse_vitals, valid_vitals

ROSTER = build_roster(20, 42, datetime(2026, 1, 1).date())
KNOWN = {p.patient_id for p in ROSTER}
T0 = datetime(2026, 1, 3, 8, 0, tzinfo=timezone.utc)


def python_verdict(payload: bytes) -> set[str]:
    try:
        event = json.loads(payload)
    except json.JSONDecodeError:
        return {"invalid_json"}
    if not isinstance(event, dict):
        return {"not_an_object"}
    return set(validate_vitals_event(event, KNOWN))


def sample_payloads() -> list[bytes]:
    patient = ROSTER[0]
    event = build_event(patient.patient_id, VitalsGenerator(patient).reading(T0).values, T0, random.Random(1))
    injector = FaultInjector(random.Random(9))
    payloads = [json.dumps(event).encode()]
    payloads += [injector.corrupt(event)[0] for _ in range(300)]
    edge_cases = [
        {**event, "heart_rate": "78"},                           # number sent as a string
        {**event, "heart_rate": True},                           # boolean
        {**event, "spo2": None},                                 # explicit null
        {**event, "timestamp": "2026-01-03T08:00:00"},           # no timezone
        {**event, "temperature": 37},                            # integer is fine
        {**event, "patient_id": 17},                             # wrong type of id
        {**event, "systolic_bp": 80, "diastolic_bp": 80},        # equal -> inverted
    ]
    payloads += [json.dumps(e).encode() for e in edge_cases]
    payloads += [b"[1, 2, 3]", b'"just a string"', b"{not json", b""]
    return payloads


def test_vitals_validation_matches_python_contract(spark, kafka_rows):
    payloads = sample_payloads()
    known = spark.createDataFrame([(p,) for p in sorted(KNOWN)], "patient_id string")
    parsed = parse_vitals(kafka_rows(payloads), known).orderBy("offset").collect()
    assert len(parsed) == len(payloads)
    for payload, row in zip(payloads, parsed):
        assert set(row["error_reasons"]) == python_verdict(payload), payload


def test_valid_rows_are_typed(spark, kafka_rows):
    known = spark.createDataFrame([("P001",)], "patient_id string")
    payloads = sample_payloads()[:1]
    row = valid_vitals(parse_vitals(kafka_rows(payloads), known)).first()
    source = json.loads(payloads[0])
    assert row["patient_id"] == "P001"
    assert row["heart_rate"] == float(source["heart_rate"])
    assert row["event_time"] == datetime.fromisoformat(source["timestamp"].replace("Z", "")).replace(tzinfo=None)


def lab_event(row: dict, number: int) -> dict:
    event = dict(row)
    try:
        event["result_value"] = float(row["result_value"])
    except (TypeError, ValueError):
        pass  # keep the bad value as text, exactly as Airflow would pass it through
    event.update(schema_version=1, source_file="labs_2026-01-03.csv", file_day="2026-01-03", row_number=number)
    return event


def test_lab_validation_matches_python_contract(spark, kafka_rows):
    day = datetime(2026, 1, 3).date()
    rows, _ = inject_bad_rows(LabDayGenerator(ROSTER).rows_for_day(day), day, 42, 0.4)
    events = [lab_event(r, i) for i, r in enumerate(rows, 1)]
    known = spark.createDataFrame([(p,) for p in sorted(KNOWN)], "patient_id string")
    parsed = parse_labs(kafka_rows([json.dumps(e).encode() for e in events], "labs.raw"), known).collect()
    by_origin = {int(r["origin"].split(":")[-1]): r for r in parsed}
    for i, (source, event) in enumerate(zip(rows, events)):
        expected = set(validate_lab_row(source, KNOWN, day))
        assert set(by_origin[i]["error_reasons"]) == expected, source


def test_lab_reference_ranges_and_flags(spark, kafka_rows):
    base = {"sample_id": "s", "patient_id": "P001", "unit": "u", "collected_at": "2026-01-03T07:00:00Z",
            "schema_version": 1, "source_file": "f.csv", "file_day": "2026-01-03", "row_number": 1}
    events = [
        {**base, "test_type": "CRP", "result_value": 181.9, "reference_range": "<5"},
        {**base, "test_type": "WBC", "result_value": 3.1, "reference_range": "4.0-11.0"},
        {**base, "test_type": "POTASSIUM", "result_value": 4.2, "reference_range": "3.5-5.3"},
    ]
    known = spark.createDataFrame([("P001",)], "patient_id string")
    rows = parse_labs(kafka_rows([json.dumps(e).encode() for e in events], "labs.raw"), known).orderBy("origin").collect()
    assert [(r["ref_low"], r["ref_high"], r["abnormal_flag"]) for r in rows] == [
        (0.0, 5.0, "H"), (4.0, 11.0, "L"), (3.5, 5.3, "N"),
    ]
