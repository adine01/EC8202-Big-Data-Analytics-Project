import pytest

from ward_common.schemas import validate_vitals_event


def good_event(**overrides):
    event = {
        "schema_version": 1,
        "event_id": "7f1c1f5e-1111-4222-8333-444455556666",
        "patient_id": "P001",
        "heart_rate": 78,
        "spo2": 97,
        "systolic_bp": 124,
        "diastolic_bp": 79,
        "temperature": 36.9,
        "timestamp": "2026-01-01T03:15:00.000Z",
        "produced_at": "2026-09-30T12:00:00.000Z",
    }
    event.update(overrides)
    return event


def test_valid_event_has_no_errors():
    assert validate_vitals_event(good_event(), known_patients={"P001"}) == []


def test_clinically_abnormal_but_plausible_values_are_valid():
    # SpO2 84 and HR 140 are alarming, not impossible: they must reach alerting.
    assert validate_vitals_event(good_event(spo2=84, heart_rate=140)) == []


@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({"heart_rate": 400}, "range:heart_rate"),
        ({"spo2": 120}, "range:spo2"),
        ({"temperature": "N/A"}, "type:temperature"),
        ({"heart_rate": True}, "type:heart_rate"),
        ({"systolic_bp": 70, "diastolic_bp": 90}, "bp_inverted"),
        ({"timestamp": "yesterday"}, "bad_timestamp:timestamp"),
        ({"timestamp": "2026-01-01T03:15:00"}, "bad_timestamp:timestamp"),  # no timezone
        ({"patient_id": "bed-7"}, "bad_patient_id"),
        ({"patient_id": None}, "missing:patient_id"),
    ],
)
def test_invalid_events_report_stable_error_codes(overrides, expected):
    assert expected in validate_vitals_event(good_event(**overrides))


def test_unknown_patient_detected_against_roster():
    assert validate_vitals_event(good_event(patient_id="P950"), known_patients={"P001"}) == [
        "unknown_patient"
    ]


def test_non_object_payload():
    assert validate_vitals_event(["not", "a", "dict"]) == ["not_an_object"]
