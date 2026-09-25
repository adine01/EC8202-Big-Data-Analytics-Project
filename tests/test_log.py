import json
import logging

from ward_common.log import configure_logging


def test_log_lines_are_json_with_extra_fields(capsys):
    configure_logging("unit-test", level="INFO", logger_name="ward.test")
    logging.getLogger("ward.test").info("event_produced", extra={"patient_id": "P001", "count": 3})

    line = capsys.readouterr().out.strip()
    record = json.loads(line)
    assert record["event"] == "event_produced"
    assert record["service"] == "unit-test"
    assert record["level"] == "INFO"
    assert record["patient_id"] == "P001"
    assert record["count"] == 3
    assert record["ts"].endswith("+00:00")


def test_exceptions_are_serialised(capsys):
    configure_logging("unit-test", level="INFO", logger_name="ward.test.exc")
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("ward.test.exc").exception("failed")

    record = json.loads(capsys.readouterr().out.strip())
    assert "ValueError: boom" in record["exc_info"]
