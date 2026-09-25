"""DAG integrity tests. They need Airflow installed, so they run inside the Airflow image:

    make airflow-test
"""

import os

import pytest

airflow = pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = os.environ.get("DAGS_DIR", "/opt/airflow/dags")


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_and_tasks(dagbag):
    assert set(dagbag.dag_ids) == {"lab_ingest", "daily_risk_report"}
    assert set(dagbag.get_dag("lab_ingest").task_ids) == {"resolve_target_day", "wait_for_lab_file", "load_landing_files"}
    assert set(dagbag.get_dag("daily_risk_report").task_ids) == {"resolve_report_day", "wait_for_lab_results",
                                                                  "build_risk_report"}


def test_scheduling_is_safe(dagbag):
    for dag in dagbag.dags.values():
        assert dag.catchup is False, dag.dag_id           # never backfill real-time history
        assert dag.max_active_runs == 1, dag.dag_id       # runs of the same day never overlap
        assert "ward" in dag.tags


def test_every_task_records_its_outcome(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.on_failure_callback, f"{dag.dag_id}.{task.task_id}"
            assert task.on_success_callback, f"{dag.dag_id}.{task.task_id}"


def test_loader_runs_even_when_the_sensor_fails(dagbag):
    load = dagbag.get_dag("lab_ingest").get_task("load_landing_files")
    assert load.trigger_rule == "all_done"
    sensor = dagbag.get_dag("lab_ingest").get_task("wait_for_lab_file")
    assert sensor.mode == "reschedule" and sensor.retries == 0


def test_report_is_triggered_by_the_lab_asset(dagbag):
    report = dagbag.get_dag("daily_risk_report")
    assert report.timetable.summary.lower().startswith("asset") or "labs.raw" in str(report.timetable)
