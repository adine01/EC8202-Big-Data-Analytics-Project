"""Prometheus metrics served by the API at /metrics.

Two kinds:
* HTTP metrics (request count / latency per route) - the serving stage.
* A custom collector that runs a few cheap SQL queries *at scrape time* and
  exposes storage and batch state: lab file ledger, Airflow task outcomes,
  table sizes, live risk tiers, data freshness. Airflow tasks are too
  short-lived to be scraped, so this is how batch processing becomes
  observable without a Pushgateway.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Callable

from prometheus_client import CollectorRegistry, Counter, Histogram, PlatformCollector, ProcessCollector
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

log = logging.getLogger("app.metrics")

UPLOAD_HOUR = float(os.environ.get("LAB_UPLOAD_HOUR", "6"))
SLA_HOURS = float(os.environ.get("LAB_SLA_HOURS", "4"))


def lab_file_overdue(sim_now: datetime, ledger: dict[date, str]) -> tuple[int, float]:
    """(overdue flag, simulated hours past the scheduled upload) for yesterday's file.

    Overdue = the SLA deadline (upload hour + SLA on D+1) has passed and the
    file has not been received (not loaded, not even quarantined).
    """
    yesterday = sim_now.date() - timedelta(days=1)
    scheduled = datetime.combine(sim_now.date(), time(0), tzinfo=timezone.utc) + timedelta(hours=UPLOAD_HOUR)
    received = ledger.get(yesterday) in ("loaded", "quarantined")
    hours_late = 0.0 if received else max((sim_now - scheduled).total_seconds() / 3600, 0.0)
    overdue = int(not received and hours_late > SLA_HOURS)
    return overdue, round(hours_late, 2)


class PipelineCollector:
    """Reads pipeline state from Postgres on every scrape."""

    def __init__(self, repository, sim_now: Callable[[], datetime]) -> None:
        self.repository = repository
        self.sim_now = sim_now

    def collect(self):
        up = GaugeMetricFamily("ward_db_up", "1 if the API could read pipeline state from Postgres")
        try:
            state = self.repository.pipeline_state()
            now = self.sim_now()
        except Exception:
            log.warning("pipeline_state_unavailable", exc_info=True)
            up.add_metric([], 0)
            yield up
            return
        up.add_metric([], 1)
        yield up

        sim_time = GaugeMetricFamily("ward_sim_time_seconds", "Simulated clock (epoch seconds)")
        sim_time.add_metric([], now.timestamp())
        yield sim_time

        # --- batch: lab files -------------------------------------------------------
        files = GaugeMetricFamily("ward_lab_files", "Simulated days in the lab ledger by status", labels=["status"])
        for status in ("loaded", "quarantined", "missing"):
            files.add_metric([status], next((r["n"] for r in state["lab_files"] if r["status"] == status), 0))
        yield files

        last = state["lab_last_loaded"]
        last_loaded = GaugeMetricFamily("ward_lab_last_loaded_timestamp_seconds",
                                        "Real time the most recent lab file was loaded")
        last_loaded.add_metric([], float(last["loaded_epoch"]) if last else 0)
        yield last_loaded

        ledger = {r["file_day"]: r["status"] for r in state["lab_ledger"]}
        overdue, hours_late = lab_file_overdue(now, ledger)
        g = GaugeMetricFamily("ward_lab_file_overdue",
                              "1 if yesterday's lab file was not received by the simulated SLA deadline")
        g.add_metric([], overdue)
        yield g
        g = GaugeMetricFamily("ward_lab_file_hours_late",
                              "Simulated hours past the scheduled upload with yesterday's file still missing")
        g.add_metric([], hours_late)
        yield g

        rows = GaugeMetricFamily("ward_lab_rows", "Lab rows across all loaded files", labels=["result"])
        rows.add_metric(["valid"], float(state["lab_rows"]["valid"]))
        rows.add_metric(["rejected"], float(state["lab_rows"]["rejected"]))
        yield rows

        # --- batch: Airflow task outcomes ----------------------------------------------
        runs = CounterMetricFamily("ward_pipeline_task_runs", "Airflow task outcomes recorded by callbacks",
                                   labels=["dag_id", "task_id", "state"])
        for r in state["task_runs"]:
            runs.add_metric([r["dag_id"], r["task_id"], r["state"]], r["n"])
        yield runs

        success = GaugeMetricFamily("ward_pipeline_last_success_timestamp_seconds",
                                    "Real time a task of the DAG last succeeded", labels=["dag_id"])
        for r in state["task_last_success"]:
            success.add_metric([r["dag_id"]], float(r["epoch"]))
        yield success

        duration = GaugeMetricFamily("ward_pipeline_task_duration_seconds",
                                     "Duration of the most recent run of each task", labels=["dag_id", "task_id"])
        for r in state["task_last_duration"]:
            duration.add_metric([r["dag_id"], r["task_id"]], float(r["duration_s"]))
        yield duration

        # --- storage ----------------------------------------------------------------------
        tables = GaugeMetricFamily("ward_table_rows", "Approximate live rows per table", labels=["table"])
        for r in state["table_rows"]:
            tables.add_metric([r["table_name"]], float(r["n"]))
        yield tables

        size = GaugeMetricFamily("ward_db_size_bytes", "Size of the ward database")
        size.add_metric([], float(state["db_size"]["bytes"]))
        yield size

        # --- serving: ward state ----------------------------------------------------------
        tiers = GaugeMetricFamily("ward_patients_by_tier", "Patients per live risk tier", labels=["tier"])
        for tier in ("LOW", "MEDIUM", "HIGH"):
            tiers.add_metric([tier], next((r["n"] for r in state["tiers"] if r["risk_tier"] == tier), 0))
        yield tiers

        lag = GaugeMetricFamily("ward_vitals_data_lag_sim_seconds",
                                "Simulated seconds between now and the newest processed reading")
        newest = (state["live"] or {}).get("last_event_time")
        lag.add_metric([], (now - newest).total_seconds() if newest else float("nan"))
        yield lag


class ApiMetrics:
    def __init__(self, repository, sim_now: Callable[[], datetime]) -> None:
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        self.requests = Counter("ward_api_requests_total", "HTTP requests", ["method", "route", "status"],
                                registry=self.registry)
        self.latency = Histogram("ward_api_request_duration_seconds", "HTTP request latency", ["route"],
                                 buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5), registry=self.registry)
        self.registry.register(PipelineCollector(repository, sim_now))
