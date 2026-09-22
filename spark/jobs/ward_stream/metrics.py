"""Prometheus metrics for the stream job, exposed from the Spark driver.

Most numbers come from Spark's own StreamingQueryProgress events. Kafka lag
is computed from progress too (latestOffset - endOffset per partition):
Spark stores its offsets in the checkpoint and does not commit them to a
Kafka consumer group, so broker-side consumer-group lag would show nothing.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyspark.sql.streaming import StreamingQueryListener

log = logging.getLogger("ward_stream.metrics")

QUERY = ["query"]

INPUT_ROWS = Counter("spark_query_input_rows_total", "Rows read by a streaming query", QUERY)
BATCH_DURATION = Gauge("spark_query_batch_duration_seconds", "Last micro-batch trigger execution time", QUERY)
INPUT_RATE = Gauge("spark_query_input_rows_per_second", "Input rate of the last micro-batch", QUERY)
PROCESS_RATE = Gauge("spark_query_processed_rows_per_second", "Processing rate of the last micro-batch", QUERY)
LAG = Gauge("spark_query_kafka_lag_records", "Records available in Kafka but not yet processed", QUERY)
WATERMARK_DROPS = Counter("spark_query_rows_dropped_by_watermark_total", "Late rows dropped by the watermark", QUERY)
STATE_ROWS = Gauge("spark_query_state_rows", "Rows held in streaming state", QUERY)
LAST_PROGRESS = Gauge("spark_query_last_progress_timestamp_seconds", "Real time of the last progress event", QUERY)
WATERMARK = Gauge("spark_query_watermark_sim_seconds", "Current event-time watermark (simulated epoch s)", QUERY)
QUERY_UP = Gauge("spark_query_active", "1 while the streaming query is running", QUERY)

VITALS_RECORDS = Counter("spark_vitals_records_total", "Vitals messages validated", ["status"])
VITALS_INVALID = Counter("spark_vitals_invalid_total", "Invalid vitals messages by first error", ["reason"])
LAB_RECORDS = Counter("spark_lab_records_total", "Lab messages validated", ["status"])
ROWS_UPSERTED = Counter("spark_rows_upserted_total", "Rows written to Postgres", ["table"])
ALERTS_RAISED = Counter("spark_alerts_raised_total", "New alerts raised", ["alert_type", "severity"])
DEADLETTERS_STORED = Counter("spark_deadletters_stored_total", "Dead-letter messages copied to Postgres", ["source"])
LAST_VITALS_PROCESSED = Gauge("spark_vitals_last_processed_timestamp_seconds",
                              "Real time a micro-batch last contained vitals")
E2E_LATENCY = Gauge("spark_vitals_end_to_end_latency_seconds",
                    "Real seconds from the newest reading being produced to it being written to Postgres")
FOREACH_DURATION = Histogram("spark_foreach_batch_seconds", "foreachBatch sink duration", QUERY,
                             buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30))


def start_server(port: int) -> None:
    start_http_server(port)


def _lag(source) -> int:
    try:
        latest = json.loads(source.latestOffset or "{}")
        end = json.loads(source.endOffset or "{}")
    except (TypeError, ValueError):
        return 0
    total = 0
    for topic, partitions in latest.items():
        for partition, offset in partitions.items():
            total += max(int(offset) - int(end.get(topic, {}).get(partition, offset)), 0)
    return total


class MetricsListener(StreamingQueryListener):
    def __init__(self) -> None:
        self._names: dict[str, str] = {}

    def onQueryStarted(self, event) -> None:
        self._names[str(event.id)] = event.name or "unnamed"
        QUERY_UP.labels(event.name or "unnamed").set(1)
        log.info("query_started", extra={"query": event.name, "id": str(event.id)})

    def onQueryProgress(self, event) -> None:
        p = event.progress
        name = p.name or "unnamed"
        INPUT_ROWS.labels(name).inc(p.numInputRows)
        INPUT_RATE.labels(name).set(p.inputRowsPerSecond or 0)
        PROCESS_RATE.labels(name).set(p.processedRowsPerSecond or 0)
        BATCH_DURATION.labels(name).set((p.durationMs or {}).get("triggerExecution", 0) / 1000)
        LAG.labels(name).set(sum(_lag(s) for s in p.sources))
        dropped = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        if dropped:
            WATERMARK_DROPS.labels(name).inc(dropped)
        STATE_ROWS.labels(name).set(sum(op.numRowsTotal for op in p.stateOperators))
        LAST_PROGRESS.labels(name).set(time.time())
        watermark = (p.eventTime or {}).get("watermark")
        if watermark:
            try:
                WATERMARK.labels(name).set(datetime.fromisoformat(watermark.replace("Z", "+00:00")).timestamp())
            except ValueError:
                pass

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        name = self._names.get(str(event.id), str(event.id))
        QUERY_UP.labels(name).set(0)
        log.warning("query_terminated", extra={"query": name, "exception": event.exception})
