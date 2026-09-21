"""Entry point of the ward streaming job (spark-submit target).

One Spark application in local mode runs all streaming queries (see
ward_stream/queries.py). If any query fails, the whole application exits
non-zero and Docker restarts it; every query then resumes from its checkpoint.
"""

from __future__ import annotations

import logging
import os
import signal
import sys

from pyspark.sql import SparkSession

from ward_common.log import configure_logging

from ward_stream import metrics
from ward_stream.queries import JobConfig, StreamJob

configure_logging("spark-stream")
logging.getLogger("py4j").setLevel(logging.WARNING)
log = logging.getLogger("ward_stream.main")


def build_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ward-stream")
        # All timestamps are UTC end to end (events carry 'Z').
        .config("spark.sql.session.timeZone", "UTC")
        # RocksDB keeps window/dedup state off the JVM heap. There is one
        # instance per stateful operator per partition (24 here), each with
        # its own default write buffers, so native memory must be capped
        # explicitly to fit the 2 GB container.
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true")
        .config("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", "128")
        .getOrCreate()
    )


def main() -> int:
    cfg = JobConfig.from_env()
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    metrics.start_server(int(os.environ.get("SPARK_METRICS_PORT", "8003")))
    spark.streams.addListener(metrics.MetricsListener())

    queries = StreamJob(spark, cfg).start_all()
    log.info("stream_job_started", extra={"queries": [q.name for q in queries],
                                          "watermark": cfg.watermark, "trigger": cfg.trigger})

    def shutdown(*_: object) -> None:
        log.info("stream_job_stopping")
        for q in queries:
            q.stop()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        spark.streams.awaitAnyTermination()
    except Exception:
        log.exception("stream_query_failed")
        return 1
    finally:
        spark.stop()
    failed = [q.name for q in queries if q.exception() is not None]
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
