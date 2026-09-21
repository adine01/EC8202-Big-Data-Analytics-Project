"""The five streaming queries of the ward job.

    vitals_quality   vitals.raw  -> validate -> counts + invalid records to `deadletter`
    vitals_windows   vitals.raw  -> dedup -> 1 h tumbling windows -> EWS + lab join + trend
                                 -> vitals_window_1h, patient_live_status, alerts (+ alerts.patient)
    vitals_trends    vitals.raw  -> dedup -> 4 h/1 h sliding windows -> slopes -> vitals_trend_4h, alerts
    labs             labs.raw    -> validate -> lab_results (invalid to `deadletter`)
    deadletter_sink  deadletter  -> dead_letter table

Each query has its own checkpoint directory (Kafka offsets + window state), so
a restart resumes exactly where it stopped; every sink is an idempotent upsert,
so a micro-batch that is re-executed after a crash changes nothing.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from ward_common.config import KafkaSettings, PostgresSettings
from ward_common.scoring_rules import LAB_LOOKBACK_HOURS, TREND_ADJUSTMENT

from ward_stream import metrics
from ward_stream.alerts import ALERT_COLUMNS, trend_alerts, window_alerts
from ward_stream.parsing import deadletter_messages, parse_labs, parse_vitals, valid_vitals
from ward_stream.postgres import connect, existing_ids, upsert
from ward_stream.scoring import lab_adjustments, risk_tier, with_early_warning_score, with_trend
from ward_stream.windows import SHORT, sliding_trend, tumbling_1h

log = logging.getLogger("ward_stream.queries")

WINDOW_KEY = ["patient_id", "window_start"]
WINDOW_COLUMNS = [
    "patient_id", "window_start", "window_end", "n_readings",
    *[f"{s}_{agg}" for s in SHORT.values() for agg in ("avg", "min", "max")],
    "last_event_time", "ews_score", "ews_red_flag", "lab_adjustment", "lab_flags",
    "trend", "trend_adjustment", "total_score", "risk_tier",
]
LIVE_COLUMNS = [
    "patient_id", "window_start", "window_end", "last_event_time",
    "hr_avg", "spo2_avg", "sbp_avg", "dbp_avg", "temp_avg",
    "ews_score", "ews_red_flag", "lab_adjustment", "lab_flags", "trend", "trend_adjustment",
    "total_score", "risk_tier",
]
TREND_COLUMNS = [
    "patient_id", "window_start", "window_end", "n_readings",
    "hr_slope", "spo2_slope", "sbp_slope", "temp_slope",
    "deterioration_index", "improvement_index", "trend",
]
LAB_COLUMNS = [
    "sample_id", "test_type", "patient_id", "result_value", "unit", "reference_range",
    "ref_low", "ref_high", "abnormal_flag", "collected_at", "file_day", "source_file",
]
DEADLETTER_SCHEMA = StructType([
    StructField("source", StringType()),
    StructField("origin", StringType()),
    StructField("error_reasons", ArrayType(StringType())),
    StructField("raw_payload", StringType()),
    StructField("detected_by", StringType()),
    StructField("detected_at", StringType()),
])


@dataclass(frozen=True)
class JobConfig:
    kafka: KafkaSettings
    pg: PostgresSettings
    checkpoint_dir: str
    watermark: str
    trigger: str
    slow_trigger: str
    max_offsets_per_trigger: int
    trend_min_readings: int

    @classmethod
    def from_env(cls) -> JobConfig:
        return cls(
            kafka=KafkaSettings.from_env(),
            pg=PostgresSettings.from_env(),
            checkpoint_dir=os.environ.get("CHECKPOINT_DIR", "/opt/app/checkpoints"),
            # Simulated time: 30 sim-minutes = 6.25 real seconds at x288.
            watermark=os.environ.get("STREAM_WATERMARK", "30 minutes"),
            trigger=os.environ.get("STREAM_TRIGGER", "5 seconds"),
            slow_trigger=os.environ.get("STREAM_SLOW_TRIGGER", "15 seconds"),
            max_offsets_per_trigger=int(os.environ.get("STREAM_MAX_OFFSETS_PER_TRIGGER", "20000")),
            trend_min_readings=int(os.environ.get("TREND_MIN_READINGS", "20")),
        )


def _ts(value: datetime) -> str:
    """Spark hands back naive UTC datetimes; render them as explicit UTC SQL literals."""
    return value.replace(tzinfo=timezone.utc).isoformat()


def _json_default(value):
    if isinstance(value, datetime):
        return _ts(value)
    raise TypeError(type(value))


class StreamJob:
    def __init__(self, spark: SparkSession, cfg: JobConfig) -> None:
        self.spark = spark
        self.cfg = cfg
        pg = cfg.pg
        self.jdbc_options = {
            "url": f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.dbname}",
            "user": pg.user,
            "password": pg.password,
            "driver": "org.postgresql.Driver",
        }

    # --- sources / helpers ----------------------------------------------------------

    def _kafka_stream(self, topic: str) -> DataFrame:
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.cfg.kafka.bootstrap_servers)
            .option("subscribe", topic)
            # A new checkpoint replays the whole topic: that is the Kappa reprocessing path.
            .option("startingOffsets", "earliest")
            # Bound each micro-batch so a replay or backlog is processed in steady chunks.
            .option("maxOffsetsPerTrigger", self.cfg.max_offsets_per_trigger)
            # Retention may delete offsets we never read while stopped; log and continue.
            .option("failOnDataLoss", "false")
            .load()
        )

    def _jdbc(self, query: str) -> DataFrame:
        return self.spark.read.format("jdbc").options(**self.jdbc_options).option("query", query).load()

    def _known_patients(self) -> DataFrame:
        # Static side of a stream-static join; Spark re-reads it every micro-batch.
        return self._jdbc("SELECT patient_id FROM patients WHERE active")

    def _write_kafka(self, df: DataFrame, topic: str) -> None:
        (df.write.format("kafka")
         .option("kafka.bootstrap.servers", self.cfg.kafka.bootstrap_servers)
         .option("topic", topic)
         .save())

    def _deduplicated_vitals(self) -> DataFrame:
        parsed = parse_vitals(self._kafka_stream(self.cfg.kafka.topic_vitals), self._known_patients())
        return (
            valid_vitals(parsed)
            # Events older than (max event time seen - watermark) are dropped as too late.
            .withWatermark("event_time", self.cfg.watermark)
            # Duplicate deliveries share an event_id; state for an id is kept only while
            # it could still arrive within the watermark, so memory stays bounded.
            .dropDuplicatesWithinWatermark(["event_id"])
        )

    def _start(self, df: DataFrame, name: str, sink, trigger: str, mode: str = "update") -> StreamingQuery:
        def timed_sink(batch: DataFrame, batch_id: int) -> None:
            started = time.monotonic()
            sink(batch, batch_id)
            metrics.FOREACH_DURATION.labels(name).observe(time.monotonic() - started)

        return (
            df.writeStream.queryName(name)
            .outputMode(mode)
            .foreachBatch(timed_sink)
            .option("checkpointLocation", f"{self.cfg.checkpoint_dir}/{name}")
            .trigger(processingTime=trigger)
            .start()
        )

    def _raise_alerts(self, conn, alerts: list[dict]) -> None:
        """Publish *new* alerts to Kafka, then upsert all of them.

        Kafka first, commit second: if we crash in between, the replayed batch
        publishes the alert again with the same alert_id (at-least-once, with an
        idempotency key consumers can de-duplicate on) rather than losing it.
        """
        if not alerts:
            return
        known = existing_ids(conn, "alerts", "alert_id", [a["alert_id"] for a in alerts])
        new = [a for a in alerts if a["alert_id"] not in known]
        if new:
            messages = [(a["patient_id"], json.dumps(a, default=_json_default)) for a in new]
            self._write_kafka(self.spark.createDataFrame(messages, "key string, value string"),
                              self.cfg.kafka.topic_alerts)
            for a in new:
                metrics.ALERTS_RAISED.labels(a["alert_type"], a["severity"]).inc()
                log.info("alert_raised", extra={"patient_id": a["patient_id"], "alert_type": a["alert_type"],
                                                "severity": a["severity"], "detail": a["message"]})
        upsert(conn, "alerts", ALERT_COLUMNS, ["alert_id"], alerts, touch="last_seen_at")

    # --- query 1: validation + dead letters -----------------------------------------

    def start_vitals_quality(self) -> StreamingQuery:
        parsed = parse_vitals(self._kafka_stream(self.cfg.kafka.topic_vitals), self._known_patients())

        def sink(batch: DataFrame, _batch_id: int) -> None:
            batch = batch.persist()
            try:
                status = F.when(F.size("error_reasons") == 0, F.lit("valid")).otherwise(F.element_at("error_reasons", 1))
                for row in batch.groupBy(status.alias("status")).count().collect():
                    if row["status"] == "valid":
                        metrics.VITALS_RECORDS.labels("valid").inc(row["count"])
                    else:
                        metrics.VITALS_RECORDS.labels("invalid").inc(row["count"])
                        metrics.VITALS_INVALID.labels(row["status"]).inc(row["count"])
                rejected = batch.where(F.size("error_reasons") > 0)
                if not rejected.isEmpty():
                    self._write_kafka(deadletter_messages(rejected, "vitals"), self.cfg.kafka.topic_deadletter)
            finally:
                batch.unpersist()

        return self._start(parsed, "vitals_quality", sink, self.cfg.trigger, mode="append")

    # --- query 2: 1 h windows, score, stream/batch join, alerts ----------------------

    def _score_windows(self, batch: DataFrame) -> DataFrame:
        bounds = batch.agg(F.min("window_start").alias("lo"), F.max("window_end").alias("hi")).first()
        lo, hi = _ts(bounds["lo"]), _ts(bounds["hi"])

        # Batch side of the join, read fresh each micro-batch and limited to the time
        # range this batch needs (so a historical replay joins historically correct labs).
        labs = self._jdbc(
            "SELECT patient_id, test_type, result_value, collected_at FROM lab_results "
            f"WHERE collected_at > timestamptz '{lo}' - interval '{LAB_LOOKBACK_HOURS} hours' "
            f"AND collected_at <= timestamptz '{hi}'"
        )
        trends = self._jdbc(
            "SELECT patient_id, window_end AS trend_end, trend FROM vitals_trend_4h "
            f"WHERE window_end > timestamptz '{lo}' - interval '2 hours' AND window_end <= timestamptz '{hi}' "
            "AND trend <> 'insufficient_data'"
        )

        keys = batch.select("patient_id", "window_start", "window_end")
        lab_adj = lab_adjustments(keys, labs)
        w, t = keys.alias("w"), trends.alias("t")
        latest_trend = (
            w.join(t, (F.col("w.patient_id") == F.col("t.patient_id"))
                   & (F.col("t.trend_end") <= F.col("w.window_end"))
                   & (F.col("t.trend_end") > F.col("w.window_end") - F.expr("INTERVAL 2 HOURS")))
            .groupBy("w.patient_id", "w.window_start")
            .agg(F.max_by("t.trend", "t.trend_end").alias("trend"))
        )

        scored = with_early_warning_score(
            batch, {"heart_rate": "hr_avg", "spo2": "spo2_avg", "systolic_bp": "sbp_avg", "temperature": "temp_avg"}
        )
        scored = (
            scored.join(lab_adj, WINDOW_KEY, "left")
            .join(latest_trend, WINDOW_KEY, "left")
            .withColumn("lab_adjustment", F.coalesce("lab_adjustment", F.lit(0)))
            .withColumn("lab_flags", F.coalesce("lab_flags", F.array().cast("array<string>")))
            .withColumn("trend", F.coalesce("trend", F.lit("unknown")))
            .withColumn("trend_adjustment",
                        F.when(F.col("trend") == "deteriorating", F.lit(TREND_ADJUSTMENT)).otherwise(F.lit(0)))
            .withColumn("total_score", (F.col("ews_score") + F.col("lab_adjustment") + F.col("trend_adjustment")).cast("int"))
        )
        return scored.withColumn("risk_tier", risk_tier(F.col("total_score"), F.col("ews_red_flag")))

    def start_vitals_windows(self) -> StreamingQuery:
        windows = tumbling_1h(self._deduplicated_vitals())

        def sink(batch: DataFrame, _batch_id: int) -> None:
            if batch.isEmpty():
                return
            scored = self._score_windows(batch).persist()
            try:
                rows = [r.asDict() for r in scored.collect()]
                alerts = [r.asDict() for r in window_alerts(scored).collect()]
            finally:
                scored.unpersist()

            latest: dict[str, dict] = {}
            for r in rows:
                if r["patient_id"] not in latest or r["window_start"] > latest[r["patient_id"]]["window_start"]:
                    latest[r["patient_id"]] = r

            with connect(self.cfg.pg) as conn:
                upsert(conn, "vitals_window_1h", WINDOW_COLUMNS, WINDOW_KEY, rows)
                # Never let an older (late/replayed) window overwrite a newer live status.
                upsert(conn, "patient_live_status", LIVE_COLUMNS, ["patient_id"], latest.values(),
                       update_where="EXCLUDED.window_start >= patient_live_status.window_start")
                self._raise_alerts(conn, alerts)
                conn.commit()

            metrics.ROWS_UPSERTED.labels("vitals_window_1h").inc(len(rows))
            metrics.ROWS_UPSERTED.labels("patient_live_status").inc(len(latest))
            metrics.LAST_VITALS_PROCESSED.set(time.time())
            newest = max(r["last_produced_at"] for r in rows)
            metrics.E2E_LATENCY.set(time.time() - newest.replace(tzinfo=timezone.utc).timestamp())

        return self._start(windows, "vitals_windows", sink, self.cfg.trigger)

    # --- query 3: sliding-window trends --------------------------------------------

    def start_vitals_trends(self) -> StreamingQuery:
        trends = sliding_trend(self._deduplicated_vitals())
        slope_columns = {"heart_rate": "hr_slope", "spo2": "spo2_slope",
                         "systolic_bp": "sbp_slope", "temperature": "temp_slope"}

        def sink(batch: DataFrame, _batch_id: int) -> None:
            scored = with_trend(batch, slope_columns, self.cfg.trend_min_readings).persist()
            try:
                rows = [r.asDict() for r in scored.collect()]
                alerts = [r.asDict() for r in trend_alerts(scored).collect()]
            finally:
                scored.unpersist()
            if not rows:
                return
            with connect(self.cfg.pg) as conn:
                upsert(conn, "vitals_trend_4h", TREND_COLUMNS, WINDOW_KEY, rows)
                self._raise_alerts(conn, alerts)
                conn.commit()
            metrics.ROWS_UPSERTED.labels("vitals_trend_4h").inc(len(rows))

        # APPEND mode: each 4 h window is emitted once, complete, after the watermark passes
        # its end. In update mode an open window was re-scored on every micro-batch while it
        # filled, so its noisier partial slope got ~10 chances to cross the threshold, and
        # stable patients drew ~10x the calibrated ~1 % false trend alerts. The cost is
        # latency: a trend lands ~4.5 simulated hours (~56 real s) after its window opens.
        return self._start(trends, "vitals_trends", sink, self.cfg.trigger, mode="append")

    # --- query 4: labs --------------------------------------------------------------

    def start_labs(self) -> StreamingQuery:
        parsed = parse_labs(self._kafka_stream(self.cfg.kafka.topic_labs), self._known_patients())

        def sink(batch: DataFrame, _batch_id: int) -> None:
            batch = batch.persist()
            try:
                valid = [r.asDict() for r in batch.where(F.size("error_reasons") == 0).select(*LAB_COLUMNS).collect()]
                rejected = batch.where(F.size("error_reasons") > 0)
                n_rejected = rejected.count()
                if n_rejected:
                    self._write_kafka(deadletter_messages(rejected, "labs"), self.cfg.kafka.topic_deadletter)
            finally:
                batch.unpersist()
            if valid:
                with connect(self.cfg.pg) as conn:
                    upsert(conn, "lab_results", LAB_COLUMNS, ["sample_id", "test_type"], valid, touch=None)
                    conn.commit()
                metrics.ROWS_UPSERTED.labels("lab_results").inc(len(valid))
            metrics.LAB_RECORDS.labels("valid").inc(len(valid))
            metrics.LAB_RECORDS.labels("invalid").inc(n_rejected)

        return self._start(parsed, "labs", sink, self.cfg.slow_trigger, mode="append")

    # --- query 5: dead-letter topic -> table ----------------------------------------

    def start_deadletter_sink(self) -> StreamingQuery:
        messages = (
            self._kafka_stream(self.cfg.kafka.topic_deadletter)
            .select(F.from_json(F.col("value").cast("string"), DEADLETTER_SCHEMA).alias("d"))
            .select("d.*")
            .where(F.col("source").isNotNull() & F.col("origin").isNotNull())
            .withColumn("detected_at", F.coalesce(F.try_to_timestamp("detected_at"), F.current_timestamp()))
            .withColumn("error_reasons", F.coalesce("error_reasons", F.array().cast("array<string>")))
            .withColumn("detected_by", F.coalesce("detected_by", F.lit("unknown")))
        )
        columns = ["source", "origin", "error_reasons", "raw_payload", "detected_by", "detected_at"]

        def sink(batch: DataFrame, _batch_id: int) -> None:
            rows = [r.asDict() for r in batch.collect()]
            if not rows:
                return
            with connect(self.cfg.pg) as conn:
                # Same record re-sent (at-least-once Kafka write) -> same (source, origin) -> ignored.
                upsert(conn, "dead_letter", columns, ["source", "origin"], rows, do_nothing=True)
                conn.commit()
            for r in rows:
                metrics.DEADLETTERS_STORED.labels(r["source"]).inc()

        return self._start(messages, "deadletter_sink", sink, self.cfg.slow_trigger, mode="append")

    def start_all(self) -> list[StreamingQuery]:
        return [
            self.start_vitals_quality(),
            self.start_vitals_windows(),
            self.start_vitals_trends(),
            self.start_labs(),
            self.start_deadletter_sink(),
        ]
