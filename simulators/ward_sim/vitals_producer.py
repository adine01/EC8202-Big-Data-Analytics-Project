"""Bedside-monitor simulator: publishes vital signs to Kafka.

    python -m ward_sim.vitals_producer --patients 20 --rate 20
    python -m ward_sim.vitals_producer --dry-run --duration 10     # print, no Kafka/DB

Each patient reports every (patients / rate) real seconds; with the defaults
that is once per real second, i.e. every ~4.8 simulated minutes at x288.
Messages are keyed by patient_id so all readings of one patient land in the
same partition, in order.
"""

from __future__ import annotations

import argparse
import heapq
import itertools
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Protocol

from ward_common.config import ClockSettings, KafkaSettings, PostgresSettings
from ward_common.log import configure_logging
from ward_common.schemas import VITALS_SCHEMA_VERSION
from ward_common.sim_clock import ClockReader, SimClock, utcnow

from ward_sim.faults import FaultInjector
from ward_sim.patients import build_roster, describe_patient
from ward_sim.vitals import VitalsGenerator

log = logging.getLogger("ward_sim.vitals_producer")


# --------------------------------------------------------------------- CLI


def _env_default(name: str, default, cast=float):
    raw = os.environ.get(name)
    return cast(raw) if raw not in (None, "") else default


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--patients", type=int, default=_env_default("SIM_PATIENTS", 20, int),
                   help="number of simulated patients (default 20)")
    p.add_argument("--rate", type=float, default=_env_default("SIM_RATE", 20.0),
                   help="total readings per real second across the ward (default 20)")
    p.add_argument("--seed", type=int, default=_env_default("SIM_SEED", 42, int),
                   help="random seed; same seed = same patients and storylines")
    p.add_argument("--deteriorating-fraction", type=float,
                   default=_env_default("SIM_DETERIORATING_FRACTION", 0.5))
    p.add_argument("--spike-rate", type=float, default=_env_default("SIM_SPIKE_RATE", 0.01),
                   help="probability per reading that a transient spike starts")
    p.add_argument("--malformed-rate", type=float, default=_env_default("SIM_MALFORMED_RATE", 0.02))
    p.add_argument("--late-rate", type=float, default=_env_default("SIM_LATE_RATE", 0.02))
    p.add_argument("--duplicate-rate", type=float, default=_env_default("SIM_DUPLICATE_RATE", 0.01))
    p.add_argument("--late-min-sim-minutes", type=float, default=_env_default("SIM_LATE_MIN_MINUTES", 5.0))
    p.add_argument("--late-max-sim-minutes", type=float, default=_env_default("SIM_LATE_MAX_MINUTES", 120.0))
    p.add_argument("--duration", type=float, default=0.0,
                   help="stop after this many real seconds (0 = run forever)")
    p.add_argument("--metrics-port", type=int, default=_env_default("SIM_METRICS_PORT", 8001, int))
    p.add_argument("--dry-run", action="store_true",
                   help="print events to stdout; no Kafka, no database, local clock")
    args = p.parse_args(argv)
    if args.patients < 1 or args.rate <= 0:
        p.error("--patients must be >= 1 and --rate must be > 0")
    return args


# ------------------------------------------------------------------- sinks


class Sink(Protocol):
    def send(self, key: str, value: bytes) -> None: ...
    def poll(self) -> None: ...
    def flush(self) -> int: ...


class StdoutSink:
    def send(self, key: str, value: bytes) -> None:
        print(f"{key}\t{value.decode(errors='replace')}", flush=True)

    def poll(self) -> None:
        pass

    def flush(self) -> int:
        return 0


class KafkaSink:
    def __init__(self, settings: KafkaSettings, metrics: "Metrics") -> None:
        from confluent_kafka import Producer

        self.topic = settings.topic_vitals
        self.metrics = metrics
        self.producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "client.id": "vitals-simulator",
                # Durability + no duplicates from producer retries.
                "acks": "all",
                "enable.idempotence": True,
                # Small batching window: better throughput, still sub-second latency.
                "linger.ms": 20,
                "compression.type": "lz4",
            }
        )

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.metrics.delivery.labels(result="error").inc()
            log.error("delivery_failed", extra={"error": str(err), "key": msg.key()})
            return
        self.metrics.delivery.labels(result="ok").inc()
        latency = msg.latency()
        if latency is not None:
            self.metrics.delivery_latency.observe(latency)

    def send(self, key: str, value: bytes) -> None:
        while True:
            try:
                self.producer.produce(self.topic, key=key.encode(), value=value, on_delivery=self._on_delivery)
                return
            except BufferError:
                # Local queue full (broker slow/unavailable): wait for deliveries, then retry.
                self.producer.poll(0.5)

    def poll(self) -> None:
        self.producer.poll(0)

    def flush(self) -> int:
        return self.producer.flush(10)


# ----------------------------------------------------------------- metrics


class Metrics:
    def __init__(self, enabled: bool, port: int) -> None:
        from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

        # Own registry (not the global one) so several instances can coexist in tests.
        r = self.registry = CollectorRegistry()
        self.sent = Counter("vitals_events_sent_total", "Events handed to the producer", ["kind"], registry=r)
        self.malformed = Counter(
            "vitals_malformed_sent_total", "Malformed events by damage type", ["reason"], registry=r
        )
        self.delivery = Counter("vitals_delivery_total", "Broker acknowledgements", ["result"], registry=r)
        self.delivery_latency = Histogram(
            "vitals_delivery_latency_seconds", "Produce-to-ack latency",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5), registry=r,
        )
        self.late_buffer = Gauge("vitals_late_buffer_size", "Readings held back to be delivered late", registry=r)
        self.patients = Gauge("vitals_patients_simulated", "Patients in the simulated ward", registry=r)
        self.severity = Gauge(
            "vitals_patient_true_severity", "Hidden ground-truth deterioration severity (0-1+)",
            ["patient_id"], registry=r,
        )
        self.sim_time = Gauge(
            "vitals_sim_time_seconds", "Simulated clock as seen by the producer (epoch s)", registry=r
        )
        if enabled:
            start_http_server(port, registry=r)


# -------------------------------------------------------------------- core


def build_event(patient_id: str, values: dict, sim_time: datetime, rng: random.Random) -> dict:
    return {
        "schema_version": VITALS_SCHEMA_VERSION,
        # Deterministic under a fixed seed; lets Spark drop duplicates by id.
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "patient_id": patient_id,
        **values,
        "timestamp": sim_time.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "produced_at": utcnow().isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }


def encode(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode()


class Simulator:
    def __init__(self, args: argparse.Namespace, clock_source, sink: Sink, metrics: Metrics) -> None:
        self.args = args
        self.clock_source = clock_source  # callable -> SimClock
        self.sink = sink
        self.metrics = metrics
        settings = ClockSettings.from_env()
        self.roster = build_roster(args.patients, args.seed, settings.sim_start_date, args.deteriorating_fraction)
        self.generators = [VitalsGenerator(p, args.spike_rate) for p in self.roster]
        self.rng = random.Random(f"{args.seed}:events")
        self.faults = FaultInjector(
            random.Random(f"{args.seed}:faults"),
            malformed_rate=args.malformed_rate,
            late_rate=args.late_rate,
            duplicate_rate=args.duplicate_rate,
            late_min=timedelta(minutes=args.late_min_sim_minutes),
            late_max=timedelta(minutes=args.late_max_sim_minutes),
        )
        self.interval = args.patients / args.rate  # real seconds between readings of one patient
        self.running = True
        self._late: list[tuple[float, int, str, bytes]] = []
        self._seq = itertools.count()
        self._in_episode = [False] * len(self.roster)
        metrics.patients.set(len(self.roster))

    def stop(self, *_: object) -> None:
        self.running = False

    def _emit(self, key: str, payload: bytes, kind: str) -> None:
        self.sink.send(key, payload)
        self.metrics.sent.labels(kind=kind).inc()

    def _release_late(self, now: float) -> None:
        while self._late and self._late[0][0] <= now:
            _, _, key, payload = heapq.heappop(self._late)
            self._emit(key, payload, "late")
        self.metrics.late_buffer.set(len(self._late))

    def _tick(self, index: int, clock: SimClock) -> None:
        patient = self.roster[index]
        sim_now = clock.now()
        reading = self.generators[index].reading(sim_now)
        self.metrics.severity.labels(patient_id=patient.patient_id).set(reading.severity)

        in_episode = reading.severity > 0
        if in_episode != self._in_episode[index]:
            self._in_episode[index] = in_episode
            log.info(
                "deterioration_started" if in_episode else "deterioration_resolved",
                extra={**describe_patient(patient), "sim_time": sim_now.isoformat()},
            )

        fate = self.faults.choose()
        key = patient.patient_id

        if fate == FaultInjector.LATE:
            # The reading is taken now but delivered later (e.g. monitor lost Wi-Fi),
            # so it arrives with an old event timestamp.
            delay = self.faults.late_delay()
            event = build_event(key, reading.values, sim_now, self.rng)
            release_at = time.monotonic() + clock.real_seconds(delay)
            heapq.heappush(self._late, (release_at, next(self._seq), key, encode(event)))
            return

        event = build_event(key, reading.values, sim_now, self.rng)
        if fate == FaultInjector.MALFORMED:
            payload, reason = self.faults.corrupt(event)
            self._emit(key, payload, "malformed")
            self.metrics.malformed.labels(reason=reason).inc()
        elif fate == FaultInjector.DUPLICATE:
            payload = encode(event)
            self._emit(key, payload, "valid")
            self._emit(key, payload, "duplicate")
        else:
            self._emit(key, encode(event), "valid")

    def run(self) -> None:
        start = time.monotonic()
        deadline = start + self.args.duration if self.args.duration > 0 else float("inf")
        # Stagger patients evenly so the load is smooth rather than bursty.
        due = [(start + i * self.interval / len(self.roster), i) for i in range(len(self.roster))]
        heapq.heapify(due)
        last_stats = start

        log.info(
            "simulator_started",
            extra={
                "patients": len(self.roster),
                "rate_per_second": self.args.rate,
                "seconds_between_readings_per_patient": self.interval,
                "scenarios": {p.patient_id: p.scenario for p in self.roster},
            },
        )

        while self.running and time.monotonic() < deadline:
            next_due, index = due[0]
            next_late = self._late[0][0] if self._late else float("inf")
            wait = min(next_due, next_late, deadline) - time.monotonic()
            if wait > 0:
                time.sleep(min(wait, 0.5))
                self.sink.poll()
                continue

            now = time.monotonic()
            self._release_late(now)
            if next_due <= now:
                heapq.heapreplace(due, (next_due + self.interval, index))
                clock = self.clock_source()
                self.metrics.sim_time.set(clock.now().timestamp())
                self._tick(index, clock)
            self.sink.poll()

            if now - last_stats >= 30:
                last_stats = now
                log.info("simulator_heartbeat", extra={"late_buffered": len(self._late)})

        undelivered = self.sink.flush()
        log.info(
            "simulator_stopped",
            extra={"undelivered": undelivered, "late_events_discarded": len(self._late)},
        )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging("vitals-simulator")
    # In dry-run the events themselves go to stdout; only show warnings alongside them.
    if args.dry_run:
        logging.getLogger().setLevel(logging.WARNING)

    metrics = Metrics(enabled=not args.dry_run, port=args.metrics_port)
    settings = ClockSettings.from_env()

    if args.dry_run:
        local_clock = SimClock.starting(settings.sim_start_date, settings.sim_day_seconds, utcnow())
        clock_source = lambda: local_clock  # noqa: E731
        sink: Sink = StdoutSink()
        simulator = Simulator(args, clock_source, sink, metrics)
    else:
        import psycopg

        from ward_sim.roster_db import sync_roster

        pg = PostgresSettings.from_env()
        connect = lambda: psycopg.connect(**pg.connect_kwargs(), connect_timeout=5)  # noqa: E731
        reader = ClockReader(connect, settings.refresh_seconds)
        reader.wait_until_ready()
        sink = KafkaSink(KafkaSettings.from_env(), metrics)
        simulator = Simulator(args, reader.clock, sink, metrics)
        with connect() as conn:
            sync_roster(conn, simulator.roster)
        log.info("roster_synced", extra={"patients": len(simulator.roster)})

    signal.signal(signal.SIGTERM, simulator.stop)
    signal.signal(signal.SIGINT, simulator.stop)
    simulator.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
