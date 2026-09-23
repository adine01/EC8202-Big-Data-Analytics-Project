"""Publish messages to Kafka and fail loudly unless every one was acknowledged."""

from __future__ import annotations

import json
from typing import Iterable


class PublishError(RuntimeError):
    pass


def publish(bootstrap_servers: str, messages: Iterable[tuple[str, str, dict]]) -> int:
    """messages: (topic, key, value). Returns the number acknowledged by the broker.

    Retrying the Airflow task after a partial failure re-sends everything; that
    is safe because every consumer is idempotent (lab_results upserts on
    (sample_id, test_type), dead_letter on (source, origin)).
    """
    from confluent_kafka import Producer  # imported lazily: only needed inside the task

    failures: list[str] = []
    acked = 0

    def on_delivery(err, _msg) -> None:
        nonlocal acked
        if err is not None:
            failures.append(str(err))
        else:
            acked += 1

    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "airflow-lab-ingest",
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 20,
    })
    for topic, key, value in messages:
        while True:
            try:
                producer.produce(topic, key=key.encode(), value=json.dumps(value).encode(), on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)

    undelivered = producer.flush(30)
    if failures or undelivered:
        raise PublishError(f"{len(failures)} failed, {undelivered} undelivered; first error: {failures[:1]}")
    return acked
