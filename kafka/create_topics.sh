#!/bin/bash
# One-shot topic provisioning (run by the `kafka-init` service).
# Topic design and justification: docs/architecture_decision.md, section 6.
# Broker auto-creation is disabled, so this is the only place topics are made.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
BIN=/opt/kafka/bin
SEVEN_DAYS_MS=604800000

echo "waiting for broker at ${BOOTSTRAP}"
for attempt in $(seq 1 60); do
    if "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
        break
    fi
    if [ "$attempt" -eq 60 ]; then
        echo "broker not reachable" >&2
        exit 1
    fi
    sleep 2
done

# create_topic <name> <partitions> <retention.ms>
create_topic() {
    local name="$1" partitions="$2" retention_ms="$3"
    "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$name" --partitions "$partitions" --replication-factor 1 \
        --config retention.ms="$retention_ms"
    # Re-apply retention so an existing topic converges to the configured value.
    "$BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --alter \
        --entity-type topics --entity-name "$name" \
        --add-config retention.ms="$retention_ms" >/dev/null
}

create_topic "${TOPIC_VITALS:-vitals.raw}"         "${VITALS_PARTITIONS:-6}" "$SEVEN_DAYS_MS"
create_topic "${TOPIC_LABS:-labs.raw}"             "${LABS_PARTITIONS:-3}"   -1   # keep forever: Kappa replay source
create_topic "${TOPIC_ALERTS:-alerts.patient}"     "${ALERTS_PARTITIONS:-3}" "$SEVEN_DAYS_MS"
create_topic "${TOPIC_DEADLETTER:-deadletter}"     1                         "$SEVEN_DAYS_MS"

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe
echo "topics ready"
