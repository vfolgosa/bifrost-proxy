#!/usr/bin/env bash
# produce.sh — Produce N messages to a Kafka topic via kcat.
# Usage: ./produce.sh <broker> <topic> <count>
# Example: ./produce.sh localhost:9093 test-topic 100

set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Usage check
# ─────────────────────────────────────────────────────────────
if [ $# -ne 3 ]; then
    echo "USAGE: $0 <broker> <topic> <count>"
    echo "EXAMPLE: $0 localhost:9093 test-topic 100"
    exit 2
fi

BROKER="$1"
TOPIC="$2"
COUNT="$3"

# ─────────────────────────────────────────────────────────────
# Input validation
# ─────────────────────────────────────────────────────────────
if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [ "$COUNT" -lt 1 ]; then
    echo "FAIL: count must be a positive integer, got '$COUNT'"
    exit 2
fi

# ─────────────────────────────────────────────────────────────
# Produce
# ─────────────────────────────────────────────────────────────
echo "=== Producing $COUNT messages to $TOPIC on $BROKER ==="

for ((i=1; i<=COUNT; i++)); do
    echo "message-$i-$(date +%s%N)" | kcat \
        -b "$BROKER" \
        -t "$TOPIC" \
        -P \
        -X security.protocol=SASL_PLAINTEXT \
        -X sasl.mechanisms=PLAIN \
        -X sasl.username=admin \
        -X sasl.password=admin-secret \
        -X message.timeout.ms=5000
done

echo "PASS: produced $COUNT messages to $TOPIC"
