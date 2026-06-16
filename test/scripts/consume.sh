#!/usr/bin/env bash
# consume.sh — Consume messages from a Kafka topic via kcat.
# Usage: ./consume.sh <broker> <topic> <group>
# Example: ./consume.sh localhost:9093 test-topic test-group
#
# Note: <group> is accepted for interface compatibility but kcat uses
# simple consumer mode (-C) since group consumer (-G) is unavailable
# with this kcat/Kafka version over SASL_PLAINTEXT.

set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Usage check
# ─────────────────────────────────────────────────────────────
if [ $# -ne 3 ]; then
    echo "USAGE: $0 <broker> <topic> <group>"
    echo "EXAMPLE: $0 localhost:9093 test-topic test-group"
    exit 2
fi

BROKER="$1"
TOPIC="$2"
GROUP="$3"

# ─────────────────────────────────────────────────────────────
# Consume all messages from beginning, exit on EOF
# ─────────────────────────────────────────────────────────────
echo "=== Consuming from $TOPIC on $BROKER (group: $GROUP) ==="

MSG_COUNT=$(kcat \
    -b "$BROKER" \
    -t "$TOPIC" \
    -C \
    -X security.protocol=SASL_PLAINTEXT \
    -X sasl.mechanisms=PLAIN \
    -X sasl.username=admin \
    -X sasl.password=admin-secret \
    -o beginning \
    -e \
    -q 2>&1 | wc -l | tr -d ' ')

echo "PASS: consumed $MSG_COUNT message(s) from $TOPIC"
