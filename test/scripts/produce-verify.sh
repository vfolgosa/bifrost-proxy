#!/usr/bin/env bash
# produce-verify.sh — Produce to cluster A (kafka1), then consume and verify count.
# Usage: ./produce-verify.sh
#
# Cluster A = kafka1 @ localhost:9093 (SASL_PLAINTEXT, admin/admin-secret)
# This script produces 10 messages, then consumes from the same topic with a
# fresh consumer group to count them, and reports PASS/FAIL.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ─────────────────────────────────────────────────────────────
# Cluster A configuration
# ─────────────────────────────────────────────────────────────
CLUSTER_A_BROKER="localhost:9093"
TOPIC="test-topic"
PRODUCE_COUNT=10

# SASL credentials (matching docker-compose kafka1 config)
SASL_FLAGS=(
    -X security.protocol=SASL_PLAINTEXT
    -X sasl.mechanisms=PLAIN
    -X sasl.username=admin
    -X sasl.password=admin-secret
)

# ─────────────────────────────────────────────────────────────
# Phase 1: Produce
# ─────────────────────────────────────────────────────────────
echo "=============================================="
echo " Phase 1: Produce $PRODUCE_COUNT messages"
echo " Cluster: $CLUSTER_A_BROKER"
echo " Topic:   $TOPIC"
echo "=============================================="

for ((i=1; i<=PRODUCE_COUNT; i++)); do
    TIMESTAMP="$(date +%s%N)"
    echo "verify-msg-$i-$TIMESTAMP" | kcat \
        -b "$CLUSTER_A_BROKER" \
        -t "$TOPIC" \
        -P \
        "${SASL_FLAGS[@]}" \
        -X message.timeout.ms=5000
done

echo ""
echo "PASS: produced $PRODUCE_COUNT messages to $TOPIC on cluster A"

# ─────────────────────────────────────────────────────────────
# Phase 2: Consume with a fresh group and count
# ─────────────────────────────────────────────────────────────
VERIFY_GROUP="verify-group-$(date +%s)"

echo ""
echo "=============================================="
echo " Phase 2: Consume (simple consumer, group=$VERIFY_GROUP)"
echo "=============================================="

CONSUMED=$(kcat \
    -b "$CLUSTER_A_BROKER" \
    -t "$TOPIC" \
    -C \
    "${SASL_FLAGS[@]}" \
    -o beginning \
    -e \
    -q 2>&1 | wc -l | tr -d ' ')

echo ""
echo "=============================================="
echo " Result"
echo "=============================================="
echo " Produced: $PRODUCE_COUNT"
echo " Consumed: $CONSUMED"

if [ "$CONSUMED" -ge "$PRODUCE_COUNT" ]; then
    echo ""
    echo "PASS: consumed $CONSUMED messages >= produced $PRODUCE_COUNT"
else
    echo ""
    echo "FAIL: consumed $CONSUMED messages < produced $PRODUCE_COUNT"
    exit 1
fi
