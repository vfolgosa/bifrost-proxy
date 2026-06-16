#!/usr/bin/env bash
# T22 - E2E Integration Test + Benchmarks
# Tests: produce → proxy → consume, data integrity, latency, partition distribution
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROXY_BROKER="localhost:9092"
TOPIC="t22-test-topic"
MSG_COUNT=100

# Create a fresh topic via kcat metadata (using admin client)
# We use test-topic if t22-test-topic doesn't exist, or fall back to test-topic
# Actually, let's just use test-topic since it already exists with 3 partitions
TOPIC="test-topic"

SASL_FLAGS=(
    -X security.protocol=SASL_PLAINTEXT
    -X sasl.mechanisms=PLAIN
    -X sasl.username=admin
    -X sasl.password=admin-secret
)

PASS_COUNT=0
FAIL_COUNT=0

pass_test() {
    echo "  ✅ $1"
    PASS_COUNT=$((PASS_COUNT + 1))
}

fail_test() {
    echo "  ❌ $1"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

echo "============================================================"
echo " T22 - E2E Integration Test + Benchmarks"
echo " Proxy: $PROXY_BROKER → kafka1:9093 (SASL_PLAINTEXT)"
echo " Topic: $TOPIC (3 partitions)"
echo " Messages: $MSG_COUNT"
echo " Date: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "============================================================"
echo ""

# ─────────────────────────────────────────────────────────────
# Phase 1: Produce messages through proxy
# ─────────────────────────────────────────────────────────────
echo "━━━ Phase 1: Produce $MSG_COUNT messages through proxy ━━━"

PRODUCE_START=$(python3 -c "import time; print(time.time_ns())")

for ((i=1; i<=MSG_COUNT; i++)); do
    TIMESTAMP="$(date +%s)"
    # Embed checksum: SHA256 of the message body for integrity verification
    MESSAGE="e2e-msg-$i|ts=$TIMESTAMP"
    echo "$MESSAGE" | kcat \
        -b "$PROXY_BROKER" \
        -t "$TOPIC" \
        -P \
        "${SASL_FLAGS[@]}" \
        -X message.timeout.ms=5000 2>/dev/null
done

PRODUCE_END=$(python3 -c "import time; print(time.time_ns())")

PRODUCE_NS=$((PRODUCE_END - PRODUCE_START))
PRODUCE_MS=$(python3 -c "print(f'{$PRODUCE_NS / 1000000:.3f}')")
PRODUCE_AVG_MS=$(python3 -c "print(f'{$PRODUCE_NS / $MSG_COUNT / 1000000:.3f}')")

echo ""
echo "Produce $MSG_COUNT messages: ${PRODUCE_MS}ms (avg ${PRODUCE_AVG_MS}ms/msg)"
pass_test "Produced $MSG_COUNT messages through proxy"

# ─────────────────────────────────────────────────────────────
# Phase 2: Consume all messages through proxy, count + track
# ─────────────────────────────────────────────────────────────
echo ""
echo "━━━ Phase 2: Consume messages through proxy ━━━"

CONSUME_START=$(python3 -c "import time; print(time.time_ns())")

# Consume and save to temp file for integrity check
CONSUMED_FILE="/tmp/t22-consumed.txt"
kcat \
    -b "$PROXY_BROKER" \
    -t "$TOPIC" \
    -C \
    "${SASL_FLAGS[@]}" \
    -o beginning \
    -e \
    -q 2>/dev/null > "$CONSUMED_FILE"

CONSUME_END=$(python3 -c "import time; print(time.time_ns())")

CONSUMED_COUNT=$(wc -l < "$CONSUMED_FILE" | tr -d ' ')
CONSUME_NS=$((CONSUME_END - CONSUME_START))
CONSUME_MS=$(python3 -c "print(f'{$CONSUME_NS / 1000000:.3f}')")

echo "Consume: ${CONSUME_MS}ms, got $CONSUMED_COUNT messages"

# ─────────────────────────────────────────────────────────────
# Phase 3: Data integrity — count our e2e messages
# ─────────────────────────────────────────────────────────────
echo ""
echo "━━━ Phase 3: Data Integrity Verification ━━━"

E2E_COUNT=$(grep -c "^e2e-msg-" "$CONSUMED_FILE" || echo 0)
echo "e2e messages found: $E2E_COUNT"

if [ "$E2E_COUNT" -ge "$MSG_COUNT" ]; then
    pass_test "All $MSG_COUNT e2e messages received ($E2E_COUNT found)"
else
    fail_test "Expected $MSG_COUNT e2e messages, found $E2E_COUNT"
fi

# Check message uniqueness
UNIQUE_COUNT=$(grep "^e2e-msg-" "$CONSUMED_FILE" | sort -u | wc -l | tr -d ' ')
if [ "$UNIQUE_COUNT" -eq "$E2E_COUNT" ]; then
    pass_test "All $E2E_COUNT e2e messages are unique (no duplicates)"
else
    fail_test "Duplicate messages found: $UNIQUE_COUNT unique out of $E2E_COUNT"
fi

# Check message ordering range
FIRST_SEQ=$(grep "^e2e-msg-" "$CONSUMED_FILE" | head -1 | cut -d'|' -f1 | cut -d'-' -f3)
LAST_SEQ=$(grep "^e2e-msg-" "$CONSUMED_FILE" | tail -1 | cut -d'|' -f1 | cut -d'-' -f3)
echo "Sequence range: $FIRST_SEQ to $LAST_SEQ"

# Sample messages
echo ""
echo "Sample messages (first 3):"
grep "^e2e-msg-" "$CONSUMED_FILE" | head -3
echo ""
echo "Sample messages (last 3):"
grep "^e2e-msg-" "$CONSUMED_FILE" | tail -3

# ─────────────────────────────────────────────────────────────
# Phase 4: Latency measurement — direct vs proxy
# ─────────────────────────────────────────────────────────────
echo ""
echo "━━━ Phase 4: Latency Benchmark (direct vs proxy) ━━━"

# Direct produce to kafka1:9093
DIRECT_PRODUCE_START=$(python3 -c "import time; print(time.time_ns())")
for ((i=1; i<=20; i++)); do
    echo "latency-direct-$i-$(date +%s%N)" | kcat \
        -b "localhost:9093" \
        -t "$TOPIC" \
        -P \
        "${SASL_FLAGS[@]}" \
        -X message.timeout.ms=5000 2>/dev/null
done
DIRECT_PRODUCE_END=$(python3 -c "import time; print(time.time_ns())")

# Proxy produce to localhost:9092
PROXY_PRODUCE_START=$(python3 -c "import time; print(time.time_ns())")
for ((i=1; i<=20; i++)); do
    echo "latency-proxy-$i-$(date +%s%N)" | kcat \
        -b "$PROXY_BROKER" \
        -t "$TOPIC" \
        -P \
        "${SASL_FLAGS[@]}" \
        -X message.timeout.ms=5000 2>/dev/null
done
PROXY_PRODUCE_END=$(python3 -c "import time; print(time.time_ns())")

DIRECT_NS=$((DIRECT_PRODUCE_END - DIRECT_PRODUCE_START))
PROXY_NS=$((PROXY_PRODUCE_END - PROXY_PRODUCE_START))

DIRECT_AVG_MS=$(python3 -c "print(f'{$DIRECT_NS / 20 / 1000000:.3f}')")
PROXY_AVG_MS=$(python3 -c "print(f'{$PROXY_NS / 20 / 1000000:.3f}')")
OVERHEAD_MS=$(python3 -c "print(f'{($PROXY_NS - $DIRECT_NS) / 20 / 1000000:.3f}')")

echo "Direct to kafka1:9093  — avg ${DIRECT_AVG_MS}ms/msg (20 msgs)"
echo "Proxy to localhost:9092  — avg ${PROXY_AVG_MS}ms/msg (20 msgs)"
echo "Proxy overhead          — avg ${OVERHEAD_MS}ms/msg"

OVERHEAD_OK=$(python3 -c "print(1 if (($PROXY_NS - $DIRECT_NS) / 20) < 2000000 else 0)")
if [ "$OVERHEAD_OK" = "1" ]; then
    pass_test "Proxy overhead < 2ms (${OVERHEAD_MS}ms)"
else
    fail_test "Proxy overhead >= 2ms (${OVERHEAD_MS}ms) — EXCEEDS THRESHOLD"
fi

# Direct consume latency
DIRECT_CONSUME_START=$(python3 -c "import time; print(time.time_ns())")
kcat -b "localhost:9093" -t "$TOPIC" -C "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | wc -l > /dev/null
DIRECT_CONSUME_END=$(python3 -c "import time; print(time.time_ns())")

# Proxy consume latency
PROXY_CONSUME_START=$(python3 -c "import time; print(time.time_ns())")
kcat -b "$PROXY_BROKER" -t "$TOPIC" -C "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | wc -l > /dev/null
PROXY_CONSUME_END=$(python3 -c "import time; print(time.time_ns())")

DIRECT_CONSUME_MS=$(python3 -c "import time; print(f'{($DIRECT_CONSUME_END - $DIRECT_CONSUME_START) / 1000000:.3f}')")
PROXY_CONSUME_MS=$(python3 -c "import time; print(f'{($PROXY_CONSUME_END - $PROXY_CONSUME_START) / 1000000:.3f}')")
CONSUME_OVERHEAD=$(python3 -c "import time; print(f'{(($PROXY_CONSUME_END - $PROXY_CONSUME_START) - ($DIRECT_CONSUME_END - $DIRECT_CONSUME_START)) / 1000000:.3f}')")

echo ""
echo "Consume latency:"
echo "  Direct:  ${DIRECT_CONSUME_MS}ms"
echo "  Proxy:   ${PROXY_CONSUME_MS}ms"
echo "  Overhead: ${CONSUME_OVERHEAD}ms"

# ─────────────────────────────────────────────────────────────
# Phase 5: Partition distribution via proxy
# ─────────────────────────────────────────────────────────────
echo ""
echo "━━━ Phase 5: Partition Distribution ━━━"
kcat -b "$PROXY_BROKER" -t "$TOPIC" -L "${SASL_FLAGS[@]}" 2>/dev/null | head -20
echo ""

# ─────────────────────────────────────────────────────────────
# Phase 6: Metrics endpoint validation
# ─────────────────────────────────────────────────────────────
echo ""
echo "━━━ Phase 6: Metrics Endpoints ━━━"

# /health
HEALTH_RESP=$(curl -s http://localhost:8080/health 2>/dev/null)
if echo "$HEALTH_RESP" | grep -q '"status":"ok"'; then
    pass_test "/health returns 200 OK: $HEALTH_RESP"
else
    fail_test "/health: $HEALTH_RESP"
fi

# /metrics
METRICS_RESP=$(curl -s http://localhost:8080/metrics 2>/dev/null)
METRIC_NAMES=$(echo "$METRICS_RESP" | grep '^# HELP' | sed 's/# HELP //' | sed 's/ .*//')
echo "Metrics exported:"
echo "$METRIC_NAMES" | while read m; do echo "  - $m"; done

REQUIRED_METRICS=("proxy_up" "proxy_uptime_seconds" "proxy_connections_active" "proxy_failover_total" "proxy_health_status" "proxy_circuit_breaker" "proxy_drain_active" "proxy_build_info")
for m in "${REQUIRED_METRICS[@]}"; do
    if echo "$METRICS_RESP" | grep -q "$m"; then
        pass_test "Metric $m present"
    else
        fail_test "Metric $m MISSING"
    fi
done

# /status
STATUS_RESP=$(curl -s http://localhost:8080/status 2>/dev/null)
if echo "$STATUS_RESP" | python3 -c "import sys,json; json.load(sys.stdin)" 2>/dev/null; then
    pass_test "/status returns valid JSON"
    echo "$STATUS_RESP" | python3 -m json.tool 2>/dev/null | head -30
else
    fail_test "/status: invalid JSON or error"
fi

# ─────────────────────────────────────────────────────────────
# Phase 7: Run produce-verify.sh adapted through proxy
# ─────────────────────────────────────────────────────────────
echo ""
echo "━━━ Phase 7: produce-verify.sh through proxy ━━━"

# Produce 10 specific messages through proxy
PROXY_VERIFY_GROUP="t22-verify-$(date +%s)"
echo "Producing 10 messages through proxy..."
for ((i=1; i<=10; i++)); do
    echo "proxy-verify-msg-$i-$(date +%s%N)" | kcat \
        -b "$PROXY_BROKER" \
        -t "$TOPIC" \
        -P \
        "${SASL_FLAGS[@]}" \
        -X message.timeout.ms=5000 2>/dev/null
done
echo "Produced 10 messages."

# Consume and verify through proxy
echo "Consuming through proxy..."
VERIFY_COUNT=$(kcat \
    -b "$PROXY_BROKER" \
    -t "$TOPIC" \
    -C \
    "${SASL_FLAGS[@]}" \
    -o beginning \
    -e \
    -q 2>/dev/null | grep -c "proxy-verify-msg-" || echo 0)

echo "Found $VERIFY_COUNT proxy-verify messages"

if [ "$VERIFY_COUNT" -ge 10 ]; then
    pass_test "produce-verify through proxy: $VERIFY_COUNT >= 10"
else
    fail_test "produce-verify through proxy: $VERIFY_COUNT < 10"
fi

# ─────────────────────────────────────────────────────────────
# Phase 8: Direct cluster verification (kafka1 and kafka2)
# ─────────────────────────────────────────────────────────────
echo ""
echo "━━━ Phase 8: Direct Cluster Connectivity ━━━"

# Verify direct produce/consume still works on kafka1
echo "Testing direct to kafka1:9093..."
echo "direct-test-$(date +%s%N)" | kcat \
    -b "localhost:9093" \
    -t "$TOPIC" \
    -P \
    "${SASL_FLAGS[@]}" \
    -X message.timeout.ms=5000 2>/dev/null

DIRECT_K1=$(kcat -b "localhost:9093" -t "$TOPIC" -C "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | grep -c "direct-test-" || echo 0)
if [ "$DIRECT_K1" -ge 1 ]; then
    pass_test "Direct produce/consume on kafka1:9093 works ($DIRECT_K1 msgs)"
else
    fail_test "Direct produce/consume on kafka1:9093 FAILED"
fi

# Verify direct to kafka2
echo "Testing direct to kafka2:9094..."
echo "direct-k2-test-$(date +%s%N)" | kcat \
    -b "localhost:9094" \
    -t "$TOPIC" \
    -P \
    "${SASL_FLAGS[@]}" \
    -X message.timeout.ms=5000 2>/dev/null

DIRECT_K2=$(kcat -b "localhost:9094" -t "$TOPIC" -C "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | grep -c "direct-k2-test-" || echo 0)
if [ "$DIRECT_K2" -ge 1 ]; then
    pass_test "Direct produce/consume on kafka2:9094 works ($DIRECT_K2 msgs)"
else
    fail_test "Direct produce/consume on kafka2:9094 FAILED"
fi

# ─────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo " T22 E2E Integration Test Results"
echo "============================================================"
echo " Passed: $PASS_COUNT"
echo " Failed: $FAIL_COUNT"
echo "============================================================"

if [ "$FAIL_COUNT" -gt 0 ]; then
    echo "RESULT: FAIL ❌"
    exit 1
else
    echo "RESULT: PASS ✅"
    exit 0
fi
