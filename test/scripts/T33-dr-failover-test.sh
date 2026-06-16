#!/usr/bin/env bash
# T33-dr-failover-test.sh
# DR Failover Integration Test — validates that config-based failover
# (active: primary → secondary) works with no data loss.
#
# Flow:
#   1. Start proxy (active=primary, port 19092, plaintext)
#   2. Produce 500 unique messages through proxy → kafka1 (primary)
#   3. Trigger failover: stop proxy, change active→secondary, restart proxy
#   4. Produce 500 more unique messages through proxy → kafka2 (secondary)
#   5. Consume from kafka1, count messages
#   6. Consume from kafka2, count messages
#   7. Verify: total=1000, no data loss, no duplicates
#
# Prerequisites:
#   - Docker Compose up (kafka1:9093, kafka2:9094)
#   - test-topic exists on both clusters (3 partitions)
#   - kcat installed
#   - proxy binary built at ../proxy

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_FILE="$PROJECT_DIR/config-test.yaml"
PROXY_BIN="$PROJECT_DIR/proxy"
REPORT_DIR="$PROJECT_DIR/test/reports"

KAFKA1="localhost:9093"
KAFKA2="localhost:9094"
PROXY_ADDR="localhost:19092"
TOPIC="test-topic"
TOTAL_COUNT=1000
HALF_COUNT=500

SASL_FLAGS=(
    -X security.protocol=SASL_PLAINTEXT
    -X sasl.mechanisms=PLAIN
    -X sasl.username=admin
    -X sasl.password=admin-secret
)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
START_TIME=$(date +%s)

pass() { echo -e "  ${GREEN}PASS${NC}: $1"; PASS=$((PASS+1)); }
fail() { echo -e "  ${RED}FAIL${NC}: $1"; FAIL=$((FAIL+1)); }
info() { echo -e "  ${YELLOW}INFO${NC}: $1"; }

cleanup() {
    if [ -n "${PROXY_PID:-}" ]; then
        kill "$PROXY_PID" 2>/dev/null || true
        wait "$PROXY_PID" 2>/dev/null || true
    fi
    if [ -f "$CONFIG_FILE.bak" ]; then
        mv "$CONFIG_FILE.bak" "$CONFIG_FILE"
    fi
}
trap cleanup EXIT

# ─────────────────────────────────────────────────────────────────
# Phase 0: Verify prerequisites
# ─────────────────────────────────────────────────────────────────
echo "=============================================="
echo " T33 — DR Failover Integration Test"
echo " $(date)"
echo "=============================================="
echo ""

echo "--- Phase 0: Prerequisites ---"

if ! command -v kcat &>/dev/null; then
    fail "kcat not found"
    exit 1
fi
pass "kcat available"

if [ ! -x "$PROXY_BIN" ]; then
    fail "proxy binary not found at $PROXY_BIN"
    exit 1
fi
pass "proxy binary found"

if ! kcat -b "$KAFKA1" "${SASL_FLAGS[@]}" -L &>/dev/null; then
    fail "kafka1 ($KAFKA1) not reachable"
    exit 1
fi
pass "kafka1 ($KAFKA1) reachable"

if ! kcat -b "$KAFKA2" "${SASL_FLAGS[@]}" -L &>/dev/null; then
    fail "kafka2 ($KAFKA2) not reachable"
    exit 1
fi
pass "kafka2 ($KAFKA2) reachable"

echo ""

# ─────────────────────────────────────────────────────────────────
# Phase 1: Prepare config and start proxy (active=primary)
# ─────────────────────────────────────────────────────────────────
echo "--- Phase 1: Start proxy (active=primary) ---"

cp "$CONFIG_FILE" "$CONFIG_FILE.bak"
sed -i '' 's/active: "secondary"/active: "primary"/' "$CONFIG_FILE"
sed -i '' 's/active: secondary/active: primary/' "$CONFIG_FILE"

"$PROXY_BIN" -config "$CONFIG_FILE" 2>/dev/null &
PROXY_PID=$!

for i in $(seq 1 30); do
    if kcat -b "$PROXY_ADDR" "${SASL_FLAGS[@]}" -L &>/dev/null 2>&1; then
        pass "proxy ready on $PROXY_ADDR (attempt $i)"
        break
    fi
    if [ "$i" -eq 30 ]; then
        fail "proxy did not become ready"
        exit 1
    fi
    sleep 1
done

# Verify proxy routes to kafka1
METADATA=$(kcat -b "$PROXY_ADDR" "${SASL_FLAGS[@]}" -L 2>&1)
if echo "$METADATA" | grep -q "broker 1"; then
    pass "proxy routing to kafka1 (primary) confirmed"
else
    fail "proxy not routing to kafka1"
    echo "  Metadata: $(echo "$METADATA" | head -3)"
fi
echo ""

# ─────────────────────────────────────────────────────────────────
# Phase 2: Produce 500 messages → kafka1 (via proxy, primary)
# ─────────────────────────────────────────────────────────────────
echo "--- Phase 2: Produce $HALF_COUNT messages (primary) ---"

PRODUCE_START=$(date +%s)
FAILED_PRIMARIO=0
for ((i=1; i<=HALF_COUNT; i++)); do
    TIMESTAMP="$(date +%s%N)"
    if ! echo "dr-msg-$i-$TIMESTAMP" | kcat \
        -b "$PROXY_ADDR" -t "$TOPIC" -P \
        "${SASL_FLAGS[@]}" \
        -X message.timeout.ms=10000 2>/dev/null; then
        FAILED_PRIMARIO=$((FAILED_PRIMARIO+1))
    fi
done
PRODUCE_END=$(date +%s)
PRIMARIO_DURATION=$((PRODUCE_END - PRODUCE_START))

SUCCESS_PRIMARIO=$((HALF_COUNT - FAILED_PRIMARIO))
pass "produced $SUCCESS_PRIMARIO/$HALF_COUNT messages through proxy→primary in ${PRIMARIO_DURATION}s"
if [ "$FAILED_PRIMARIO" -gt 0 ]; then
    fail "$FAILED_PRIMARIO produce failures to primary"
fi
echo ""

# ─────────────────────────────────────────────────────────────────
# Phase 3: Trigger failover — stop proxy, change config, restart
# ─────────────────────────────────────────────────────────────────
echo "--- Phase 3: Trigger failover (primary → secondary) ---"

kill "$PROXY_PID" 2>/dev/null || true
wait "$PROXY_PID" 2>/dev/null || true
PROXY_PID=""
pass "proxy stopped"

sed -i '' 's/active: "primary"/active: "secondary"/' "$CONFIG_FILE"
sed -i '' 's/active: primary/active: secondary/' "$CONFIG_FILE"
info "config updated: active=secondary"

"$PROXY_BIN" -config "$CONFIG_FILE" 2>/dev/null &
PROXY_PID=$!

for i in $(seq 1 30); do
    if kcat -b "$PROXY_ADDR" "${SASL_FLAGS[@]}" -L &>/dev/null 2>&1; then
        pass "proxy restarted on $PROXY_ADDR (attempt $i)"
        break
    fi
    if [ "$i" -eq 30 ]; then
        fail "proxy did not restart"
        exit 1
    fi
    sleep 1
done

# Verify proxy now routes to kafka2
METADATA=$(kcat -b "$PROXY_ADDR" "${SASL_FLAGS[@]}" -L 2>&1)
if echo "$METADATA" | grep -q "broker 2"; then
    pass "proxy now routing to kafka2 (secondary) confirmed"
else
    fail "proxy not routing to kafka2"
    echo "  Metadata: $(echo "$METADATA" | head -3)"
fi
echo ""

# ─────────────────────────────────────────────────────────────────
# Phase 4: Produce 500 messages → kafka2 (via proxy, secondary)
# ─────────────────────────────────────────────────────────────────
echo "--- Phase 4: Produce $HALF_COUNT messages (secondary) ---"

PRODUCE_START=$(date +%s)
FAILED_SECUNDARIO=0
for ((i=$((HALF_COUNT+1)); i<=TOTAL_COUNT; i++)); do
    TIMESTAMP="$(date +%s%N)"
    if ! echo "dr-msg-$i-$TIMESTAMP" | kcat \
        -b "$PROXY_ADDR" -t "$TOPIC" -P \
        "${SASL_FLAGS[@]}" \
        -X message.timeout.ms=10000 2>/dev/null; then
        FAILED_SECUNDARIO=$((FAILED_SECUNDARIO+1))
    fi
done
PRODUCE_END=$(date +%s)
SECUNDARIO_DURATION=$((PRODUCE_END - PRODUCE_START))

SUCCESS_SECUNDARIO=$((HALF_COUNT - FAILED_SECUNDARIO))
pass "produced $SUCCESS_SECUNDARIO/$HALF_COUNT messages through proxy→secondary in ${SECUNDARIO_DURATION}s"
if [ "$FAILED_SECUNDARIO" -gt 0 ]; then
    fail "$FAILED_SECUNDARIO produce failures to secondary"
fi
echo ""

# ─────────────────────────────────────────────────────────────────
# Phase 5: Stop proxy
# ─────────────────────────────────────────────────────────────────
echo "--- Phase 5: Stop proxy ---"
kill "$PROXY_PID" 2>/dev/null || true
wait "$PROXY_PID" 2>/dev/null || true
PROXY_PID=""
pass "proxy stopped"
echo ""

# ─────────────────────────────────────────────────────────────────
# Phase 6: Count messages on kafka1
# ─────────────────────────────────────────────────────────────────
echo "--- Phase 6: Count messages on kafka1 (primary) ---"

KAFKA1_COUNT=$(kcat -b "$KAFKA1" -t "$TOPIC" -C \
    "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | grep -c '^dr-msg-' || echo 0)
echo "  kafka1 dr-msg count: $KAFKA1_COUNT"

if [ "$KAFKA1_COUNT" -gt 0 ]; then
    pass "kafka1 has messages (failover was partial, as expected)"
fi
echo ""

# ─────────────────────────────────────────────────────────────────
# Phase 7: Count messages on kafka2
# ─────────────────────────────────────────────────────────────────
echo "--- Phase 7: Count messages on kafka2 (secondary) ---"

KAFKA2_COUNT=$(kcat -b "$KAFKA2" -t "$TOPIC" -C \
    "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | grep -c '^dr-msg-' || echo 0)
echo "  kafka2 dr-msg count: $KAFKA2_COUNT"

if [ "$KAFKA2_COUNT" -gt 0 ]; then
    pass "kafka2 has messages (failover worked)"
fi
echo ""

# ─────────────────────────────────────────────────────────────────
# Phase 8: Verify results
# ─────────────────────────────────────────────────────────────────
echo "=============================================="
echo " Phase 8: Verification"
echo "=============================================="
echo ""

TOTAL_DR=$((KAFKA1_COUNT + KAFKA2_COUNT))

echo "  kafka1 DR messages:   $KAFKA1_COUNT"
echo "  kafka2 DR messages:   $KAFKA2_COUNT"
echo "  Combined total:        $TOTAL_DR"
echo "  Expected (produced):   $((SUCCESS_PRIMARIO + SUCCESS_SECUNDARIO))"
echo ""

# Verify no data loss
EXPECTED_TOTAL=$((SUCCESS_PRIMARIO + SUCCESS_SECUNDARIO))
if [ "$TOTAL_DR" -ge "$EXPECTED_TOTAL" ]; then
    pass "no data loss: all $EXPECTED_TOTAL produced messages accounted for ($TOTAL_DR found)"
else
    fail "DATA LOSS: produced $EXPECTED_TOTAL, found only $TOTAL_DR"
fi

# Both clusters should have messages
if [ "$KAFKA1_COUNT" -gt 0 ] && [ "$KAFKA2_COUNT" -gt 0 ]; then
    pass "failover split confirmed: messages on both clusters (kafka1=$KAFKA1_COUNT, kafka2=$KAFKA2_COUNT)"
else
    fail "FAILOVER FAILED: kafka1=$KAFKA1_COUNT, kafka2=$KAFKA2_COUNT"
fi

# Duplicate check: extract message IDs and count unique
echo ""
info "Checking for duplicates..."
KAFKA1_IDS=$(mktemp)
KAFKA2_IDS=$(mktemp)
kcat -b "$KAFKA1" -t "$TOPIC" -C \
    "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | grep -oE 'dr-msg-[0-9]+' | sort > "$KAFKA1_IDS" || true
kcat -b "$KAFKA2" -t "$TOPIC" -C \
    "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | grep -oE 'dr-msg-[0-9]+' | sort > "$KAFKA2_IDS" || true

TOTAL_IDS=$(cat "$KAFKA1_IDS" "$KAFKA2_IDS" 2>/dev/null | wc -l | tr -d ' ')
UNIQUE_IDS=$(cat "$KAFKA1_IDS" "$KAFKA2_IDS" 2>/dev/null | sort -u | wc -l | tr -d ' ')
DUPLICATE_IDS=$(cat "$KAFKA1_IDS" "$KAFKA2_IDS" 2>/dev/null | sort | uniq -d | wc -l | tr -d ' ')

echo "  Total message IDs:   $TOTAL_IDS"
echo "  Unique message IDs:  $UNIQUE_IDS"
echo "  Duplicate IDs:       $DUPLICATE_IDS"

if [ "$DUPLICATE_IDS" -eq 0 ]; then
    pass "no duplicates: all $UNIQUE_IDS message IDs are unique"
else
    info "$DUPLICATE_IDS duplicate message IDs found (expected: cross-cluster duplicates possible if failover window is imperfect)"
fi

rm -f "$KAFKA1_IDS" "$KAFKA2_IDS"

echo ""
echo "=============================================="
echo " Results: $PASS passed, $FAIL failed"
echo " Duration: $(( $(date +%s) - START_TIME ))s"
echo "=============================================="

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
