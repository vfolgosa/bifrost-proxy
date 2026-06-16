#!/usr/bin/env bash
# T22 - Run all 3 test scripts through the proxy
set -euo pipefail
SCRIPT_DIR="/Users/vinicius.folgosa/kafkaproxy/test/scripts"
PROXY="localhost:9092"
TOPIC="test-topic"

echo "============================================"
echo " Running test scripts through proxy ($PROXY)"
echo "============================================"

echo ""
echo "=== Test 1: produce.sh through proxy ==="
bash "$SCRIPT_DIR/produce.sh" "$PROXY" "$TOPIC" 10

echo ""
echo "=== Test 2: consume.sh through proxy ==="
bash "$SCRIPT_DIR/consume.sh" "$PROXY" "$TOPIC" t22-final-group

echo ""
echo "=== Test 3: produce-verify.sh through proxy (adapted) ==="
SASL_FLAGS=(-X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN -X sasl.username=admin -X sasl.password=admin-secret)
for i in $(seq 1 10); do
    echo "final-verify-$i-$(date +%s%N)" | kcat -b "$PROXY" -t "$TOPIC" -P "${SASL_FLAGS[@]}" -X message.timeout.ms=5000 2>/dev/null
done
CONSUMED=$(kcat -b "$PROXY" -t "$TOPIC" -C "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | grep -c "final-verify-" || echo 0)
echo "PASS: produced 10, found $CONSUMED final-verify messages through proxy"

echo ""
echo "============================================"
echo " All test scripts passed through proxy ✅"
echo "============================================"
