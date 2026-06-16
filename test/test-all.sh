#!/bin/bash
# kafkaproxy test script — validates active/passive, load_balance, and failover
set -e

PROXY_HOST="localhost"
FINANCE_PORT="9093"
LOGISTICS_PORT="9094"
TOPIC="finance-topic"

SASL="-X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN -X sasl.username=${KAFKA_SASL_USER:-admin} -X sasl.password=${KAFKA_SASL_PASS:-admin-secret}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

# ─── 1. Health check ───────────────────────────────────────
info "=== Test 1: Proxy health ==="
curl -s http://localhost:8080/health | grep -q "ok" && pass "Proxy health OK" || fail "Proxy health failed"

# ─── 2. finance (active_passive) — list topics ──────────
info "=== Test 2: finance — list topics ==="
kcat -b ${PROXY_HOST}:${FINANCE_PORT} ${SASL} -L 2>&1 | grep -q "${TOPIC}" && pass "finance: topic visible" || fail "finance: topic not found"

# ─── 3. finance — produce/consume ───────────────────────
info "=== Test 3: finance — produce/consume ==="
MSG="finance-$(date +%s)"
echo "$MSG" | kcat -P -b ${PROXY_HOST}:${FINANCE_PORT} ${SASL} -t ${TOPIC}
sleep 1
kcat -C -b ${PROXY_HOST}:${FINANCE_PORT} ${SASL} -t ${TOPIC} -o -1 -e 2>&1 | grep -q "$MSG" && pass "finance: round-trip OK" || fail "finance: message not received"

# ─── 4. logistics (load_balance) — list topics ─────────────
info "=== Test 4: logistics — list topics ==="
kcat -b ${PROXY_HOST}:${LOGISTICS_PORT} ${SASL} -L 2>&1 | grep -q "logistics-topic" && pass "logistics: topic visible" || fail "logistics: topic not found"

# ─── 5. logistics — produce/consume ────────────────────────
info "=== Test 5: logistics — produce/consume ==="
MSG="logistics-$(date +%s)"
echo "$MSG" | kcat -P -b ${PROXY_HOST}:${LOGISTICS_PORT} ${SASL} -t logistics-topic
sleep 1
kcat -C -b ${PROXY_HOST}:${LOGISTICS_PORT} ${SASL} -t logistics-topic -o -1 -e 2>&1 | grep -q "$MSG" && pass "logistics: round-trip OK" || fail "logistics: message not received"

# ─── 6. /status endpoint ───────────────────────────────────
info "=== Test 6: /status endpoint ==="
curl -s http://localhost:8080/status | python3 -c "import sys,json; d=json.load(sys.stdin); names=[c['name'] for c in d['clusters']]; assert 'finance' in names; assert 'logistics' in names; print('OK')" && pass "/status: both clusters present" || fail "/status: cluster missing"

# ─── 7. /metrics endpoint ──────────────────────────────────
info "=== Test 7: /metrics endpoint ==="
curl -s http://localhost:8080/metrics | grep -q "proxy_connections_active\|proxy_routing_total\|proxy_health_check_total" && pass "/metrics: key metrics present" || fail "/metrics: metrics missing"

echo ""
echo -e "${GREEN}═══════════════════════════════════${NC}"
echo -e "${GREEN}  All tests passed!${NC}"
echo -e "${GREEN}═══════════════════════════════════${NC}"
echo ""
echo "Dashboards:"
echo "  Redpanda kafka1:  http://localhost:8081"
echo "  Redpanda kafka2:  http://localhost:8082"
echo "  Prometheus:       http://localhost:9090"
echo "  Proxy status:     http://localhost:8080/status"
echo "  Proxy metrics:    http://localhost:8080/metrics"
echo ""
echo "Failover test (manual):"
echo "  docker compose stop kafka1   # simula queda do primário"
echo "  curl http://localhost:8080/status | jq .  # verifica failover"
echo "  docker compose start kafka1  # restaura"
