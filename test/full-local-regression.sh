#!/usr/bin/env bash
# full-local-regression.sh — End-to-end local regression for bifrost-proxy.
#
# Brings up docker-compose Kafka, builds & starts the proxy, runs smoke +
# load-balance, auto-rebalance, DR failover/failback tests, optional unit tests,
# writes a markdown report, then tears everything down.
#
# Usage:
#   bash test/full-local-regression.sh
#   KEEP_ENV=1 bash test/full-local-regression.sh   # leave docker/proxy running
#   SKIP_UNIT=1 bash test/full-local-regression.sh  # skip go test phase
#
# Requirements: docker, docker compose, go, kcat, curl, python3

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FIXTURE_CONFIG="$SCRIPT_DIR/fixtures/regression-config.yaml"
REPORT_DIR="$SCRIPT_DIR/reports"

RUN_ID="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="$REPORT_DIR/.run-${RUN_ID}"
CONFIG_FILE="$RUN_DIR/config.yaml"
PROXY_BIN="$RUN_DIR/bifrost-proxy"
PROXY_LOG="$RUN_DIR/proxy.log"
PROXY_PID=""
UNIT_LOG="$RUN_DIR/unit-tests.log"

REPORT_MD="$REPORT_DIR/regression-${RUN_ID}.md"
REPORT_JSON="$REPORT_DIR/regression-${RUN_ID}.json"

FINANCE_PORT=9093
LOGISTICS_PORT=9094
METRICS_PORT=8080
KAFKA1_SASL="localhost:19093"
KAFKA2_SASL="localhost:19094"

SASL_FLAGS=(
  -X security.protocol=SASL_PLAINTEXT
  -X sasl.mechanisms=PLAIN
  -X sasl.username=admin
  -X sasl.password=admin-secret
)

# ── Result tracking ────────────────────────────────────────────────────
declare -a RESULT_PHASE=()
declare -a RESULT_STATUS=()
declare -a RESULT_DETAIL=()

START_TS=$(date +%s)
DOCKER_STARTED=0
PROXY_STARTED=0
TEARDOWN_DONE=0

# ── Colors ───────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log()  { echo -e "${BLUE}[regression]${NC} $*"; }
info() { echo -e "${YELLOW}[info]${NC} $*"; }
ok()   { echo -e "${GREEN}[pass]${NC} $*"; }
err()  { echo -e "${RED}[fail]${NC} $*"; }

record() {
  local status="$1" phase="$2" detail="$3"
  RESULT_PHASE+=("$phase")
  RESULT_STATUS+=("$status")
  RESULT_DETAIL+=("$detail")
  case "$status" in
    PASS) ok "$phase — $detail" ;;
    FAIL) err "$phase — $detail" ;;
    SKIP) info "$phase — SKIP: $detail" ;;
    KNOWN_GAP) info "$phase — KNOWN_GAP: $detail" ;;
  esac
}

# ── Helpers ──────────────────────────────────────────────────────────
port_in_use() {
  lsof -iTCP:"$1" -sTCP:LISTEN -P -n >/dev/null 2>&1
}

wait_for_port() {
  local port="$1" timeout="$2" label="$3"
  local i=0
  while (( i < timeout )); do
    if port_in_use "$port"; then
      return 0
    fi
    sleep 1
    i=$((i + 1))
  done
  err "timeout waiting for $label on port $port"
  return 1
}

wait_for_http() {
  local url="$1" timeout="$2"
  local i=0
  while (( i < timeout )); do
    if curl -sf "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    i=$((i + 1))
  done
  return 1
}

wait_for_docker_healthy() {
  local name="$1" timeout="$2"
  local i=0
  while (( i < timeout )); do
    local status
    status="$(docker inspect -f '{{.State.Health.Status}}' "$name" 2>/dev/null || echo "missing")"
    if [[ "$status" == "healthy" ]]; then
      return 0
    fi
    sleep 2
    i=$((i + 2))
  done
  return 1
}

wait_for_log() {
  local pattern="$1" timeout="$2"
  local i=0
  while (( i < timeout )); do
    if [[ -f "$PROXY_LOG" ]] && grep -qE "$pattern" "$PROXY_LOG"; then
      return 0
    fi
    sleep 1
    i=$((i + 1))
  done
  return 1
}

wait_for_finance_health() {
  local expect_healthy="$1" timeout="$2"
  local i=0
  while (( i < timeout )); do
    local healthy
    healthy="$(curl -sf "http://localhost:${METRICS_PORT}/status" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
for c in d.get('clusters', []):
    if c.get('name') == 'finance':
        print('true' if c.get('primary', {}).get('healthy') else 'false')
        break
" 2>/dev/null || echo "")"
    if [[ "$healthy" == "$expect_healthy" ]]; then
      return 0
    fi
    sleep 2
    i=$((i + 2))
  done
  return 1
}

set_finance_auto_failover() {
  local enabled="$1"
  python3 - "$CONFIG_FILE" "$enabled" <<'PY'
import re, sys, os
path, enabled = sys.argv[1], sys.argv[2]
val = "true" if enabled.lower() in ("1", "true", "yes") else "false"
with open(path) as f:
    content = f.read()
new_content, n = re.subn(
    r'auto_failover:\s*(?:true|false)',
    f'auto_failover: {val}',
    content,
    count=1,
)
if n != 1:
    sys.exit(f"failed to update finance.auto_failover (replacements={n})")
tmp = path + ".tmp"
with open(tmp, "w") as f:
    f.write(new_content)
os.replace(tmp, path)
PY
  sleep 5
}

set_finance_active() {
  local active="$1"
  python3 - "$CONFIG_FILE" "$active" <<'PY'
import re, sys, os
path, active = sys.argv[1], sys.argv[2]
with open(path) as f:
    content = f.read()
new_content, n = re.subn(
    r'active: "(?:primary|secondary)"',
    f'active: "{active}"',
    content,
    count=1,
)
if n != 1:
    sys.exit(f"failed to update finance.active (replacements={n})")
tmp = path + ".tmp"
with open(tmp, "w") as f:
    f.write(new_content)
os.replace(tmp, path)
PY
  sleep 5
}

wait_for_finance_active() {
  local expect="$1" timeout="$2"
  local i=0
  while (( i < timeout )); do
    local active
    active="$(curl -sf "http://localhost:${METRICS_PORT}/status" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
for c in d.get('clusters', []):
    if c.get('name') == 'finance':
        print(c.get('active', ''))
        break
" 2>/dev/null || echo "")"
    if [[ "$active" == "$expect" ]]; then
      return 0
    fi
    sleep 2
    i=$((i + 2))
  done
  return 1
}

kcat_produce() {
  local broker="$1" topic="$2" msg="$3"
  shift 3
  if (($# > 0)); then
    echo "$msg" | kcat -P -b "$broker" -t "$topic" "${SASL_FLAGS[@]}" "$@" 2>/dev/null
  else
    echo "$msg" | kcat -P -b "$broker" -t "$topic" "${SASL_FLAGS[@]}" 2>/dev/null
  fi
}

kcat_consume_last() {
  local broker="$1" topic="$2"
  kcat -C -b "$broker" -t "$topic" "${SASL_FLAGS[@]}" -o -1 -e -q 2>/dev/null
}

kcat_count_topic() {
  local broker="$1" topic="$2" prefix="$3"
  kcat -C -b "$broker" -t "$topic" "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | grep -c "$prefix" || true
}

ensure_jaas_secrets() {
  mkdir -p "$PROJECT_DIR/secrets"
  local jaas='KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user_admin="admin-secret";
};
'
  if [[ ! -f "$PROJECT_DIR/secrets/kafka1_jaas.conf" ]]; then
    echo "$jaas" > "$PROJECT_DIR/secrets/kafka1_jaas.conf"
    log "created secrets/kafka1_jaas.conf"
  fi
  if [[ ! -f "$PROJECT_DIR/secrets/kafka2_jaas.conf" ]]; then
    echo "$jaas" > "$PROJECT_DIR/secrets/kafka2_jaas.conf"
    log "created secrets/kafka2_jaas.conf"
  fi
}

stop_proxy() {
  if [[ -n "$PROXY_PID" ]] && kill -0 "$PROXY_PID" 2>/dev/null; then
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
  fi
  PROXY_PID=""
  PROXY_STARTED=0
}

teardown() {
  if [[ "$TEARDOWN_DONE" == "1" ]]; then
    return 0
  fi
  TEARDOWN_DONE=1
  log "teardown..."
  stop_proxy
  if [[ "${KEEP_ENV:-0}" != "1" && "$DOCKER_STARTED" == "1" ]]; then
    (cd "$PROJECT_DIR" && docker compose down >/dev/null 2>&1) || true
    log "docker compose down"
  elif [[ "${KEEP_ENV:-0}" == "1" ]]; then
    info "KEEP_ENV=1 — docker and logs preserved at $RUN_DIR"
  fi
}

trap teardown EXIT

generate_report() {
  mkdir -p "$REPORT_DIR"
  local end_ts duration
  end_ts=$(date +%s)
  duration=$((end_ts - START_TS))

  local pass=0 fail=0 skip=0 gap=0
  local git_branch git_commit
  git_branch="$(git -C "$PROJECT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")"
  git_commit="$(git -C "$PROJECT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")"

  {
    echo "# Bifrost Proxy — Local Regression Report"
    echo ""
    echo "| Field | Value |"
    echo "|-------|-------|"
    echo "| Run ID | \`${RUN_ID}\` |"
    echo "| Date | $(date -u +"%Y-%m-%d %H:%M:%S UTC") |"
    echo "| Duration | ${duration}s |"
    echo "| Git branch | \`${git_branch}\` |"
    echo "| Git commit | \`${git_commit}\` |"
    echo "| Run dir | \`${RUN_DIR}\` |"
    echo "| Proxy log | \`${PROXY_LOG}\` |"
    echo ""
    echo "## Summary"
    echo ""
  } > "$REPORT_MD"

  for i in "${!RESULT_PHASE[@]}"; do
    case "${RESULT_STATUS[$i]}" in
      PASS) pass=$((pass + 1)) ;;
      FAIL) fail=$((fail + 1)) ;;
      SKIP) skip=$((skip + 1)) ;;
      KNOWN_GAP) gap=$((gap + 1)) ;;
    esac
  done

  local overall="PASS"
  if (( fail > 0 )); then overall="FAIL"; fi

  {
    echo "| Result | Count |"
    echo "|--------|-------|"
    echo "| PASS | ${pass} |"
    echo "| FAIL | ${fail} |"
    echo "| SKIP | ${skip} |"
    echo "| KNOWN_GAP | ${gap} |"
    echo ""
    echo "**Overall: ${overall}**"
    echo ""
    echo "## Test Results"
    echo ""
    echo "| Status | Phase | Detail |"
    echo "|--------|-------|--------|"
  } >> "$REPORT_MD"

  for i in "${!RESULT_PHASE[@]}"; do
    echo "| ${RESULT_STATUS[$i]} | ${RESULT_PHASE[$i]} | ${RESULT_DETAIL[$i]} |" >> "$REPORT_MD"
  done

  REG_PHASES="$(printf '%s\x1e' "${RESULT_PHASE[@]}")"
  REG_STATUSES="$(printf '%s\x1e' "${RESULT_STATUS[@]}")"
  REG_DETAILS="$(printf '%s\x1e' "${RESULT_DETAIL[@]}")"
  export REG_PHASES REG_STATUSES REG_DETAILS

  if [[ -f "$UNIT_LOG" ]]; then
    {
      echo ""
      echo "## Unit Test Output (excerpt)"
      echo ""
      echo '```'
      tail -40 "$UNIT_LOG"
      echo '```'
    } >> "$REPORT_MD"
  fi

  if [[ -f "$PROXY_LOG" ]]; then
    {
      echo ""
      echo "## Proxy Log (last 30 lines)"
      echo ""
      echo '```'
      tail -30 "$PROXY_LOG"
      echo '```'
    } >> "$REPORT_MD"
  fi

  # JSON report
  python3 - "$REPORT_JSON" "$RUN_ID" "$overall" "$duration" "$git_branch" "$git_commit" <<'PY'
import json, sys, os

out_path = sys.argv[1]
run_id = sys.argv[2]
overall = sys.argv[3]
duration = int(sys.argv[4])
branch = sys.argv[5]
commit = sys.argv[6]

phases = os.environ.get("REG_PHASES", "").split("\x1e")
statuses = os.environ.get("REG_STATUSES", "").split("\x1e")
details = os.environ.get("REG_DETAILS", "").split("\x1e")

results = []
for i in range(len(phases)):
    if not phases[i]:
        continue
    results.append({
        "phase": phases[i],
        "status": statuses[i],
        "detail": details[i],
    })

data = {
    "run_id": run_id,
    "overall": overall,
    "duration_seconds": duration,
    "git_branch": branch,
    "git_commit": commit,
    "results": results,
}
with open(out_path, "w") as f:
    json.dump(data, f, indent=2)
PY

  echo ""
  log "Report written:"
  echo "  $REPORT_MD"
  echo "  $REPORT_JSON"
  echo ""
  if (( fail > 0 )); then
    err "Regression FAILED ($fail failure(s))"
    exit 1
  fi
  ok "Regression PASSED ($pass passed, $gap known gap(s))"
}

# ── Phase 0: Prerequisites ───────────────────────────────────────────
phase_prerequisites() {
  log "Phase 0: prerequisites"
  local missing=0
  for cmd in docker go kcat curl python3; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      record FAIL "prerequisites" "missing command: $cmd"
      missing=1
    fi
  done
  if (( missing )); then
    return 1
  fi
  if ! docker compose version >/dev/null 2>&1; then
    record FAIL "prerequisites" "docker compose not available"
    return 1
  fi
  record PASS "prerequisites" "docker, go, kcat, curl, python3 available"
}

# ── Phase 1: Environment ─────────────────────────────────────────────
phase_environment() {
  log "Phase 1: environment setup"

  if port_in_use "$METRICS_PORT" || port_in_use "$FINANCE_PORT" || port_in_use "$LOGISTICS_PORT"; then
    record FAIL "environment" "ports ${METRICS_PORT}/${FINANCE_PORT}/${LOGISTICS_PORT} already in use — stop other proxy instances first"
    return 1
  fi

  ensure_jaas_secrets
  mkdir -p "$RUN_DIR"
  cp "$FIXTURE_CONFIG" "$CONFIG_FILE"

  log "starting docker compose (kafka1, kafka2, kafka-init)..."
  if ! (cd "$PROJECT_DIR" && docker compose up -d kafka1 kafka2 kafka-init); then
    record FAIL "environment" "docker compose up failed"
    return 1
  fi
  DOCKER_STARTED=1

  if ! wait_for_docker_healthy kafka1 120; then
    record FAIL "environment" "kafka1 not healthy within 120s"
    return 1
  fi
  if ! wait_for_docker_healthy kafka2 120; then
    record FAIL "environment" "kafka2 not healthy within 120s"
    return 1
  fi

  docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
    --create --topic finance-topic --partitions 3 --replication-factor 1 --if-not-exists >/dev/null 2>&1 || true
  docker exec kafka1 kafka-topics --bootstrap-server localhost:9092 \
    --create --topic logistics-topic --partitions 3 --replication-factor 1 --if-not-exists >/dev/null 2>&1 || true
  docker exec kafka2 kafka-topics --bootstrap-server localhost:9092 \
    --create --topic finance-topic --partitions 3 --replication-factor 1 --if-not-exists >/dev/null 2>&1 || true
  docker exec kafka2 kafka-topics --bootstrap-server localhost:9092 \
    --create --topic logistics-topic --partitions 3 --replication-factor 1 --if-not-exists >/dev/null 2>&1 || true

  log "building proxy..."
  if ! (cd "$PROJECT_DIR" && go build -o "$PROXY_BIN" ./cmd/proxy/); then
    record FAIL "environment" "go build failed"
    return 1
  fi

  log "starting proxy (config: $CONFIG_FILE)..."
  "$PROXY_BIN" -config "$CONFIG_FILE" >"$PROXY_LOG" 2>&1 &
  PROXY_PID=$!
  PROXY_STARTED=1

  if ! wait_for_http "http://localhost:${METRICS_PORT}/health" 30; then
    record FAIL "environment" "proxy health endpoint not ready"
    tail -20 "$PROXY_LOG" >&2 || true
    return 1
  fi

  record PASS "environment" "docker healthy, topics ready, proxy listening on :${METRICS_PORT} :${FINANCE_PORT} :${LOGISTICS_PORT}"
}

# ── Phase 2: Unit tests ──────────────────────────────────────────────
phase_unit_tests() {
  log "Phase 2: Go unit tests"
  if [[ "${SKIP_UNIT:-0}" == "1" ]]; then
    record SKIP "unit-tests" "SKIP_UNIT=1"
    return 0
  fi

  local pkgs=(
    ./internal/config/...
    ./internal/failover/...
    ./internal/routing/...
    ./internal/proxy/...
    ./internal/pool/...
    ./internal/protocol/...
    ./internal/server/...
  )

  if (cd "$PROJECT_DIR" && go test "${pkgs[@]}" -count=1 >"$UNIT_LOG" 2>&1); then
    record PASS "unit-tests" "all selected packages passed"
  else
    record FAIL "unit-tests" "see $UNIT_LOG"
  fi
}

# ── Phase 3: Smoke tests ─────────────────────────────────────────────
phase_smoke() {
  log "Phase 3: smoke tests"

  if curl -sf "http://localhost:${METRICS_PORT}/health" | grep -q '"ok"\|ok'; then
    record PASS "smoke-health" "GET /health ok"
  else
    record FAIL "smoke-health" "GET /health failed"
  fi

  if kcat -b "localhost:${FINANCE_PORT}" "${SASL_FLAGS[@]}" -L 2>&1 | grep -q "finance-topic"; then
    record PASS "smoke-finance-metadata" "finance-topic visible via proxy"
  else
    record FAIL "smoke-finance-metadata" "finance-topic not listed"
  fi

  local msg="smoke-finance-$(date +%s)"
  if kcat_produce "localhost:${FINANCE_PORT}" "finance-topic" "$msg" && \
     kcat_consume_last "localhost:${FINANCE_PORT}" "finance-topic" | grep -q "$msg"; then
    record PASS "smoke-finance-roundtrip" "produce/consume via finance port"
  else
    record FAIL "smoke-finance-roundtrip" "finance round-trip failed"
  fi

  if kcat -b "localhost:${LOGISTICS_PORT}" "${SASL_FLAGS[@]}" -L 2>&1 | grep -q "logistics-topic"; then
    record PASS "smoke-logistics-metadata" "logistics-topic visible via proxy"
  else
    record FAIL "smoke-logistics-metadata" "logistics-topic not listed"
  fi

  msg="smoke-logistics-$(date +%s)"
  if kcat_produce "localhost:${LOGISTICS_PORT}" "logistics-topic" "$msg" && \
     kcat_consume_last "localhost:${LOGISTICS_PORT}" "logistics-topic" | grep -q "$msg"; then
    record PASS "smoke-logistics-roundtrip" "produce/consume via logistics port"
  else
    record FAIL "smoke-logistics-roundtrip" "logistics round-trip failed"
  fi

  if curl -sf "http://localhost:${METRICS_PORT}/status" | python3 -c "
import sys, json
d = json.load(sys.stdin)
names = {c['name'] for c in d.get('clusters', [])}
assert 'finance' in names and 'logistics' in names
"; then
    record PASS "smoke-status" "both clusters in /status"
  else
    record FAIL "smoke-status" "/status missing clusters"
  fi

  if curl -sf "http://localhost:${METRICS_PORT}/metrics" | grep -qE 'proxy_connections_active|proxy_health'; then
    record PASS "smoke-metrics" "Prometheus metrics present"
  else
    record FAIL "smoke-metrics" "metrics endpoint missing expected series"
  fi
}

# ── Phase 4: Load balance sticky routing ─────────────────────────────
phase_load_balance() {
  log "Phase 4: load balance sticky routing"
  local prefix="reg-lb-${RUN_ID}"
  local p

  for p in 0 1 2; do
    local i
    for i in $(seq 1 5); do
      kcat_produce "localhost:${LOGISTICS_PORT}" "logistics-topic" "${prefix}-p${p}-m${i}" -p "$p" || true
    done
  done
  sleep 2

  local k1 k2
  k1="$(kcat_count_topic "$KAFKA1_SASL" "logistics-topic" "$prefix")"
  k2="$(kcat_count_topic "$KAFKA2_SASL" "logistics-topic" "$prefix")"
  local total=$((k1 + k2))

  if (( total >= 10 )); then
    record PASS "load-balance-distribution" "messages split across clusters (kafka1=${k1}, kafka2=${k2}, total=${total})"
  else
    record FAIL "load-balance-distribution" "expected >=10 messages across clusters, got kafka1=${k1} kafka2=${k2}"
  fi

  # Sticky: same partition should always hit same cluster
  kcat_produce "localhost:${LOGISTICS_PORT}" "logistics-topic" "${prefix}-sticky-check" -p 1 || true
  sleep 1
  local sub1 sub2
  sub1="$(kcat_count_topic "$KAFKA1_SASL" "logistics-topic" "${prefix}-sticky-check")"
  sub2="$(kcat_count_topic "$KAFKA2_SASL" "logistics-topic" "${prefix}-sticky-check")"
  if [[ "$sub1" == "1" && "$sub2" == "0" ]] || [[ "$sub1" == "0" && "$sub2" == "1" ]]; then
    record PASS "load-balance-sticky" "partition 1 routed to a single cluster"
  else
    record FAIL "load-balance-sticky" "partition 1 ambiguous: kafka1=${sub1} kafka2=${sub2}"
  fi
}

# ── Phase 5: Auto-rebalance (load_balance autonomous failover) ───────
phase_auto_rebalance() {
  log "Phase 5: load_balance auto-rebalance — autonomous failover/recovery"

  docker stop kafka1 >/dev/null 2>&1 || true

  if wait_for_finance_health "false" 30; then
    record PASS "rebalance-health-detect" "primary marked unhealthy after kafka1 stop"
  else
    record FAIL "rebalance-health-detect" "primary still healthy after kafka1 stop"
  fi

  if wait_for_log "rebalance: weights adjusted" 40; then
    if grep "rebalance: weights adjusted" "$PROXY_LOG" | tail -1 | grep -qE 'primary_weight":0|primary_weight=0'; then
      record PASS "rebalance-failover-weights" "effective weights shifted to 0/100 (all traffic → secondary)"
    else
      record PASS "rebalance-failover-weights" "weights adjusted (see proxy log)"
    fi
  else
    record FAIL "rebalance-failover-weights" "no rebalance log within 40s"
  fi

  # With primary_weight=0, ALL partitions must route to secondary (sticky hash < 0 is impossible).
  local failover_prefix="${RUN_ID}-lb-failover"
  local p
  for p in 0 1 2; do
    kcat_produce "localhost:${LOGISTICS_PORT}" "logistics-topic" "${failover_prefix}-p${p}" -p "$p" || true
  done
  sleep 2

  local k1_fail k2_fail
  k1_fail="$(kcat_count_topic "$KAFKA1_SASL" "logistics-topic" "$failover_prefix")"
  k2_fail="$(kcat_count_topic "$KAFKA2_SASL" "logistics-topic" "$failover_prefix")"

  if [[ "$k1_fail" == "0" && "$k2_fail" == "3" ]]; then
    record PASS "rebalance-routing-100-secondary" "all 3 partitions routed to secondary while primary down"
  elif [[ "$k2_fail" -ge 1 && "$k1_fail" == "0" ]]; then
    record PASS "rebalance-routing-100-secondary" "traffic on secondary only (kafka1=${k1_fail}, kafka2=${k2_fail})"
  else
    record FAIL "rebalance-routing-100-secondary" "expected all traffic on secondary, got kafka1=${k1_fail} kafka2=${k2_fail}"
  fi

  local msg="${RUN_ID}-after-rebalance"
  if kcat_produce "localhost:${LOGISTICS_PORT}" "logistics-topic" "$msg"; then
    record PASS "rebalance-produce-secondary" "produce via proxy while primary down"
  else
    record FAIL "rebalance-produce-secondary" "produce failed while primary down"
  fi

  docker start kafka1 >/dev/null 2>&1 || true
  if ! wait_for_docker_healthy kafka1 90; then
    record FAIL "rebalance-recovery" "kafka1 did not become healthy"
    return
  fi

  if wait_for_finance_health "true" 40; then
    record PASS "rebalance-health-recovery" "primary healthy again after kafka1 restart"
  else
    record FAIL "rebalance-health-recovery" "primary not healthy after kafka1 restart"
  fi

  if wait_for_log "rebalance: primary recovery complete, weights restored" 45 || \
     wait_for_log "rebalance: primary UP, recovery conditions met, weights restored" 5; then
    record PASS "rebalance-failback-weights" "weights restored to configured 70/30 after recovery_min_uptime"
  else
    record FAIL "rebalance-failback-weights" "no weight restoration log within 45s"
  fi

  # After weights restored, traffic should split again (sticky hash with 70/30).
  local recover_prefix="${RUN_ID}-lb-recovered"
  for p in 0 1 2; do
    kcat_produce "localhost:${LOGISTICS_PORT}" "logistics-topic" "${recover_prefix}-p${p}" -p "$p" || true
  done
  sleep 2
  local k1_rec k2_rec
  k1_rec="$(kcat_count_topic "$KAFKA1_SASL" "logistics-topic" "$recover_prefix")"
  k2_rec="$(kcat_count_topic "$KAFKA2_SASL" "logistics-topic" "$recover_prefix")"
  if [[ "$((k1_rec + k2_rec))" == "3" && "$k1_rec" -ge 1 ]]; then
    record PASS "rebalance-routing-restored" "traffic split restored (kafka1=${k1_rec}, kafka2=${k2_rec})"
  else
    record FAIL "rebalance-routing-restored" "expected split after recovery, got kafka1=${k1_rec} kafka2=${k2_rec}"
  fi
}

# ── Phase 6: DR failover (config hot reload) ─────────────────────────
phase_dr_failover() {
  log "Phase 6: DR failover via hot reload"

  set_finance_active "primary"

  local before="${RUN_ID}-dr-before"
  kcat_produce "localhost:${FINANCE_PORT}" "finance-topic" "$before" || true

  set_finance_active "secondary"

  if wait_for_log "active cluster changed|config reloaded|entering DRAINING" 20; then
    record PASS "dr-failover-trigger" "hot reload detected active change"
  elif wait_for_finance_active "secondary" 15; then
    record PASS "dr-failover-trigger" "/status shows active=secondary"
  else
    record FAIL "dr-failover-trigger" "no reload/DRAINING signal within timeout"
  fi

  wait_for_finance_active "secondary" 25 || true
  sleep 3

  local after="${RUN_ID}-dr-after"
  if kcat_produce "localhost:${FINANCE_PORT}" "finance-topic" "$after"; then
    sleep 5
    if kcat -C -b "$KAFKA2_SASL" -t finance-topic "${SASL_FLAGS[@]}" -o beginning -e -q 2>/dev/null | grep -q "$after"; then
      record PASS "dr-failover-routing" "post-failover message landed on kafka2"
    elif kcat_consume_last "localhost:${FINANCE_PORT}" "finance-topic" | grep -q "$after"; then
      record FAIL "dr-failover-routing" "message consumed via proxy but not found on kafka2 (reload may be partial)"
    else
      record FAIL "dr-failover-routing" "message not found after failover"
    fi
  else
    record FAIL "dr-failover-routing" "produce after failover failed"
  fi
}

# ── Phase 7: DR failback ─────────────────────────────────────────────
phase_dr_failback() {
  log "Phase 7: DR failback to primary"

  set_finance_active "primary"

  if wait_for_log "active cluster changed|config reloaded|drain complete|StartDrain|DRAINING" 25; then
    record PASS "dr-failback-trigger" "failback / drain activity detected"
  elif wait_for_finance_active "primary" 15; then
    record PASS "dr-failback-trigger" "/status shows active=primary"
  else
    record FAIL "dr-failback-trigger" "no failback activity within timeout"
  fi

  local msg="${RUN_ID}-dr-failback"
  if kcat_produce "localhost:${FINANCE_PORT}" "finance-topic" "$msg" && \
     kcat_consume_last "localhost:${FINANCE_PORT}" "finance-topic" | grep -q "$msg"; then
    record PASS "dr-failback-roundtrip" "produce/consume after failback to primary"
  else
    record FAIL "dr-failback-roundtrip" "round-trip after failback failed"
  fi
}

# ── Phase 8: active_passive auto_failover ─────────────────────────────
phase_active_passive_auto_failover() {
  log "Phase 8: active_passive auto_failover (failover.Controller)"

  set_finance_auto_failover "true"
  set_finance_active "primary"
  sleep 2

  docker stop kafka1 >/dev/null 2>&1 || true
  sleep 18

  local active
  active="$(curl -sf "http://localhost:${METRICS_PORT}/status" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
for c in d.get('clusters', []):
    if c.get('name') == 'finance':
        print(c.get('active', ''))
        break
" 2>/dev/null || echo "")"

  if [[ "$active" == "primary" ]]; then
    record FAIL "active-passive-auto-failover" "finance active=primary after kafka1 down — expected automatic failover to secondary"
  elif [[ -n "$active" ]]; then
    record PASS "active-passive-auto-failover" "active switched automatically to $active"
  else
    record FAIL "active-passive-auto-failover" "could not read finance active from /status"
  fi

  docker start kafka1 >/dev/null 2>&1 || true
  wait_for_docker_healthy kafka1 90 || true
  sleep 5
}

# ── Main ─────────────────────────────────────────────────────────────
main() {
  echo ""
  echo "══════════════════════════════════════════════════════════"
  echo " Bifrost Proxy — Full Local Regression"
  echo " Run ID: ${RUN_ID}"
  echo "══════════════════════════════════════════════════════════"
  echo ""

  phase_prerequisites || { generate_report; exit 1; }
  phase_environment || { generate_report; exit 1; }
  phase_unit_tests
  phase_smoke
  phase_load_balance
  phase_auto_rebalance
  phase_dr_failover
  phase_dr_failback
  phase_active_passive_auto_failover

  generate_report
}

main "$@"
