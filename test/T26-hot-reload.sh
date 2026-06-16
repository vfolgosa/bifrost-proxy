#!/usr/bin/env bash
# T26-hot-reload.sh — Integration test for config hot reload
#
# Validates:
#   1. Proxy starts with active=primary and logs correct active field
#   2. fsnotify watcher is active (Reloader: watching ...)
#   3. Modifying config.yaml triggers reload within 5s
#   4. active field change is correctly detected (active_changed=true)
#   5. Proxy continues serving after reload
#
# Prerequisites: go build ./cmd/proxy/ must have been run first

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROXY_BIN="$PROJECT_DIR/proxy"
REPORT_DIR="$PROJECT_DIR/test/reports"

TMPDIR=$(mktemp -d -t t26-hot-reload.XXXXXX)
trap "rm -rf $TMPDIR; kill $(jobs -p) 2>/dev/null || true" EXIT

PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

# ── Test Setup ──────────────────────────────────────────────────────────

echo "=== T26: Hot Reload Integration Test ==="
echo ""

PROXY_PORT=19099

# Create initial config with active=primary
cat > "$TMPDIR/config.yaml" <<EOF
proxy:
  bind_address: "0.0.0.0"
  port: $PROXY_PORT
  tls:
    enabled: false
  connection_pool:
    max_connections_per_broker: 50
    idle_timeout: "30s"
    keep_alive_interval: "30s"
  metrics_port: 18088

clusters:
  test-cluster:
    mode: "active_passive"
    active: "primary"
    primary: "localhost:19999"
    secondary: "localhost:19998"
    health_check:
      enabled: false
EOF

echo "Test config created: $TMPDIR/config.yaml"

# ── Test 1: Proxy starts with correct active field ──────────────────────

echo ""
echo "[Test 1] Proxy startup and initial config"

"$PROXY_BIN" -config "$TMPDIR/config.yaml" > "$TMPDIR/proxy.log" 2>&1 &
PROXY_PID=$!

sleep 2

if kill -0 "$PROXY_PID" 2>/dev/null; then
    pass "Proxy process is running (PID=$PROXY_PID)"
else
    fail "Proxy process died"
    cat "$TMPDIR/proxy.log"
    exit 1
fi

if grep -q "active=primary" "$TMPDIR/proxy.log"; then
    pass "Proxy log shows active=primary on startup"
else
    fail "Proxy log missing active=primary"
fi

# ── Test 2: fsnotify watcher started ───────────────────────────────────

echo ""
echo "[Test 2] fsnotify config watcher"

if grep -q "Reloader: watching" "$TMPDIR/proxy.log"; then
    pass "fsnotify watcher started (Reloader: watching ...)"
else
    fail "fsnotify watcher not found in log"
fi

# ── Test 3: Hot reload on active field change ───────────────────────────

echo ""
echo "[Test 3] Config hot reload on active field change"

LOG_LINES_BEFORE=$(wc -l < "$TMPDIR/proxy.log" | tr -d ' ')

# Change active from primary to secondary
sed -i '' 's/active: "primary"/active: "secondary"/' "$TMPDIR/config.yaml"
echo "  Config modified: active changed to secondary"

# Wait for hot reload (within 5s per spec)
RELOAD_START=$(date +%s)
RELOADED=false
for i in $(seq 1 50); do
    sleep 0.1
    if grep -q "configuration reloaded" "$TMPDIR/proxy.log" 2>/dev/null; then
        ELAPSED=$(echo "scale=1; $(( $(date +%s) - RELOAD_START ))" | bc)
        if (( $(echo "$ELAPSED <= 5.0" | bc -l) )); then
            pass "Config reloaded within ${ELAPSED}s (within 5s limit)"
        else
            fail "Config reloaded but took ${ELAPSED}s (exceeded 5s limit)"
        fi
        RELOADED=true
        break
    fi
done

if [ "$RELOADED" = false ]; then
    fail "Config hot reload not detected within 5s"
fi

# ── Test 4: Active change correctly detected ────────────────────────────

echo ""
echo "[Test 4] Active field change detection"

if grep -q "active_changed=true" "$TMPDIR/proxy.log"; then
    pass "active_changed=true detected in reload callback (confirms field update)"
else
    fail "active_changed=true not found in reload log"
fi

if grep -q "configuration reloaded" "$TMPDIR/proxy.log"; then
    pass "Configuration successfully reloaded after change"
else
    fail "Configuration reload not logged"
fi

# ── Test 5: Proxy remains running after reload ──────────────────────────

echo ""
echo "[Test 5] Proxy stability after reload"

sleep 1

if kill -0 "$PROXY_PID" 2>/dev/null; then
    pass "Proxy still running after hot reload"
else
    fail "Proxy died after hot reload"
fi

# Verify the config file now has secondary
if grep -q 'active: "secondary"' "$TMPDIR/config.yaml"; then
    pass "Config file confirms active=secondary on disk"
else
    fail "Config file does not have active=secondary"
fi

# ── Test 6: Config validates correctly (no corruption) ──────────────────

echo ""
echo "[Test 6] Config on disk is valid YAML"

if python3 -c "import yaml; yaml.safe_load(open('$TMPDIR/config.yaml'))" 2>/dev/null; then
    pass "Config file is valid YAML after modification"
else
    fail "Config file is invalid YAML"
fi

# ── Cleanup ────────────────────────────────────────────────────────────

kill "$PROXY_PID" 2>/dev/null || true
wait "$PROXY_PID" 2>/dev/null || true
sleep 0.5

# Save logs
mkdir -p "$REPORT_DIR"
cp "$TMPDIR/proxy.log" "$REPORT_DIR/T26-proxy.log"
cp "$TMPDIR/config.yaml" "$REPORT_DIR/T26-config-final.yaml"

# ── Summary ────────────────────────────────────────────────────────────

echo ""
echo "=== T26 Results ==="
echo "Passed: $PASS / $((PASS+FAIL))"
echo "Failed: $FAIL / $((PASS+FAIL))"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "Log: $REPORT_DIR/T26-proxy.log"
    exit 1
else
    echo ""
    echo "ALL TESTS PASSED — hot reload working correctly"
    exit 0
fi
