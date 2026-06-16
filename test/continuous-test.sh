#!/bin/bash
# Continuous produce loop for logistics BU — Ctrl+C to stop

PROXY_HOST="localhost"
PORT="9094"
TOPIC="logistics-topic"
SASL_USER="${KAFKA_SASL_USER:-admin}"
SASL_PASS="${KAFKA_SASL_PASS:-admin-secret}"
SASL="-X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN -X sasl.username=${SASL_USER} -X sasl.password=${SASL_PASS}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

COUNT=0
START=$(date +%s)

trap 'echo ""; echo -e "${GREEN}Stopped. Sent $COUNT messages in $(( $(date +%s) - START ))s${NC}"; exit 0' INT TERM

echo -e "${YELLOW}Continuous produce — logistics:${PORT} → ${TOPIC} — Ctrl+C to stop${NC}"
echo ""

while true; do
  COUNT=$((COUNT + 1))
  TS=$(date +%H:%M:%S)
  MSG="logistics-msg-${COUNT}-$(date +%s%N)"

  echo "$MSG" | kcat -P -b ${PROXY_HOST}:${PORT} ${SASL} -t ${TOPIC} 2>/dev/null
  sleep 0.1

  if [ $((COUNT % 10)) -eq 0 ]; then
    echo -e "[${TS}] ${BLUE}#${COUNT}${NC} ${GREEN}OK${NC} — ${MSG}"
  fi
done
