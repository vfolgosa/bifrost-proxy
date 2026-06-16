#!/bin/bash
# High-throughput producer for logistics BU

PORT="9094"
TOPIC="logistics-topic"
SASL_USER="${KAFKA_SASL_USER:-admin}"
SASL_PASS="${KAFKA_SASL_PASS:-admin-secret}"
SASL="-X security.protocol=SASL_PLAINTEXT -X sasl.mechanisms=PLAIN -X sasl.username=${SASL_USER} -X sasl.password=${SASL_PASS}"
PROXY="localhost:${PORT}"

PARALLEL=${1:-4}

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}High-throughput producer — ${PARALLEL} workers${NC}"
echo "Proxy: ${PROXY}  Topic: ${TOPIC}"
echo "Ctrl+C to stop"
echo ""

COUNT=0
START=$(date +%s)
trap 'echo ""; ELAPSED=$(($(date +%s)-START)); echo -e "${GREEN}Sent $COUNT msgs in ${ELAPSED}s${NC}"; kill 0; exit 0' INT TERM

for i in $(seq 1 $PARALLEL); do
  (
    while true; do
      echo "msg-${i}-$(date +%s%N)" | kcat -P -b ${PROXY} ${SASL} -t ${TOPIC} 2>/dev/null
    done
  ) &
done

while true; do
  sleep 2
  ELAPSED=$(($(date +%s)-START))
  echo -e "[$(date +%H:%M:%S)] running ${ELAPSED}s"
done
