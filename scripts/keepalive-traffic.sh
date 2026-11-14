#!/usr/bin/env bash
# ==============================================================================
# Payment Ledger Engine — 24/7 Anti-Idle Traffic & Health Heartbeat Daemon
# Generates realistic synthetic payment transfers to exercise the full pipeline:
# NGINX (:8088) -> Ingestion Cluster -> Kafka KRaft -> Settlement Workers -> Postgres & Redis
# Prevents Oracle Cloud Always Free 7-day idle VM reclamation (<20% CPU/RAM).
# ==============================================================================

GATEWAY_URL="http://127.0.0.1:8088/api/v1/transfers"
LOG_FILE="/var/log/ledger-keepalive.log"
CACHE_FILE="/tmp/ledger_recent_keys.txt"

touch "$CACHE_FILE" 2>/dev/null || true

# Execute 6 transfers spaced out by 10 seconds each (1 minute run duration)
for i in {1..6}; do
  TIMESTAMP=$(date +%s)
  RAND_VAL=$((RANDOM % 100))

  # 15% Idempotent Replay ratio, 85% fresh transfers
  if [ "$RAND_VAL" -lt 15 ] && [ -s "$CACHE_FILE" ]; then
    IDEMP_KEY=$(shuf -n 1 "$CACHE_FILE")
    IS_REPLAY="true"
  else
    if command -v uuidgen &>/dev/null; then
      IDEMP_KEY=$(uuidgen)
    else
      IDEMP_KEY=$(cat /proc/sys/kernel/random/uuid 2>/dev/null || date +%s%N)
    fi
    echo "$IDEMP_KEY" >> "$CACHE_FILE"
    # Keep only the last 50 keys in cache
    tail -n 50 "$CACHE_FILE" > "$CACHE_FILE.tmp" && mv "$CACHE_FILE.tmp" "$CACHE_FILE"
    IS_REPLAY="false"
  fi

  SRC_ID="ACC-$((1001 + RANDOM % 60))"
  TGT_ID="ACC-$((2001 + RANDOM % 60))"
  AMOUNT=$(awk -v min=10 -v max=500 'BEGIN{srand(); printf "%.2f", min+rand()*(max-min)}')

  PAYLOAD=$(cat <<EOF
{
  "sourceAccountId": "$SRC_ID",
  "targetAccountId": "$TGT_ID",
  "amount": $AMOUNT,
  "currency": "INR",
  "client_timestamp": $TIMESTAMP
}
EOF
)

  # Send HTTP request through NGINX API Gateway with precision timestamping
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$GATEWAY_URL" \
    -H "Content-Type: application/json" \
    -H "Idempotency-Key: $IDEMP_KEY" \
    -H "X-Client-Timestamp: $TIMESTAMP" \
    -d "$PAYLOAD" \
    --connect-timeout 3 \
    --max-time 5 || echo "CONN_ERR")

  NOW=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[$NOW] Keepalive Transfer | Key: ${IDEMP_KEY:0:8}... | $SRC_ID -> $TGT_ID | ₹$AMOUNT | Replay: $IS_REPLAY | HTTP: $HTTP_CODE" >> "$LOG_FILE" 2>/dev/null || true

  # Sleep 10s between requests (skip sleep on last iteration)
  if [ "$i" -lt 6 ]; then
    sleep 10
  fi
done
