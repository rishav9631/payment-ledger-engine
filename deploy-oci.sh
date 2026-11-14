#!/usr/bin/env bash
# ==============================================================================
# Oracle Cloud Always Free (ARM64) Automated Provision & Deploy Script
# Target: payment-ledger-engine (2 OCPUs, 12 GB RAM - VM.Standard.A1.Flex)
# ==============================================================================

set -euo pipefail

INSTANCE_NAME="payment-ledger-prod"
SSH_KEY_PUB="${HOME}/.ssh/id_rsa.pub"
SSH_KEY_PRIV="${HOME}/.ssh/id_rsa"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure OCI CLI is available in PATH (check standard pip/local paths)
if ! command -v oci &> /dev/null; then
  for CANDIDATE in "$HOME/.local/bin" "$HOME/bin" "$APPDATA/Python/Python311/Scripts"; do
    if [ -x "$CANDIDATE/oci" ] || [ -x "$CANDIDATE/oci.exe" ]; then
      export PATH="$CANDIDATE:$PATH"
      break
    fi
  done
fi

if ! command -v oci &> /dev/null; then
  echo "[ERROR] OCI CLI ('oci') not found."
  echo "        Please install OCI CLI or run: oci session authenticate"
  exit 1
fi

# Check OCI Session / Authentication
echo "[1/7] Verifying OCI CLI authentication..."
if ! oci iam compartment list --limit 1 &> /dev/null; then
  echo "[INFO] No active OCI session found. Launching 1-click browser login..."
  oci session authenticate
fi

# Ensure SSH Key exists
if [ ! -f "$SSH_KEY_PUB" ]; then
  echo "[INFO] Generating SSH key pair at $SSH_KEY_PUB..."
  mkdir -p "${HOME}/.ssh"
  ssh-keygen -t rsa -b 4096 -f "$SSH_KEY_PRIV" -N ""
fi

# 2. Discover Tenancy, Compartment and Subnet
echo "[2/7] Fetching OCI Tenancy, Compartment, and Subnet..."
TENANCY_ID=$(oci iam compartment list --include-root --query "data[?contains(\"id\", 'tenancy')].id | [0]" --raw-output 2>/dev/null || oci iam compartment list --query "data[0].\"compartment-id\"" --raw-output)
if [ -z "$TENANCY_ID" ] || [ "$TENANCY_ID" == "null" ]; then
  TENANCY_ID=$(oci iam compartment list --query "data[0].id" --raw-output)
fi

SUBNET_ID=$(oci network subnet list --compartment-id "$TENANCY_ID" --query "data[0].id" --raw-output 2>/dev/null || true)
if [ -z "$SUBNET_ID" ] || [ "$SUBNET_ID" == "null" ]; then
  echo "[ERROR] No public subnet found in compartment. Please create a default VCN with Internet Connectivity in OCI Console."
  exit 1
fi

SEC_LIST_ID=$(oci network subnet get --subnet-id "$SUBNET_ID" --query "data.\"security-list-ids\"[0]" --raw-output)

# 3. Open Ingress Ports in VCN Security List (80, 443, 3000, 3005, 5000, 8089)
echo "[3/7] Ensuring ports (80, 443, 3000, 3005, 5000, 8089) are open in VCN Security List ($SEC_LIST_ID)..."
CURRENT_INGRESS=$(oci network security-list get --security-list-id "$SEC_LIST_ID" --query "data.\"ingress-security-rules\"" --raw-output)

UPDATED_INGRESS=$(python3 -c "
import json, sys
rules = json.loads('''$CURRENT_INGRESS''')
ports = [80, 443, 3000, 3005, 5000, 8089]
existing = set()
for r in rules:
    if r.get('protocol') == '6' and 'tcp-options' in r:
        rng = r['tcp-options'].get('destination-port-range')
        if rng:
            existing.add(rng.get('min'))
for p in ports:
    if p not in existing:
        rules.append({
            'is-stateless': False,
            'protocol': '6',
            'source': '0.0.0.0/0',
            'source-type': 'CIDR_BLOCK',
            'tcp-options': {'destination-port-range': {'min': p, 'max': p}}
        })
print(json.dumps(rules))
")

oci network security-list update \
  --security-list-id "$SEC_LIST_ID" \
  --ingress-security-rules "$UPDATED_INGRESS" \
  --force > /dev/null

# 4. Fetch Latest Canonical Ubuntu 24.04 ARM64 Image
echo "[4/7] Finding latest Ubuntu 24.04 ARM64 image..."
IMAGE_ID=$(oci compute image list \
  --compartment-id "$TENANCY_ID" \
  --operating-system "Canonical Ubuntu" \
  --operating-system-version "24.04" \
  --shape "VM.Standard.A1.Flex" \
  --sort-by TIMECREATED --sort-order DESC \
  --query "data[0].id" --raw-output)

# 5. Provision Always Free ARM VM (Loop across Availability Domains for capacity)
echo "[5/7] Provisioning Always Free VM ($INSTANCE_NAME: 2 OCPUs, 12 GB RAM)..."
INSTANCE_ID=$(oci compute instance list \
  --compartment-id "$TENANCY_ID" \
  --display-name "$INSTANCE_NAME" \
  --lifecycle-state RUNNING \
  --query "data[0].id" --raw-output 2>/dev/null || true)

if [ -n "$INSTANCE_ID" ] && [ "$INSTANCE_ID" != "null" ]; then
  echo "[INFO] Existing instance $INSTANCE_NAME is already RUNNING ($INSTANCE_ID)."
else
  ADS=$(oci iam availability-domain list --compartment-id "$TENANCY_ID" --query "data[*].name" --raw-output | tr -d '[],"' | xargs)
  LAUNCHED=false

  for AD in $ADS; do
    echo "   -> Trying Availability Domain: $AD ..."
    if OUTPUT=$(oci compute instance launch \
      --availability-domain "$AD" \
      --compartment-id "$TENANCY_ID" \
      --shape "VM.Standard.A1.Flex" \
      --shape-config '{"ocpus": 2, "memoryInGBs": 12}' \
      --image-id "$IMAGE_ID" \
      --subnet-id "$SUBNET_ID" \
      --assign-public-ip true \
      --ssh-authorized-keys-file "$SSH_KEY_PUB" \
      --display-name "$INSTANCE_NAME" \
      --wait-for-state RUNNING 2>&1); then
      INSTANCE_ID=$(echo "$OUTPUT" | python3 -c "import sys, json; print(json.load(sys.stdin)['data']['id'])")
      LAUNCHED=true
      echo "[SUCCESS] VM launched successfully in $AD! Instance ID: $INSTANCE_ID"
      break
    else
      echo "   [WARN] Capacity unavailable in $AD, trying next AD..."
    fi
  done

  if [ "$LAUNCHED" = false ]; then
    echo "[ERROR] Out of host capacity in all ADs. Try again in a few minutes or switch account to PAYG (billing stays ₹0 at 2 OCPU/12GB)."
    exit 1
  fi
fi

# 6. Retrieve Public IP & Wait for SSH
echo "[6/7] Retrieving Public IP and verifying SSH connectivity..."
VNIC_ID=$(oci compute instance list-vnics --instance-id "$INSTANCE_ID" --query "data[0].id" --raw-output)
PUBLIC_IP=$(oci network vnic get --vnic-id "$VNIC_ID" --query "data.\"public-ip\"" --raw-output)
echo "[INFO] Instance Public IP: $PUBLIC_IP"

echo "[INFO] Waiting for SSH daemon to become ready on $PUBLIC_IP..."
until ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i "$SSH_KEY_PRIV" ubuntu@"$PUBLIC_IP" "echo SSH_READY" &> /dev/null; do
  sleep 4
done

# 7. Sync Code, Deploy Docker Stack & Install Keepalive Cron Job
echo "[7/7] Syncing project files and starting Docker Compose stack..."
rsync -avz --delete \
  --exclude 'target/' \
  --exclude '.git/' \
  --exclude '.mvn/' \
  -e "ssh -o StrictHostKeyChecking=no -i $SSH_KEY_PRIV" \
  "$PROJECT_DIR/" ubuntu@"$PUBLIC_IP":~/payment-ledger-engine/

ssh -o StrictHostKeyChecking=no -i "$SSH_KEY_PRIV" ubuntu@"$PUBLIC_IP" << 'EOF'
  set -e
  if ! command -v docker &> /dev/null; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq docker.io docker-compose-v2 iptables-persistent uuid-runtime curl jq
    sudo usermod -aG docker $USER
  fi

  # Open OS-level Ubuntu iptables rules
  for PORT in 80 443 3000 3005 5000 8089 9090; do
    sudo iptables -C INPUT -p tcp --dport $PORT -j ACCEPT 2>/dev/null || \
      sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport $PORT -j ACCEPT
  done
  sudo netfilter-persistent save > /dev/null 2>&1 || true

  # Install 24/7 Anti-Idle Keepalive Cron Daemon
  sudo chmod +x ~/payment-ledger-engine/scripts/keepalive-traffic.sh
  sudo cp ~/payment-ledger-engine/scripts/keepalive-traffic.sh /usr/local/bin/keepalive-traffic.sh
  echo "* * * * * root /usr/local/bin/keepalive-traffic.sh >/dev/null 2>&1" | sudo tee /etc/cron.d/ledger-keepalive > /dev/null
  sudo chmod 0644 /etc/cron.d/ledger-keepalive

  cd ~/payment-ledger-engine
  sudo docker compose up -d --build
  nohup python3 -u server.py > server.log 2>&1 &
EOF

echo "=============================================================================="
echo "🎉 DEPLOYMENT COMPLETE! PAYMENT LEDGER IS LIVE ON ORACLE CLOUD"
echo "=============================================================================="
echo "👉 Interactive Workbench: http://${PUBLIC_IP}:5000"
echo "👉 Payment Ingress API:   http://${PUBLIC_IP}:8088/api/v1/transfers"
echo "👉 Grafana Dashboards:    http://${PUBLIC_IP}:3005 (admin / admin)"
echo "👉 Kafbat Kafka UI:       http://${PUBLIC_IP}:8089"
echo "👉 Prometheus Engine:     http://${PUBLIC_IP}:9090"
echo "👉 24/7 Anti-Idle Cron:   Active (/etc/cron.d/ledger-keepalive)"
echo "=============================================================================="
