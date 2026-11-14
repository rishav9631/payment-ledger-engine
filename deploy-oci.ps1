<#
.SYNOPSIS
  Automated Oracle Cloud Always Free (ARM64) Provision & Deploy Script for Windows PowerShell
  Target: payment-ledger-engine (2 OCPUs, 12 GB RAM - VM.Standard.A1.Flex)
#>

$ErrorActionPreference = "Stop"

$InstanceName = "payment-ledger-prod"
$SshKeyPub = "$env:USERPROFILE\.ssh\id_rsa.pub"
$SshKeyPriv = "$env:USERPROFILE\.ssh\id_rsa"
$ProjectDir = $PSScriptRoot

# 1. Resolve OCI CLI path
$OciExe = Get-Command "oci" -ErrorAction SilentlyContinue
$OciCmd = "oci"
if ($OciExe) {
    $OciCmd = "oci"
} elseif (Test-Path "$PSScriptRoot\oci.bat") {
    $OciCmd = "$PSScriptRoot\oci.bat"
} else {
    Write-Host "[ERROR] OCI CLI not found." -ForegroundColor Red
    exit 1
}

# 2. Verify OCI Authentication
Write-Host "[1/7] Verifying OCI CLI authentication..." -ForegroundColor Cyan
if (-not (Test-Path "$env:USERPROFILE\.oci\config")) {
    Write-Host "[INFO] No active OCI config found at ~/.oci/config." -ForegroundColor Yellow
    Write-Host "[ACTION REQUIRED] Launching 1-click browser login via OCI session authenticate..." -ForegroundColor Yellow
    Write-Host "Please sign in to your Oracle Cloud account in the browser window and choose your home region." -ForegroundColor Yellow
    & $OciCmd session authenticate
}

# 3. Ensure SSH Key exists
if (-not (Test-Path $SshKeyPub)) {
    Write-Host "[INFO] Generating SSH key pair at $SshKeyPub..." -ForegroundColor Yellow
    if (-not (Test-Path "$env:USERPROFILE\.ssh")) { New-Item -ItemType Directory -Path "$env:USERPROFILE\.ssh" | Out-Null }
    ssh-keygen -t rsa -b 4096 -f $SshKeyPriv -N ""
}

# 4. Discover Tenancy and Subnet
Write-Host "[2/7] Fetching OCI Tenancy and Subnet..." -ForegroundColor Cyan
$TenancyId = (& $OciCmd iam compartment list --include-root --query "data[?contains(\"id\", 'tenancy')].id | [0]" --raw-output 2>$null)
if (-not $TenancyId -or $TenancyId -eq "null") {
    $TenancyId = (& $OciCmd iam compartment list --query "data[0].id" --raw-output)
}

$SubnetJson = (& $OciCmd network subnet list --compartment-id $TenancyId --query "data[0]" --raw-output 2>$null | ConvertFrom-Json)
if (-not $SubnetJson) {
    Write-Host "[ERROR] No public subnet found in compartment. Please create a default VCN with Internet Connectivity in OCI Console." -ForegroundColor Red
    exit 1
}

$SubnetId = $SubnetJson.id
$SecListId = $SubnetJson."security-list-ids"[0]

# 5. Open Ingress Ports in VCN Security List
Write-Host "[3/7] Updating VCN Security List ($SecListId) for ports 80, 443, 3000, 3005, 5000, 8089, 9090..." -ForegroundColor Cyan
$CurrentIngressJson = (& $OciCmd network security-list get --security-list-id $SecListId --query "data.`"ingress-security-rules`"" --raw-output)
$Rules = $CurrentIngressJson | ConvertFrom-Json
$ExistingPorts = @()
foreach ($r in $Rules) {
    if ($r.protocol -eq "6" -and $r."tcp-options") {
        $ExistingPorts += $r."tcp-options"."destination-port-range".min
    }
}

$PortsToAdd = @(80, 443, 3000, 3005, 5000, 8089, 9090)
foreach ($p in $PortsToAdd) {
    if ($ExistingPorts -notcontains $p) {
        $Rules += [PSCustomObject]@{
            "is-stateless" = $false
            "protocol" = "6"
            "source" = "0.0.0.0/0"
            "source-type" = "CIDR_BLOCK"
            "tcp-options" = [PSCustomObject]@{
                "destination-port-range" = [PSCustomObject]@{ "min" = $p; "max" = $p }
            }
        }
    }
}

$UpdatedRulesJson = ($Rules | ConvertTo-Json -Depth 10 -Compress)
& $OciCmd network security-list update --security-list-id $SecListId --ingress-security-rules $UpdatedRulesJson --force | Out-Null

# 6. Fetch Latest Ubuntu 24.04 ARM64 Image
Write-Host "[4/7] Finding latest Ubuntu 24.04 ARM64 image..." -ForegroundColor Cyan
$ImageId = (& $OciCmd compute image list --compartment-id $TenancyId --operating-system "Canonical Ubuntu" --operating-system-version "24.04" --shape "VM.Standard.A1.Flex" --sort-by TIMECREATED --sort-order DESC --query "data[0].id" --raw-output)

# 7. Provision Always Free ARM VM
Write-Host "[5/7] Provisioning Always Free VM ($($InstanceName) - 2 OCPUs, 12 GB RAM)..." -ForegroundColor Cyan
$InstanceId = (& $OciCmd compute instance list --compartment-id $TenancyId --display-name $InstanceName --lifecycle-state RUNNING --query "data[0].id" --raw-output 2>$null)

if ($InstanceId -and $InstanceId -ne "null") {
    Write-Host "[INFO] Existing instance $InstanceName is already RUNNING ($InstanceId)." -ForegroundColor Green
} else {
    $AdsRaw = (& $OciCmd iam availability-domain list --compartment-id $TenancyId --query "data[*].name" --raw-output | ConvertFrom-Json)
    $Launched = $false

    foreach ($ad in $AdsRaw) {
        Write-Host "   -> Trying Availability Domain: $ad ..." -ForegroundColor Yellow
        try {
            $LaunchOutput = (& $OciCmd compute instance launch `
                --availability-domain $ad `
                --compartment-id $TenancyId `
                --shape "VM.Standard.A1.Flex" `
                --shape-config '{"ocpus": 2, "memoryInGBs": 12}' `
                --image-id $ImageId `
                --subnet-id $SubnetId `
                --assign-public-ip true `
                --ssh-authorized-keys-file $SshKeyPub `
                --display-name $InstanceName `
                --wait-for-state RUNNING 2>&1)
            
            $LaunchObj = $LaunchOutput | ConvertFrom-Json
            $InstanceId = $LaunchObj.data.id
            $Launched = $true
            Write-Host "[SUCCESS] VM launched in $ad! Instance ID: $InstanceId" -ForegroundColor Green
            break
        } catch {
            Write-Host "   [WARN] Capacity unavailable in $ad, trying next AD..." -ForegroundColor DarkYellow
        }
    }

    if (-not $Launched) {
        Write-Host "[ERROR] Out of host capacity in all ADs. Try again in a few minutes or switch account to PAYG." -ForegroundColor Red
        exit 1
    }
}

# 8. Retrieve Public IP & Verify SSH
Write-Host "[6/7] Retrieving Public IP and verifying SSH connectivity..." -ForegroundColor Cyan
$VnicId = (& $OciCmd compute instance list-vnics --instance-id $InstanceId --query "data[0].id" --raw-output)
$PublicIp = (& $OciCmd network vnic get --vnic-id $VnicId --query "data.`"public-ip`"" --raw-output)
Write-Host "[INFO] Instance Public IP: $PublicIp" -ForegroundColor Green

Write-Host "[INFO] Waiting for SSH daemon on $PublicIp..." -ForegroundColor Yellow
$Ready = $false
for ($i = 0; $i -lt 30; $i++) {
    $res = ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i $SshKeyPriv ubuntu@$PublicIp "echo SSH_READY" 2>$null
    if ($res -match "SSH_READY") { $Ready = $true; break }
    Start-Sleep -Seconds 4
}

# 9. Sync Files and Start Docker
Write-Host "[7/7] Transferring project files and configuring remote VM..." -ForegroundColor Cyan
ssh -o StrictHostKeyChecking=no -i $SshKeyPriv ubuntu@$PublicIp "mkdir -p ~/payment-ledger-engine"

# Archive and transfer project excluding unnecessary folders
$TarFile = "$env:TEMP\payment-ledger.tar.gz"
tar --exclude="target" --exclude=".git" --exclude=".mvn" -czf $TarFile -C $ProjectDir .
scp -o StrictHostKeyChecking=no -i $SshKeyPriv $TarFile "ubuntu@${PublicIp}:~/payment-ledger.tar.gz"
Remove-Item $TarFile -Force

$RemoteScript = @"
set -e
tar -xzf ~/payment-ledger.tar.gz -C ~/payment-ledger-engine
rm ~/payment-ledger.tar.gz

if ! command -v docker &> /dev/null; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq docker.io docker-compose-v2 iptables-persistent uuid-runtime curl jq python3
  sudo usermod -aG docker `$USER
fi

for PORT in 80 443 3000 3005 5000 8089 9090; do
  sudo iptables -C INPUT -p tcp --dport `$PORT -j ACCEPT 2>/dev/null || \
    sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport `$PORT -j ACCEPT
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
"@

ssh -o StrictHostKeyChecking=no -i $SshKeyPriv ubuntu@$PublicIp $RemoteScript

Write-Host "==============================================================================" -ForegroundColor Green
Write-Host "🎉 DEPLOYMENT COMPLETE! PAYMENT LEDGER IS LIVE ON ORACLE CLOUD" -ForegroundColor Green
Write-Host "==============================================================================" -ForegroundColor Green
Write-Host "👉 Interactive Workbench: http://${PublicIp}:5000" -ForegroundColor Cyan
Write-Host "👉 Payment Ingress API:   http://${PublicIp}:8088/api/v1/transfers" -ForegroundColor Cyan
Write-Host "👉 Grafana Dashboards:    http://${PublicIp}:3005 (admin / admin)" -ForegroundColor Cyan
Write-Host "👉 Kafbat Kafka UI:       http://${PublicIp}:8089" -ForegroundColor Cyan
Write-Host "👉 Prometheus Engine:     http://${PublicIp}:9090" -ForegroundColor Cyan
Write-Host "👉 24/7 Anti-Idle Cron:   Active (/etc/cron.d/ledger-keepalive)" -ForegroundColor Cyan
Write-Host "==============================================================================" -ForegroundColor Green
