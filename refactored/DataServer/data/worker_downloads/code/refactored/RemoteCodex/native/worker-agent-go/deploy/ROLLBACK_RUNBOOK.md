# Velox Worker - Deploy & Rollback Runbook

This runbook describes operational procedures for deployment, verification, and rollback of the Velox Go worker agent.

---

## Index

- [Prerequisites](#prerequisites)
- [Deployment Procedure](#deployment-procedure)
- [Verify Procedure](#verify-procedure)
- [Rollback Procedure](#rollback-procedure)
- [Troubleshooting](#troubleshooting)
- [Operational Checklist](#operational-checklist)

---

## Prerequisites

### Before Deployment

1. **Gate tests passed**:
   ```bash
   cd /home/pierone/Pyt/VeloxEditing/refactored/RemoteCodex/native/worker-agent-go
   go test ./... -count=1
   go test ./internal/tests/contract ./internal/tests/smoke -count=1
   ```

2. **Build completed**:
   ```bash
   make agent
   ```

3. **Backup existing binary** (if present):
   ```bash
   cp /usr/local/bin/velox-worker-agent /opt/velox/backups/velox-worker-agent.pre-deploy
   ```

4. **Master reachable**:
   ```bash
   curl -sf http://MASTER_URL:PORT/health
   ```

---

## Deployment Procedure

### Step 1: Install Worker

```bash
# Dry run first
sudo ./deploy/install-worker.sh \
    --master http://MASTER_URL:PORT \
    --work-dir /opt/velox \
    --dry-run

# Real install
sudo ./deploy/install-worker.sh \
    --master http://MASTER_URL:PORT \
    --work-dir /opt/velox
```

### Step 2: Restart Service

```bash
sudo systemctl restart velox-worker

# Verify status
sudo systemctl status velox-worker
```

### Step 3: Verify Master Connectivity

```bash
# Watch logs for confirmation
journalctl -u velox-worker -f

# Expected output:
# "Velox Worker Agent Go started. Master: http://... | WorkDir: /opt/velox"
# "Worker registered successfully"
# "Heartbeat sent successfully"
```

### Step 4: Verify Heartbeat

```bash
# From the master side, verify the worker is registered
curl http://MASTER_URL:PORT/api/workers

# Output should show the worker with:
# - status: "idle" or "working"
# - last_heartbeat: recent timestamp
# - connection_status: "on"
```

---

## Verify Procedure

### Post-Deployment Checklist (within 5 minutes)

- [ ] Worker registered on master
- [ ] Heartbeat regular (every 30s default)
- [ ] Logs without critical errors
- [ ] Process active and stable

### Mandatory Runtime Verification

#### 1. Register → Heartbeat → Get_Job → Complete_Job

```bash
# Verify via logs or master dashboard
# The worker must:
# 1. Be registered
# 2. Send regular heartbeats
# 3. Accept a job
# 4. Complete the job successfully
```

#### 2. Register → Heartbeat → Get_Job → Fail_Job

```bash
# Send a job that fails deliberately
# Verify that the worker:
# 1. Reports the error correctly
# 2. Continues to function
# 3. Sends subsequent heartbeats
```

#### 3. restart_worker Command

```bash
# From master, send restart_worker command
curl -X POST http://MASTER_URL:PORT/api/workers/WORKER_ID/commands \
    -d '{"command": "restart_worker"}'

# Verify that the worker:
# 1. Receives the command
# 2. Sends ACK
# 3. Restarts
# 4. Re-registers
```

#### 4. Heartbeat Continuity During Long Jobs

```bash
# Run a job that lasts >= 3 minutes
# Verify that:
# 1. Heartbeat continues during the job
# 2. Worker does not timeout
# 3. Job completes successfully
```

### Automatic Verification Script

```bash
#!/bin/bash
# verify-worker.sh

MASTER_URL="http://localhost:8080"
WORKER_ID="$1"

if [[ -z "$WORKER_ID" ]]; then
    echo "Usage: $0 WORKER_ID"
    exit 1
fi

echo "=== Verifying Worker $WORKER_ID ==="

# 1. Check registration
echo -n "Registration: "
REG_STATUS=$(curl -sf "$MASTER_URL/api/workers/$WORKER_ID" | jq -r '.status // "unknown"')
echo "$REG_STATUS"

# 2. Check heartbeat (wait up to 60s)
echo -n "Heartbeat: "
for i in {1..12}; do
    LAST_HB=$(curl -sf "$MASTER_URL/api/workers/$WORKER_ID" | jq -r '.last_heartbeat // empty')
    if [[ -n "$LAST_HB" ]]; then
        echo "OK ($LAST_HB)"
        break
    fi
    sleep 5
done

# 3. Check service status
echo -n "Service: "
systemctl is-active velox-worker

# 4. Check recent errors
echo "Recent Errors:"
journalctl -u velox-worker --since "5 minutes ago" | grep -i error || echo "None"

echo "=== Verification Complete ==="
```

---

## Rollback Procedure

Rollback restores a previous Go binary version. All workers run Docker/Go — there is no Python fallback path.

### Quick Rollback (< 5 minutes)

#### Option 1: Rollback via Script

```bash
sudo ./deploy/rollback-worker.sh --force
```

#### Option 2: Manual Rollback

```bash
# 1. Stop service
sudo systemctl stop velox-worker

# 2. Restore previous binary (if backup exists)
LATEST_BACKUP=$(ls -t /opt/velox/backups/velox-worker-agent.* | head -1)
if [[ -n "$LATEST_BACKUP" ]]; then
    sudo cp "$LATEST_BACKUP" /usr/local/bin/velox-worker-agent
fi

# 3. Restart
sudo systemctl start velox-worker
```

### Rollback to Specific Version

```bash
# List available backups
ls -lt /opt/velox/backups/velox-worker-agent.*

# Rollback to N-th most recent backup
sudo ./deploy/rollback-worker.sh --version 2 --force
```

### Post-Rollback Verification

```bash
# Verify service is running
sudo systemctl status velox-worker

# Verify worker re-registers with master
journalctl -u velox-worker -f --since "2 minutes ago"

# Verify master sees the worker
curl http://MASTER_URL:PORT/api/workers | jq '.[] | select(.worker_id == "WORKER_ID")'
```

---

## Troubleshooting

### Worker does not register

**Symptoms**: Logs show connection errors to master

**Solution**:
```bash
# Verify connectivity
curl -sf http://MASTER_URL:PORT/health

# Verify firewall
sudo iptables -L -n | grep PORT

# Verify configuration
cat /opt/velox/worker_config.json
```

### Heartbeat not sent

**Symptoms**: Worker appears "stale" on master

**Solution**:
```bash
# Verify worker is running
systemctl status velox-worker

# Check logs for errors
journalctl -u velox-worker -n 100

# Verify no blocking in the loop
# (worker may be stuck on a job)
```

### Job not completed

**Symptoms**: Job remains in "running" state

**Solution**:
```bash
# Check worker logs
journalctl -u velox-worker --since "10 minutes ago"

# If worker is stuck, restart
sudo systemctl restart velox-worker

# Job should be marked as failed by master
```

### Rollback failed

**Symptoms**: Worker does not start after rollback

**Solution**:
```bash
# Check if the restored binary exists and is executable
ls -la /usr/local/bin/velox-worker-agent

# Check service logs
journalctl -u velox-worker -n 50

# Try manual start for debugging
/usr/local/bin/velox-worker-agent -config /opt/velox/worker_config.json -master MASTER_URL -work-dir /opt/velox
```

---

## Operational Checklist

### Pre-Deployment

- [ ] Gate tests green
- [ ] Build completed
- [ ] Binary backup created
- [ ] Master reachable

### Post-Deployment

- [ ] Worker Go registered
- [ ] Heartbeat regular
- [ ] Test job completed
- [ ] Logs without critical errors
- [ ] Rollback tested (< 5 min)

### Post-Real-Job

- [ ] 1 real job completed successfully
- [ ] Valid video output
- [ ] Master received complete_job
- [ ] Worker ready for next job

---

## Contacts & Escalation

In case of critical unresolved issues:

1. **Immediate rollback** to previous Go binary version
2. **Notify** DevOps team
3. **Document** the error in logs

---

## Related Files

| File | Description |
|------|-------------|
| `deploy/install-worker.sh` | Installation script |
| `deploy/rollback-worker.sh` | Rollback script (Go binary versions) |
| `cmd/velox-worker-agent/main.go` | Go entry point |
