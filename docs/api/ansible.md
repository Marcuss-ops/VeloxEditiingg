# Ansible API — compatibility bridge

> **Production rollout boundary:** these endpoints are retained for worker
> migration, host convergence, preflight, and diagnostics. They are not the
> canonical production path for image updates or rollbacks. Use
> `scripts/fleetctl`, which publishes to the Master FleetController and lets
> `UpdateExecutor` resolve `WorkerNodeRegistry` → SSH →
> `velox-worker-activate-image`.
>
> The endpoints below generate per-operation inventory from the
> `WorkerNodeRegistry`/`ansible_hosts` database view. They do not use a static
> `inventory.ini`; do not run their playbooks fleet-wide as a release mechanism.
>
> All endpoints require admin authentication.

## POST /api/v1/admin/ansible/computers/run_action

Execute an Ansible action on target computers.

### Request body

```json
{
  "action": "deploy_workers",
  "targets": ["149.56.131.97", "57.129.132.133"]
}
```

### Supported actions

- `deploy_workers` - Legacy migration deployment for target hosts
- `rollout_update` - Compatibility-bridge rollout for legacy hosts
- `install_workers` - Fresh install during worker migration
- `preflight_workers` - Read-only checks (SSH, Docker, disk)
- `update_workers` - Legacy bridge update; not a certified release rollout
- `test_ssh` - Read-only SSH connectivity test

### Response

```json
{
  "run_id": "run_abc123",
  "action": "deploy_workers",
  "targets": ["149.56.131.97"],
  "status": "started"
}
```

## GET /api/v1/admin/ansible/runs

List all Ansible runs.

### Response

```json
{
  "runs": [
    {
      "run_id": "run_abc123",
      "action": "deploy_workers",
      "status": "completed",
      "started_at": "...",
      "finished_at": "..."
    }
  ]
}
```

## GET /api/v1/admin/ansible/runs/:id

Get details of a specific Ansible run.

## POST /api/v1/admin/ansible/computers/run_shell

Run a shell command on target hosts via SSH.

## POST /api/v1/admin/ansible/computers/test_ssh

Test SSH connectivity to target hosts.

## GET /api/v1/admin/ansible/capabilities

Check if Ansible is available and get capabilities.

## GET /api/v1/admin/ansible/computers/list

List all managed computers from inventory.

## GET /api/v1/admin/ansible/computers/summary

Summary of managed computers (online/offline counts).

## POST /api/v1/admin/ansible/computers

Save/update computer inventory.

## DELETE /api/v1/admin/ansible/computers/:id

Remove a computer from inventory.

## GET /api/v1/admin/ansible/computers/logs/:id

Get logs for a specific computer.
