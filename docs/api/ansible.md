# Ansible API — compatibility bridge

> **Production rollout boundary:** Ansible deployment actions are retired and
> fail closed. Use `fleetctl`, which publishes to Master FleetController and
> lets `UpdateExecutor` resolve `WorkerNodeRegistry` → restricted SSH →
> `velox-worker-activate-image`.
>
> The endpoints below generate per-operation inventory from the
> `WorkerNodeRegistry`/`ansible_hosts` database view. They do not use a static
> `inventory.ini`; do not run their playbooks fleet-wide as a release mechanism.
>
> All endpoints require admin authentication.

## POST /api/v1/admin/ansible/computers/run_action

Retained compatibility endpoint. Deployment actions return `501 Not
Implemented`; no Ansible subprocess or remote rollout is started.

### Request body

```json
{
  "action": "deploy_workers",
  "targets": ["149.56.131.97", "57.129.132.133"]
}
```

### Actions

There are no supported deployment actions. Use `fleetctl ssh-check` for
connectivity and `fleetctl update <worker_id> <ghcr-image@sha256:digest>` for
release activation.

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
