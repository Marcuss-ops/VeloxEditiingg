# WorkerNodeRegistry — persistent fleet inventory (Phase 9)

## Single source of truth

Fleet connectivity lives in **one place**: the `ansible_hosts` table in the
master SQLite DB, exposed through the canonical `WorkerNode` view
(`store.ListWorkerNodes` in `DataServer/internal/store/worker_nodes.go`).

| Concern | Source |
| --- | --- |
| Node connectivity (host, ssh user, secret ref, enabled) | `ansible_hosts` DB (migration 004) |
| Canonical fleet registry (in-memory, built at boot) | `fleet.WorkerRegistry` ← `ListWorkerNodes()` |
| SSH surface (health probes A/B, smoke D) | `fleet.NewSSHClientFromRegistry` |
| Per-operation Ansible inventory | `AnsibleComputerManager.GenerateInventory` ← `ansible_hosts` |

The runtime worker registry (`/api/v1/workers` — connected sessions)
continues to describe **active sessions**; it is NOT infrastructure
inventory. Node connectivity is exclusively the DB.

## What changed (Phase 9)

- **No more hardcoded SSH targets.** The composition root previously
  embedded a `map[string]SSHWorkerTarget` with real IPs. That map is gone;
  `buildWorkerRegistryFromStore` populates the `fleet.WorkerRegistry` from
  `ListWorkerNodes()`, and `NewSSHClientFromRegistry` derives the client
  from it. An unseeded inventory fails per-target at Run time with a clear
  error instead of silently carrying stale IPs.
- **Canonical view.** `ListWorkerNodes()` returns only `enabled=1` rows
  with a non-empty `worker_id` — a disabled or unmapped host is not a
  schedulable node and never enters the SSH registry.
  `ListWorkerNodesWithDisabled()` exists for audit tooling.
- **One-time seed command.** The static `deploy/ansible/inventory.ini` and the
  `velox-admin sync-worker-nodes` migration command have been **retired**: the
  DB is the single registry and rows are inserted directly (see below).

## Registering nodes (no static inventory)

Insert rows directly into `ansible_hosts` with both `worker_id` and
`ansible_host` set — for example via `POST /api/v1/admin/ansible/computers`
or SQLite against `/var/lib/velox-server/velox.db`. Rows are canonical only
when they carry both `worker_id` and `ansible_host`; the alias is preserved as
`linked_worker_id`, `host_group` becomes the `Environment`. Existing
operator-side fields (availability, tags, notes, secret_ref) are preserved on
update.

Verify with the boot log line:

```
[BOOTSTRAP] WorkerNodeRegistry: loaded N worker nodes from persistent inventory
```

## Onboarding a new node

1. Insert the node into `ansible_hosts` (`POST /api/v1/admin/ansible/computers`
   or SQLite) with `worker_id` and `ansible_host` set.
2. Provision the OpenBao AppRole material + worker credential on the host:
   `scripts/operator/provision-worker-openbao.sh --worker <worker_id> --ssh-host <ip> --ssh-user <user>`.
3. Install the worker runtime on the host (`prepare-host.sh` /
   `velox-worker-activate-image` install the canonical unit and the mTLS renew).
4. Restart the master (or wait for the next boot) so the WorkerRegistry
   picks up the new node.
