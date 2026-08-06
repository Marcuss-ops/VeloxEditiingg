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
- **One-time seed command.** The static `deploy/ansible/inventory.ini` is
  now a seed/ops reference only.

## One-time migration from the legacy inventory

If the DB was provisioned before Phase 9 (or a new node was added to
`deploy/ansible/inventory.ini`), seed the registry:

```bash
velox-admin sync-worker-nodes \
    --db /var/lib/velox-server/velox.db \
    --inventory deploy/ansible/inventory.ini \
    --apply
```

`--dry-run` (default per convention) parses and reports without writing.
Rows are imported only when they carry both `worker_id` and `ansible_host`;
the alias is preserved as `linked_worker_id`, `host_group` becomes the
`Environment`. Existing operator-side fields (availability, tags, notes,
secret_ref) are preserved on re-run.

After seeding, verify:

```bash
velox-admin sync-worker-nodes --db /var/lib/velox-server/velox.db \
    --inventory deploy/ansible/inventory.ini --dry-run
```

and confirm the boot log line:

```
[BOOTSTRAP] WorkerNodeRegistry: loaded N worker nodes from persistent inventory
```

## Onboarding a new node

1. Provision the host (key auth, sudo, worker runtime):
   `ansible-playbook -i deploy/ansible/inventory.ini deploy/playbooks/bootstrap-ssh.yml --limit <alias>`
2. Add the node to the inventory file (seed reference).
3. Re-run `velox-admin sync-worker-nodes --apply`.
4. Restart the master (or wait for the next boot) so the WorkerRegistry
   picks up the new node.
