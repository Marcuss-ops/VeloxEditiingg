# Credential registry — operational source of truth

This registry records ownership and materialization paths only. It must never
contain a credential value, hash, vault password, or private key.

## Canonical ownership

| Credential | Canonical source | Materialization | Runtime consumer |
| --- | --- | --- | --- |
| Master admin token | OpenBao `velox/production/master/admin-token`; Ansible Vault is the fallback during migration | `/etc/velox-server.env` as `VELOX_ADMIN_TOKEN` | Master admin middleware and operator clients |
| Worker credential | One OpenBao/Ansible-Vault entry keyed by canonical `worker_id` | `/etc/velox-worker/worker.env` as `VELOX_WORKER_SECRET` | Worker agent and Master `worker_credentials` |
| Worker mTLS key/certificate | OpenBao PKI CSR workflow keyed by canonical `worker_id` | `/etc/velox-worker/certs/current/worker.{key,crt}` and `ca.crt`; key generated locally | Worker gRPC control plane |
| SSH operator identity | OpenBao SSH CA; `authorized_keys` is transitional fallback | Host SSH configuration | `velox-deploy` access |

## Canonical fleet identity

The stable `worker_id` is the join key across all systems:

```text
worker_id
  ├── Master worker registry / worker_credentials
  ├── WorkerNodeRegistry.ansible_hosts
  ├── Ansible seed inventory host
  ├── OpenBao secret path: velox/production/workers/<worker_id>/credential
  └── worker certificate identity (CN/SAN; CSR signed by OpenBao PKI)
```

Host alias, IP address, certificate filename, and Vault field name are not
independent identities. They must resolve to the same `worker_id`.

## Inventory lifecycle

1. Copy `deploy/ansible/inventory.ini.example` to the ignored local
   `deploy/ansible/inventory.ini`.
2. Validate the seed inventory and provision the host.
3. Run `velox-admin sync-worker-nodes --dry-run`, review the diff, then run
   `--apply` against the Master DB.
4. Use the DB-backed `WorkerNodeRegistry` for runtime health, smoke, and SSH
   operations. Do not edit a second inventory to change runtime state.

Existing operator copies of `deploy/ansible/inventory.ini` remain usable from
the worktree, but the file is no longer versioned. New templates must not be
added elsewhere.

## Worker credential repair rule

For a mismatch, compare only metadata and references first; never print the
secret. If the canonical source still exists, rematerialize it to the worker
and leave the Master credential unchanged. Rotate only when the canonical
source is unavailable, updating source, Master record, and worker file in one
change window.

Legacy `/etc/velox-worker/secrets/worker_credential` is migration input only.
The canonical runtime value is `VELOX_WORKER_SECRET` in
`/etc/velox-worker/worker.env`; clean up the legacy file only after the
migration marker and a successful authenticated health check are present.
