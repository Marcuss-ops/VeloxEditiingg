# Credential registry — operational source of truth

This registry records ownership and materialization paths only. It must never
contain a credential value, hash, vault password, or private key.

## Canonical ownership

| Credential | Canonical source | Materialization | Runtime consumer |
| --- | --- | --- | --- |
| Master admin token | OpenBao `velox/production/master/admin-token` | `/etc/velox-server.env` as `VELOX_ADMIN_TOKEN` | Master admin middleware and operator clients |
| Worker credential | OpenBao entry keyed by canonical `worker_id` | `/etc/velox-worker/worker.env` as `VELOX_WORKER_SECRET` | Worker agent and Master `worker_credentials` |
| Worker mTLS key/certificate | OpenBao PKI CSR workflow keyed by canonical `worker_id` | `/etc/velox-worker/certs/current/worker.{key,crt}` and `ca.crt`; key generated locally | Worker gRPC control plane |
| SSH operator identity | OpenBao SSH CA; `authorized_keys` is transitional fallback | Host SSH configuration | `velox-deploy` access |

## Canonical fleet identity

The stable `worker_id` is the join key across all systems:

```text
worker_id
  ├── Master worker registry / worker_credentials
  ├── WorkerNodeRegistry.ansible_hosts  (single inventory source)
  ├── OpenBao secret path: velox/production/workers/<worker_id>/credential
  └── worker certificate identity (CN/SAN; CSR signed by OpenBao PKI)
```

Host alias, IP address, certificate filename, and Vault field name are not
independent identities. They must resolve to the same `worker_id`.

## Fleet identity lifecycle

1. Insert the node into `ansible_hosts` (the `WorkerNodeRegistry` — the single
   inventory source) via `POST /api/v1/admin/ansible/computers` or SQLite.
2. Provision host + OpenBao AppRole material:
   `scripts/operator/provision-worker-openbao.sh --worker <id> --ssh-host <ip> --ssh-user <user>`.
3. The Master generates any per-operation inventory from the DB
   (`AnsibleComputerManager.GenerateInventory`); there is no static inventory
   file, template, or one-time sync.

Do not edit a second inventory to change runtime state — the DB row is the
only source.

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
