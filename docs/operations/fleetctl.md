# Fleet Operations

> **Path selection:** read `docs/operations/worker-rollout-paths.md` before
> changing a worker. The definitive production path is the FleetController API;
> the bundle/Ansible path is retained only as a server-side migration bridge for
> workers that have not yet reached the canonical runtime. Do not treat the
> bridge and canonical commands as interchangeable.

`scripts/fleetctl` is the operator facade for the worker fleet. It talks to
the Master with the admin bearer token and never prints that token.

## Read and control

```bash
export VELOX_MASTER_URL="http://MASTER:8000"
export VELOX_ADMIN_TOKEN="..."

scripts/fleetctl status
scripts/fleetctl inspect worker-id
scripts/fleetctl inspect --json worker-id   # machine-readable WorkerCard
scripts/fleetctl drain worker-id "prepare deployment"
scripts/fleetctl resume worker-id "deployment complete"
scripts/fleetctl quarantine worker-id "asset failures"
scripts/fleetctl operations worker-id
```

### Inspect: IMAGE + LAST UPDATE OPERATION

`fleetctl inspect <worker_id>` renders the worker card as two canonical
sections — image state and rollout history are deliberately separate views:

```text
IMAGE
  running_digest = sha256:337...
  target_digest  = sha256:337...
  digest_match   = true

LAST UPDATE OPERATION
  status       = FAILED
  reason       = connection reset by peer
  operation_id = op_2026-08-08_...
  type         = update
  started_at   = 2026-08-08T10:11:12Z
  finished_at  = 2026-08-08T10:11:19Z
```

A worker whose digests match is healthy even when the last rollout
operation failed: `digest_state` no longer conflates the two (Phase A2).

`--json` prints the full machine-readable WorkerCard (`image_state` /
`operation_state`) for scripts:

```bash
scripts/fleetctl inspect --json worker-id | jq '.image_state.digest_match'
```

### Worker identity

`worker_id` is the immutable security identity: it is bound to the mTLS
certificate, Master registration, leases, and job history. Use it for every
`fleetctl` command and never replace it with a display name.

`worker_name` is the mutable operator-facing name. `status` output and worker
API cards show both values as `NAME` and `WORKER_ID`; changing `worker_name`
must not require certificate or OpenBao changes. Display names must never be
added to `VELOX_ALLOWED_WORKERS`.

`drain`, `resume`, and `quarantine` use the canonical admin worker routes and
return the operation ledger response. `operations` reads the audit ledger.
The smoke command delegates to `tests/worker-cert/smoke_one.sh`, which submits
a real asset job, waits for `SUCCEEDED`, and verifies the lease worker:

```bash
scripts/fleetctl smoke worker-id
```

## Digest rollout — canonical Master API

`scripts/fleetctl update` and `scripts/fleetctl rollback` route through the
canonical Master API: `POST /api/v1/admin/workers/{worker_id}/update` with the
pinned `@sha256:` digest. There is no separate rollback route: `rollback`
re-posts the previous-known-good digest and the Master `UpdateExecutor` owns
the drain, cosign verification, activation and the `deployment_records` audit
cascade.

The canonical chain is:

```text
fleetctl/API → Master → FleetController → UpdateExecutor →
WorkerNodeRegistry → SSH → velox-worker-activate-image
```

The worker must already be `DRAINING` with `active_jobs=0`. The command
refuses to run otherwise.

```bash
scripts/fleetctl update worker-id \
  ghcr.io/marcuss-ops/velox-worker@sha256:<64-hex-digest> \
  "canary release"
```

Manual rollback uses the previously recorded pinned image reference:

```bash
scripts/fleetctl rollback worker-id \
  ghcr.io/marcuss-ops/velox-worker@sha256:<previous-64-hex-digest> \
  "rollback after smoke failure"
```

No `FLEET_INVENTORY`, `FLEET_ROLLOUT_PLAYBOOK`, `FLEET_ROLLOUT_BUNDLE_HASH` or
`ansible-*` binaries are used: the release-evidence checks live server-side in
the FleetController/UpdateExecutor gates. Both commands poll the
`fleet_operations` ledger to terminal state (`FLEETCTL_WAIT_TIMEOUT_SECONDS`,
default 1800 s).

## Host lifecycle

`restart` sends the existing authenticated worker restart command. It should
be used only after the worker is drained and has no active jobs:

```bash
scripts/fleetctl restart worker-id "restart after config change"
```

Image updates, rollback and restart are performed by the Master
(`fleet_operations` ledger + `UpdateExecutor`); direct `sudo`, SSH, and Docker
commands are never a replacement for the approved path. The host-convergence
playbooks under `DataServer/data/ansible/playbooks/` remain an internal
Master-side runtime (inventory generated from the `WorkerNodeRegistry` DB via
`AnsibleComputerManager.GenerateInventory`), not an operator entrypoint. The
smoke command is the promotion gate after the operation completes.
