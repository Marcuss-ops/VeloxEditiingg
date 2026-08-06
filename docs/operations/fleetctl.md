# Fleet Operations

> **Path selection:** read `docs/operations/worker-rollout-paths.md` before
> changing a worker. It distinguishes the legacy bundle bridge, the
> transitional GHCR Ansible bridge, and the definitive FleetController API
> path. Do not treat their commands as interchangeable.

`scripts/fleetctl` is the operator facade for the worker fleet. It talks to
the Master with the admin bearer token and never prints that token.

## Read and control

```bash
export VELOX_MASTER_URL="http://MASTER:8000"
export VELOX_ADMIN_TOKEN="..."

scripts/fleetctl status
scripts/fleetctl inspect worker-id
scripts/fleetctl drain worker-id "prepare deployment"
scripts/fleetctl resume worker-id "deployment complete"
scripts/fleetctl quarantine worker-id "asset failures"
scripts/fleetctl operations worker-id
```

`drain`, `resume`, and `quarantine` use the canonical admin worker routes and
return the operation ledger response. `operations` reads the audit ledger.
The smoke command delegates to `tests/worker-cert/smoke_one.sh`, which submits
a real asset job, waits for `SUCCEEDED`, and verifies the lease worker:

```bash
scripts/fleetctl smoke worker-id
```

## Digest rollout — transitional GHCR Ansible bridge

For the current implementation, `scripts/fleetctl update` is a host-mutation
bridge: it checks the Master state and invokes Ansible on exactly one inventory
host. It is **not** yet the direct FleetController API entrypoint. For the
complete path map and production prohibitions, see
`docs/operations/worker-rollout-paths.md`.

The worker must already be `DRAINING` with `active_jobs=0`. The command
refuses to run otherwise. It invokes Ansible on exactly one inventory host and
the playbook performs the host-side digest validation, `prepare-host.sh`,
readiness check, deployment manifest, and automatic restoration of the
previous env file on failure:

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

`FLEET_INVENTORY` must point to an operator-local inventory file; never use
`deploy/ansible/inventory.ini.example` or another repository template as a
production inventory. The default playbook is
`deploy/playbooks/rollout-worker-digest.yml`; override it with
`FLEET_ROLLOUT_PLAYBOOK` when a host topology uses different paths.

`update` and `rollback` fail closed without the current-commit release
certificate (the same evidence the `worker-image` workflow publishes in its
baseline manifest):

```bash
export FLEET_ROLLOUT_BUNDLE_HASH="<64-hex bundle hash from BUILD_INFO.json>"
export FLEET_ROLLOUT_ENGINE_SHA="<64-hex engine SHA-256>"
export FLEET_ROLLOUT_SOURCE_HASH="<64-hex source hash from BUILD_INFO.json>"
```

All three are required; never omit them, otherwise the rollout playbook
refuses to mutate the host.

## Host lifecycle

`restart` sends the existing authenticated worker restart command. It should
be used only after the worker is drained and has no active jobs:

```bash
scripts/fleetctl restart worker-id "restart after config change"
```

Image updates and rollback on this transitional bridge are deliberately
performed by Ansible, where SSH, non-interactive sudo, digest pinning, serial
rollout, and host-side health checks are available. The definitive target is
the Master `fleet_operations` / FleetController path described in the rollout
paths guide; direct `sudo`, SSH, and Docker commands are never a replacement
for either approved path.

```bash
ansible-playbook -i deploy/inventory/production.ini \
  DataServer/data/ansible/playbooks/update_workers.yml \
  --limit worker-id -e "master_url=$VELOX_MASTER_URL"

ansible-playbook -i deploy/inventory/production.ini \
  DataServer/data/ansible/playbooks/restart_workers.yml \
  --limit worker-id
```

The Master remains the source of truth for worker state and job placement;
Ansible remains the source of truth for host mutation. The smoke command is
the promotion gate after the playbook completes.
