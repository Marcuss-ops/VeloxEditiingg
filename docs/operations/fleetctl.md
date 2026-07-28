# Fleet Operations

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

## Digest rollout

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

Override the inventory with `FLEET_INVENTORY`. The default playbook is
`deploy/playbooks/rollout-worker-digest.yml`; override it with
`FLEET_ROLLOUT_PLAYBOOK` when a host topology uses different paths.

## Host lifecycle

`restart` sends the existing authenticated worker restart command. It should
be used only after the worker is drained and has no active jobs:

```bash
scripts/fleetctl restart worker-id "restart after config change"
```

Image updates and rollback are deliberately performed by Ansible, where SSH,
non-interactive sudo, digest pinning, serial rollout, and host-side health
checks are available. They are not implemented as `sudo` or SSH calls inside
a Master HTTP handler.

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
