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

## Host lifecycle

`restart` sends the existing authenticated worker restart command. It should
be used only after the worker is drained and has no active jobs:

```bash
scripts/fleetctl restart worker-id "restart after config change"
```

Image updates and rollback are deliberately performed by Ansible, where SSH,
non-interactive sudo, digest pinning, serial rollout, and host-side health
checks are available. They must not be implemented as `sudo` or SSH calls
inside a Master HTTP handler. Use the existing worker playbooks with a real
inventory and `--limit` for one canary at a time:

```bash
ansible-playbook -i deploy/inventory/production.ini \
  DataServer/data/ansible/playbooks/update_workers.yml \
  --limit worker-id -e "master_url=$VELOX_MASTER_URL"

ansible-playbook -i deploy/inventory/production.ini \
  DataServer/data/ansible/playbooks/restart_workers.yml \
  --limit worker-id
```

The current command is an operator facade, not a full deployment controller:
the repository still needs a production inventory, immutable worker image
release variables, and a worker-specific rollback playbook before exposing
`update` and `rollback` as automatic CLI operations.
