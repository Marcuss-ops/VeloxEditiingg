# Worker rollout paths: bridge and definitive route

This document is the operator map for moving from legacy workers to the
immutable GHCR/FleetController model. It deliberately separates commands that
only inspect state from commands that mutate a worker.

## Decision table

| Path | Use | Image/source | State owner | Production status |
|---|---|---|---|---|
| **GHCR/FleetController (Master API)** | Definitive rollout path | One signed GHCR digest produced by `worker-image.yml` | Master `fleet_operations` ledger + FleetController/UpdateExecutor | Canonical path |
| ~~Legacy Ansible bridge~~ | **Retired** — playbooks and static inventory removed | — | — | Historical |
| ~~GHCR Ansible bridge~~ | **Retired** — `rollout-worker-digest.yml` removed; `fleetctl update` now POSTs the Master API | — | — | Historical |

Do not combine paths in one rollout. In particular, do not build a local image
on a worker and also claim that the worker is running the certified GHCR image.

## 1. One-time legacy migration for old workers

Use this path only once when an old worker must be migrated to the canonical
systemd/container layout. The migration retires legacy units, containers,
environment paths, and writable trees, then records
`/var/lib/velox-worker/migration/completed`. After that marker exists, normal
rollouts must use the canonical release path and must not rediscover or clean
legacy paths.

The bridge is:

```text
WorkerNodeRegistry row (ansible_hosts DB)
  -> preflight_workers.yml
  -> update_workers.yml
  -> bundle download from Master
  -> local Docker build on the worker
  -> canonical systemd/runtime convergence
  -> readiness and registration checks
```

Host convergence runs through the **Master** (`AnsibleComputerManager`): the
per-operation inventory is generated from the `WorkerNodeRegistry` DB and the
preflight/update playbooks under `DataServer/data/ansible/playbooks/` are
invoked server-side (`POST /api/v1/admin/ansible/computers/run_action`). There
is no operator-side static inventory and no `--vault-password-file`:

```bash
export MASTER_URL=https://MASTER.example.invalid:8000

# 1. Drain through the Master and wait until active_tasks=0.
scripts/fleetctl drain <worker_id> "legacy bridge rollout"
scripts/fleetctl inspect <worker_id>   # repeat until DRAINING and active_tasks=0
# 2. Trigger the Master-side preflight/update for this worker via
#    POST /api/v1/admin/ansible/computers/run_action (action=update).
```

Before proceeding, verify the bundle identity against the commit, version,
source/bundle hash and engine hash that are intended for the canary. This path
still performs a local build on the worker, so it is a bridge and not a
reproducible GHCR release deployment.

After the bridge, verify the canonical unit, one container, readiness,
registration, fresh heartbeat, a real Level-D smoke, and a short canary window.
Only then resume the worker and continue one host at a time.

### One-time migration limitations

- It does **not** prove that every worker runs identical image bytes; use the
  digest rollout path for release certification.
- It must not be used to roll out a mutable Docker tag.
- It must not be run against all workers concurrently.
- It must not be used as evidence that a GHCR digest was signed or certified.
- Once the completion marker exists, do not re-run legacy cleanup as part of a
  normal rollout; investigate a missing marker or broken canonical tree first.

## 2. GHCR Ansible bridge — RETIRED

The transitional host-side bridge (`deploy/playbooks/rollout-worker-digest.yml`,
`FLEET_INVENTORY`, `FLEET_ROLLOUT_PLAYBOOK`, `ansible-playbook`) has been
retired. `scripts/fleetctl update` / `scripts/fleetctl rollback` now POST the
pinned digest directly to the Master API and poll the `fleet_operations`
ledger; see §3.

## 3. Definitive GHCR/FleetController path

The canonical production architecture is:

```text
main commit
  -> GitHub Actions worker-image.yml
  -> one GHCR image
  -> Cosign signature + SBOM/provenance
  -> baseline manifest bound to the source commit
  -> fleetctl/API -> Master FleetController
  -> fleet_operations ledger -> UpdateExecutor
  -> WorkerNodeRegistry (ansible_hosts)
  -> SSH -> velox-worker-activate-image
  -> readiness + reconnect + heartbeat + Level-D smoke + Drive
  -> resume on success, rollback and quarantine on failure
```

The definitive update operation is the Master API operation published by
`scripts/fleetctl` to `POST /api/v1/admin/workers/{worker_id}/update`. Use this
path in production only after
the real FleetController backends and all release gates are wired and green;
the document describes the target contract, not permission to bypass an
unwired composition root:

```text
POST /api/v1/admin/workers/{worker_id}/update
GET  /api/v1/admin/operations/{operation_id}
```

`fleet-update.yml` is an API delegate: it does not SSH to the host or run
`docker pull` locally. It posts the operation, accepts `202` or an existing
`409` operation with `operation_id`, and polls the ledger until `SUCCEEDED`,
`FAILED`, or `ROLLBACK`.

A complete API-delegate invocation is:

> **Gate before execution:** confirm the current worker-image certification
> manifest matches the intended commit and digest, and record its bundle hash,
> engine SHA, executor identity/version, and signed provenance. Confirm that
> the FleetController backends are wired and green. The command below is not a
> substitute for those checks.

```bash
export VELOX_MASTER_URL=https://MASTER.example.invalid:8000
export VELOX_ADMIN_TOKEN='read-from-secure-secret-store'

scripts/fleetctl status                 # read-only
scripts/fleetctl inspect <worker_id>    # read-only
scripts/fleetctl drain <worker_id> "canary rollout"
# wait for the drain idle precondition (active_jobs=0), then:
scripts/fleetctl update <worker_id> \
  ghcr.io/<owner>/velox-worker@sha256:<64-lowercase-hex> \
  "canary rollout"
```

The internal API-delegate playbook `deploy/playbooks/fleet-update.yml`, where
still enabled for server-side compatibility orchestration, posts the same
operation (`POST /api/v1/admin/workers/{worker_id}/update`, with
`update_target_digest=sha256:<64 hex>`) and polls the ledger. It is not an
operator entrypoint; operators use `scripts/fleetctl`.

The definitive path must consume an immutable digest, not a version tag:

```text
ghcr.io/<owner>/velox-worker@sha256:<64-lowercase-hex>
```

The image must come from the current worker-image certification run. Never
reuse a digest certified for an older commit merely because its version tag
matches the desired version.

### Important current distinction

`scripts/fleetctl update` IS the direct Master API entrypoint. The playbook
`deploy/playbooks/fleet-update.yml` remains only as an internal API-delegate
(for server-side compatibility orchestration); the two entrypoints are not
interchangeable.

## 4. Diagnostic-only commands

The following commands inspect state and do not intentionally mutate workers.
They are not release or promotion commands.
They are appropriate for preflight, evidence collection, and troubleshooting.

The following are **mutating operations**, not diagnostics: `drain`, `resume`,
`quarantine`, `restart`, `update`, `rollback`, and `smoke`. They require an
explicit operator decision and the appropriate production gate.

```bash
# Master read-side checks
scripts/fleetctl status
scripts/fleetctl inspect <worker_id>
scripts/fleetctl operations <worker_id>

# Host read-side checks (Master-side; inventory generated from the DB)
#   POST /api/v1/admin/ansible/computers/run_action  action=preflight

# Direct host inspection only
systemctl status velox-worker-<alias> --no-pager
docker ps --filter 'name=velox-worker'
docker inspect <container> --format '{{.Config.Image}} {{.Image}}'
curl -fsS http://127.0.0.1:<health-port>/health/ready
```

`status`, `inspect`, `operations`, `systemctl status`, `docker ps`,
`docker inspect`, and the readiness `curl` are diagnostics. A successful
readiness probe is not a deployment certification and is not a substitute for
Level-D smoke and artifact verification.

`fleetctl smoke` is **not** read-only: it creates an audited smoke operation
and may lease work, render, and upload an artifact. Run it only as an explicit
promotion gate on a drained canary.

## 5. Commands and practices forbidden in production

Do not use these as an independent production rollout path:

```text
ssh <worker> docker pull ...
docker pull <mutable-tag>
docker compose ... up -d
sudo systemctl restart velox-worker-...
manual edits to /etc/velox-worker/worker.env
running prepare-host.sh by hand to bypass the operation ledger
running docker build on every worker as a release mechanism
```

Direct host mutation bypasses one or more of the digest, Cosign, drain,
operation-ledger, rollback, reconnect, smoke, or Drive gates. `prepare-host.sh`
is an implementation step invoked by the approved rollout path
(`velox-worker-activate-image`); it is not an operator-facing release command.

Also forbidden:

- using `latest`, `main`, `stable`, or a mutable semver tag in production;
- using a static or committed inventory (the only inventory is the
  `WorkerNodeRegistry` DB, generated per-operation by the Master);
- printing admin tokens, vault passwords, SSH private keys, or full env files;
- updating multiple workers at once;
- treating `health/ready=200` as proof that rendering and delivery work;
- using an old certified digest for the current commit without re-certification.

## 6. Promotion and rollback rule

For every worker:

```text
diagnostic status/inspect
  -> drain
  -> active_tasks=0
  -> one-worker update
  -> readiness + reconnect + heartbeat
  -> Level-D smoke + Drive artifact
  -> observe 15–30 minutes or 2–3 jobs
  -> resume
```

Any failed gate stops the rollout. Keep the previous digest and deployment
record, let the FleetController/UpdateExecutor execute rollback, quarantine the
worker, and investigate before touching the next host.

## Related files

- `docs/operations/fleetctl.md` — operator command reference.
- `docs/worker_deployment.md` — worker layout and compatibility details.
- `deploy/playbooks/fleet-update.yml` — internal Master API/FleetController
  delegate (scheduled for removal).
- `DataServer/data/ansible/playbooks/update_workers.yml` — Master-side legacy
  bundle bridge (inventory generated from the `WorkerNodeRegistry` DB).
- `.github/workflows/worker-image.yml` — build, sign, and certify image.
