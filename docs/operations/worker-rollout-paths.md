# Worker rollout paths: bridge and definitive route

This document is the operator map for moving from legacy workers to the
immutable GHCR/FleetController model. It deliberately separates commands that
only inspect state from commands that mutate a worker.

## Decision table

| Path | Use | Image/source | State owner | Production status |
|---|---|---|---|---|
| **Legacy Ansible bridge** | Normalize or upgrade an old worker that still receives bundles | Bundle downloaded from the Master; Docker image is built locally on the worker | Ansible + systemd on the host | Temporary migration path only |
| **GHCR Ansible bridge** | Controlled canary when the host is not yet managed by FleetController | Existing, signed GHCR `@sha256:` image | Ansible host mutation, Master state | Transitional; one host at a time |
| **GHCR/FleetController** | Definitive rollout path | One signed GHCR digest produced by `worker-image.yml` | Master `fleet_operations` ledger + FleetController | Canonical target path |

Do not combine paths in one rollout. In particular, do not build a local image
on a worker and also claim that the worker is running the certified GHCR image.

## 1. Legacy Ansible bridge for old workers

Use this path only when an old worker must first be normalized to the canonical
systemd/container layout or cannot yet consume a pinned GHCR image.

The bridge is:

```text
operator-local inventory
  -> preflight_workers.yml
  -> update_workers.yml
  -> bundle download from Master
  -> local Docker build on the worker
  -> canonical systemd/runtime convergence
  -> readiness and registration checks
```

The playbook is:

```text
DataServer/data/ansible/playbooks/update_workers.yml
```

Use an operator-local inventory, never a repository template or a copied
production inventory:

```bash
export INVENTORY=/path/to/operator-local/inventory.ini
export MASTER_URL=https://MASTER.example.invalid:8000

# 1. Drain through the Master and wait until active_tasks=0.
scripts/fleetctl drain <worker_id> "legacy bridge rollout"
scripts/fleetctl inspect <worker_id>   # repeat until DRAINING and active_tasks=0

ansible-playbook \
  -i "$INVENTORY" \
  DataServer/data/ansible/playbooks/preflight_workers.yml \
  --limit <inventory_host>

ansible-playbook \
  -i "$INVENTORY" \
  DataServer/data/ansible/playbooks/update_workers.yml \
  --limit <inventory_host> \
  --vault-password-file ~/.vault-velox-pass \
  -e "master_url=$MASTER_URL"
```

Before proceeding, verify the bundle identity against the commit, version,
source/bundle hash and engine hash that are intended for the canary. This path
still performs a local build on the worker, so it is a bridge and not a
reproducible GHCR release deployment.

After the bridge, verify the canonical unit, one container, readiness,
registration, fresh heartbeat, a real Level-D smoke, and a short canary window.
Only then resume the worker and continue one host at a time.

### Legacy bridge limitations

- It does **not** prove that every worker runs identical image bytes.
- It must not be used to roll out a mutable Docker tag.
- It must not be run against all workers concurrently.
- It must not be used as evidence that a GHCR digest was signed or certified.
- Retire it after the worker fleet has migrated to the definitive path.

## 2. GHCR Ansible bridge for a controlled canary

When the image is already built and signed, but the host is not yet fully
managed by FleetController, the transitional host-side playbook is:

```text
deploy/playbooks/rollout-worker-digest.yml
```

It requires a complete release-evidence tuple from the worker-image workflow:

```text
expected_bundle_hash
expected_engine_sha256
expected_executor_id
expected_executor_version
```

It performs, serially and with rollback on failure:

```text
drain/idle precondition
  -> pinned GHCR image
  -> host convergence
  -> readiness
  -> master reconnect/session/heartbeat
  -> executor identity
  -> bundle hash + engine SHA + ldd
  -> Level-D smoke
  -> Drive artifact verification
  -> current or rollback
```

The repository `scripts/fleetctl update` command currently reaches this
Ansible bridge and therefore requires `FLEET_INVENTORY` to point to an
operator-local inventory:

```bash
export FLEET_INVENTORY=/path/to/operator-local/inventory.ini
export VELOX_MASTER_URL=https://MASTER.example.invalid:8000
export VELOX_ADMIN_TOKEN='read-from-secure-secret-store'

scripts/fleetctl status                 # read-only
scripts/fleetctl inspect <worker_id>    # read-only
scripts/fleetctl drain <worker_id> "canary rollout"
# wait for the command's idle precondition, then:
scripts/fleetctl update <worker_id> \
  ghcr.io/<owner>/velox-worker@sha256:<64-lowercase-hex> \
  "canary rollout"
```

This is a useful bridge for a controlled old host, but it is not the final
FleetController entrypoint while `scripts/fleetctl update` still invokes
Ansible directly.

## 3. Definitive GHCR/FleetController path

The target architecture is:

```text
main commit
  -> GitHub Actions worker-image.yml
  -> one GHCR image
  -> Cosign signature + SBOM/provenance
  -> baseline manifest bound to the source commit
  -> Master FleetController operation
  -> fleet_operations ledger
  -> UpdateExecutor
  -> drain + active_tasks=0
  -> Cosign/Docker/SSH/registry gates
  -> reconnect + heartbeat + Level-D smoke + Drive
  -> resume on success, rollback and quarantine on failure
```

The definitive update operation is the Master API operation represented by
`deploy/playbooks/fleet-update.yml`. Use this path in production only after
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
>
> `fleet-update.yml` accepts `update_target_digest=sha256:<64 hex>` because the
> Master API payload carries the digest; `scripts/fleetctl update` instead
> accepts the complete `ghcr.io/<owner>/velox-worker@sha256:<64 hex>` reference.

```bash
export INVENTORY=/path/to/operator-local/inventory.ini
export MASTER_URL=https://MASTER.example.invalid:8000

ansible-playbook \
  -i "$INVENTORY" \
  deploy/playbooks/fleet-update.yml \
  --limit <inventory_host> \
  --ask-vault-pass \
  -e "worker_id=<worker_id>" \
  -e "update_target_digest=sha256:<64-lowercase-hex>" \
  -e "velox_master_public_url=MASTER.example.invalid" \
  -e "velox_master_port=8000" \
  -e "update_reason=certified canary rollout"
```

The definitive path must consume an immutable digest, not a version tag:

```text
ghcr.io/<owner>/velox-worker@sha256:<64-lowercase-hex>
```

The image must come from the current worker-image certification run. Never
reuse a digest certified for an older commit merely because its version tag
matches the desired version.

### Important current distinction

Until the operator entrypoint is fully switched to submit this Master
operation directly, use `fleet-update.yml` for the API/FleetController route
and treat `scripts/fleetctl update` as the GHCR Ansible bridge described above.
Do not document or operate the two commands as interchangeable.

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

# Host read-side checks (run against one inventory host)
ansible-playbook -i "$INVENTORY" \
  DataServer/data/ansible/playbooks/preflight_workers.yml \
  --limit <inventory_host>

# Syntax validation only; no remote connection
ansible-playbook --syntax-check \
  -i "$INVENTORY" \
  DataServer/data/ansible/playbooks/rollout-worker-digest.yml

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
is an implementation step invoked by the approved Ansible bridge; it is not an
operator-facing release command.

Also forbidden:

- using `latest`, `main`, `stable`, or a mutable semver tag in production;
- using the committed `*.example` inventory as `-i` input;
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
record, let the controller/bridge execute rollback, quarantine the worker, and
investigate before touching the next host.

## Related files

- `docs/operations/fleetctl.md` — operator command reference.
- `docs/worker_deployment.md` — worker layout and compatibility details.
- `deploy/playbooks/fleet-update.yml` — Master API/FleetController delegate.
- `deploy/playbooks/rollout-worker-digest.yml` — GHCR Ansible bridge.
- `DataServer/data/ansible/playbooks/update_workers.yml` — legacy bundle bridge.
- `.github/workflows/worker-image.yml` — build, sign, and certify image.
