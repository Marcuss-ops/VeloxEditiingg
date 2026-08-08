# Legacy worker deployment removal plan

This plan prepares the migration away from local Docker builds on workers,
worker bundles, and duplicate rollout entrypoints. It is intentionally phased:
no legacy path is deleted until the compatibility gate for that phase is green.

## Current decision

The production rollout path is now the FleetController API. Keep
`update_workers.yml` and the bundle APIs only as a recovery/migration bridge for
old hosts; they are not release entrypoints. The definitive path is
`scripts/fleetctl` → Master FleetController → UpdateExecutor and must remain
backed by real backends and a canary.

The current paths are:

| Path | Current role | Removal condition |
|---|---|---|
| `DataServer/data/ansible/playbooks/update_workers.yml` | Old-worker bundle bridge; builds Docker locally on the worker | Every remaining host is on a pinned GHCR image and has a tested rollback |
| `tasks/prepare_worker_image.yml` | Local Docker build task included by the bundle release path | No production playbook includes it for a migrated host |
| Bundle endpoint and `data/worker_downloads/` | Legacy source distribution | No supported worker needs bundle bootstrap or rollback |
| `deploy/playbooks/rollout-worker-digest.yml` | GHCR host-mutation bridge | FleetController owns the same gates and has passed canary acceptance |
| `scripts/fleetctl update` | Canonical operator facade; posts to the Master API | Keep the direct Master operation path and its ledger contract |
| `deploy/playbooks/fleet-update.yml` | Master API/FleetController delegate | Remains canonical; do not remove |

## Compatibility gate before every removal commit

Run the read-only gate from a clean checkout, with the CI/base commit
available locally:

```bash
git fetch origin main --depth=1
BASE_REF=origin/main python3 scripts/ci/check-worker-rollout-compatibility.py
```

If the checkout contains unrelated staged, unstaged, or untracked work, do not
let that work enter a removal commit. Either verify from a clean worktree or
provide an explicit production-file scope for the preparation change:

```bash
BASE_REF=HEAD \
COMPAT_SCOPE=scripts/ci/verify.sh \
COMPAT_ALLOWLIST=scripts/ci/verify.sh \
python3 scripts/ci/check-worker-rollout-compatibility.py
```

The gate intentionally fails closed on a dirty checkout without
`COMPAT_SCOPE` and the identical `COMPAT_ALLOWLIST`. `ALLOW_DIRTY=1` only bypasses the outer dirty-tree stop in
`verify.sh`; it does not bypass this compatibility gate.

The gate checks that the migration contract still has both sides during the
transition:

- the old bridge files and their bundle/build responsibilities are still
  present until their removal phase;
- the GHCR workflow records current-commit provenance and immutable digests;
- the internal API delegate, where retained for compatibility, still posts and
  polls a ledger operation;
- `fleetctl update` posts directly to the Master API and requires no operator
  inventory or Ansible binary;
- the image Dockerfile and runtime verification entrypoints remain available.

Also run the focused syntax and structural checks relevant to the files changed
in that commit. Never count a compatibility gate as a deployment or as proof
that a production canary succeeded.

## Phase 0 — inventory and freeze (this preparation)

1. Enumerate every worker from the Master and operator inventory.
2. Record for each worker:
   - `worker_id` and inventory host;
   - current runtime mode and canonical systemd unit;
   - complete GHCR image digest, not a tag;
   - worker-image certification commit, bundle hash, engine SHA and executor;
   - session/heartbeat state and active tasks;
   - previous digest and rollback location;
   - last Level-D smoke and Drive artifact evidence.
3. Mark any worker still dependent on a bundle or local build as `LEGACY`.
4. Freeze new production consumers of bundle/build entrypoints. New work must
   use the FleetController contract; the bundle path is reserved for migration
   and recovery of old hosts.

No files are deleted in Phase 0.

## Phase 1 — migrate old hosts to the GHCR bridge

For one canary host at a time:

```text
diagnostic status/inspect
  -> drain
  -> active_tasks=0
  -> signed current-commit GHCR digest
  -> rollout-worker-digest.yml
  -> readiness
  -> reconnect/session/heartbeat
  -> executor, bundle hash, engine SHA, ldd
  -> Level-D smoke and Drive artifact
  -> observe 15–30 minutes or 2–3 jobs
  -> resume
```

The host is eligible for the next phase only when the evidence is persisted and
rollback to the previous digest has been tested. The old bundle path remains
available for hosts that have not passed this gate.

## Phase 2 — FleetController acceptance record

The direct Master API path is now the production operator path. Retire the
remaining GHCR Ansible bridge only after the following acceptance evidence is
recorded for the supported fleet:

- `UpdateExecutor` is constructed with real SSH, Docker, Cosign, Registry,
  Smoke and Drive implementations;
- drain and `active_tasks=0` are owned by the operation, not by operator notes;
- `202` and `409 with operation_id` both attach to a pollable ledger operation;
- `409` without `operation_id` fails closed;
- forward failure records failure and rollback is attempted;
- rollback failure leaves the worker quarantined or explicitly actionable;
- Level-D smoke verifies render, artifact commit and Drive delivery;
- a canary passes with the same image digest used for the real rollout;
- the compatibility gate and focused tests are green on the exact commit.

Until these conditions are green for a specific host, keep that host on the
migration/recovery bridge. `deploy/playbooks/fleet-update.yml` is retained only
as an internal API-delegate artifact; it is not permission to invoke a static
inventory or bypass `scripts/fleetctl`.

## Phase 3 — remove local Docker builds

Only after every supported host is GHCR-native:

1. stop including `tasks/prepare_worker_image.yml` from migrated release paths;
2. remove worker-side `docker build --pull --no-cache` tasks;
3. make host preparation pull only the certified `GHCR@sha256` image;
4. retain image ID, digest, commit and rollback evidence;
5. test idempotency, restart, readiness, reconnect and rollback on a canary;
6. remove local image-tag assumptions from post-deploy checks.

The Dockerfile itself is **not** removed: CI still uses it to build the single
release image. Only the worker-side compilation path is retired.

## Phase 4 — retire bundle distribution

After Phase 3 has been accepted across the fleet:

1. stop generating new worker bundles in release automation;
2. stop using `data/worker_downloads/` as a deployment source;
3. keep a read-only archival/export procedure for incident recovery;
4. remove bundle download and fallback-curl tasks from the production bridge;
5. remove bundle-only API documentation and tests only after all consumers are
   migrated;
6. retain an explicit migration error rather than silently falling back.

Do not delete bundle artifacts from historical evidence before the retention
and incident-response policy permits it.

## Phase 5 — retire duplicate operator paths

After FleetController has owned production updates for a complete canary window:

- keep `fleetctl update` submitting the Master operation directly (completed);
- keep `status`, `inspect`, `operations` and other read-side diagnostics;
- remove any remaining Ansible invocation from auxiliary rollout entrypoints
  only after an end-to-end test proves the same ledger, rollback and evidence
  contract;
- retire `rollout-worker-digest.yml` only after no automation references it;
- remove duplicate inventory and systemd/compose rollout documentation;
- update alerts, runbooks and rollback procedures in the same atomic change.

## What must never be removed or bypassed

- immutable GHCR digest validation and Cosign verification;
- current-commit release provenance;
- serial rollout (`serial: 1`) and stop-on-first-failure behavior;
- drain and active-task idleness;
- Master reconnect, session and fresh heartbeat checks;
- executor, engine SHA and dependency checks;
- Level-D smoke, artifact commit and Drive delivery verification;
- deployment ledger, rollback and quarantine evidence;
- WorkerNodeRegistry-backed connectivity and secret handling.

## Commit discipline

Each removal phase is a separate commit. Before each commit:

```bash
# Prefer a clean checkout; otherwise scope only the production files in this phase.
git fetch origin main --depth=1
BASE_REF=origin/main python3 scripts/ci/check-worker-rollout-compatibility.py
# or, for this preparation commit in a deliberately dirty checkout:
BASE_REF=HEAD \
COMPAT_SCOPE=scripts/ci/verify.sh \
COMPAT_ALLOWLIST=scripts/ci/verify.sh \
python3 scripts/ci/check-worker-rollout-compatibility.py
# plus focused tests for the changed playbooks/scripts

git diff --check
```

Commit only the phase's files. Push the phase before beginning the next one.
If the gate fails, stop and restore compatibility; do not use a bypass flag to
make a removal commit appear green.
