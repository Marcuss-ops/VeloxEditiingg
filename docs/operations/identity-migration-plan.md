# Identity migration plan — uniform worker naming

> **Status:** preparation only — no rename is authorized by this document.
>
> **Scope:** prepare and validate a serial, dual-identity, forward-only
> migration from the four current worker identities to the uniform names below.
> This plan does not mutate the Master DB, OpenBao, worker hosts, certificates,
> inventory, runtime snapshots, or legacy records.

## Decision gate

The identity rename remains blocked until all of the following are explicitly
approved and green:

- fleet certification is green and all four workers are canonical, reachable,
  healthy, and observed with fresh sessions/heartbeats;
- OpenBao is healthy and the target policy, AppRole, and KV paths are verified;
- a per-worker credential and mTLS certificate cutover has been rehearsed;
- a read-only DB backup/restore rehearsal and an operator-approved rollback
  window exist;
- the serial cutover procedure has been reviewed for the exact release;
- a separate authorization is granted for cleanup of old DB/OpenBao records.

Until then, only the validator in `scripts/ci/validate-identity-migration-plan.py`
may be run. It is read-only and never calls SSH, Ansible, Docker, OpenBao, or
the Master API. Its scope is limited to the repository mapping and optional
local SQLite metadata; OpenBao, certificates, inventory, and audit are not
verified by this tool.

```text
OPENBAO_CERTIFICATE_INVENTORY_AUDIT=NOT_VERIFIED_BY_THIS_TOOL
```

The supported registry inspection workflow for a future cutover is
`velox-admin sync-worker-nodes --dry-run`. This preparation does not run it.
The current-to-target alias correspondence in this document is static
preparation metadata; it is not verified against the ignored live inventory by
the validator.
`--apply` is not a rename primitive or an atomic identity-cutover mechanism;
it is forbidden for this plan until a dedicated migration transaction/workflow
has been implemented and separately authorized. This preparation does not edit
inventory manually and does not use ad-hoc SQL.

The validator output is preparation status only, not a complete certification:
OpenBao, certificates, inventory, and audit remain explicitly outside its
execution scope.

## Complete mapping

| Current operational ID | Inventory alias | Target uniform ID |
|---|---|---|
| `host_57_129_132_133` | `worker_57_129` | `velox-worker-57-129-132-133` |
| `host_57_131_20_173` | `worker_57_131` | `velox-worker-57-131-20-173` |
| `velox-worker-13197` | `worker_13197` | `velox-worker-149-56-131-97` |
| `velox-worker-523925eb` | `worker_523925` | `velox-worker-51-222-204-158` |

The host alias and address are routing metadata only. The stable `worker_id`
is the identity join key; no target ID may be inferred from an alias alone.

The Master also contains historical IDs using the `velox_worker_<ip>` form.
They are not silently treated as aliases for this migration. Each requires an
explicit host-level identity reconciliation before any cleanup decision.

## Dependency graph

The `worker_id` is coupled to all of these surfaces:

```text
Master workers registry
  ├── worker_credentials (hash metadata only; value never printed)
  ├── ansible_hosts / WorkerNodeRegistry
  ├── worker sessions, task attempts, and active leases
  ├── new worker_runtime_snapshots
  ├── VELOX_WORKER_ID in the canonical worker environment
  ├── OpenBao KV: velox/production/workers/<worker_id>/*
  ├── OpenBao policy: worker-<worker_id>
  ├── OpenBao AppRole: worker-<worker_id>
  ├── materialized worker credential and mTLS files
  ├── mTLS certificate CN/SAN identity, if identity-bound
  └── audit, migration manifest, and evidence records
```

The SSH operator CA is a separate control plane. Its `velox-deploy` principal
is not a worker identity and must not be renamed as part of this operation.

`worker_runtime_snapshots` are historical identity records. Existing snapshots
must remain immutable; the cutover creates forward-looking snapshots under the
target ID instead of rewriting historical rows.

## Read-only baseline

Baseline captured during the preparation run on **2026-08-07**. It is an
assessment snapshot, not a live certification; rerun the read-only checks before
any future authorization window.

The preparation audit found:

- Master SQLite `integrity_check`: `ok`;
- `workers`: current IDs plus four distinct historical `velox_worker_*` IDs;
- `ansible_hosts`: four rows, still keyed by the current operational IDs;
- runtime snapshots: current, historical, and local test identities are present;
- current credential rows and non-empty hash metadata: `4/4`;
- current identity coverage in the inspected `workers`, `worker_credentials`,
  and `ansible_hosts` tables: `4/4`;
- target-ID collisions in workers, credentials, and runtime snapshots: `0`;
- local AppRole material exists for current IDs, not for target IDs;
- OpenBao live health: `503`, so target paths/policies/AppRoles are not yet
  certifiable;
- inventory and hardware snapshot still use current IDs;
- certification status: `NOT_CERTIFIED`;
- the canary SSH CA prerequisite remains incomplete;
- no DB, OpenBao, worker, certificate, inventory, or repository mutation was
  performed by this preparation.

Hash values, credentials, tokens, private keys, certificate bodies, addresses,
and raw JSON are deliberately excluded from this document and validator output.

## Required serial sequence after authorization

No phase may proceed if its gate fails. Only one worker is in the cutover phase
at a time.

### Phase 0 — freeze and evidence

1. Freeze the mapping above for the release window.
2. Capture read-only evidence for the four workers: current/target IDs,
   registry state, active tasks, sessions, heartbeat, digest, credential hash
   presence, OpenBao references, certificate metadata, and rollback location.
3. Verify clean ownership of the operation and preserve unrelated worktree
   changes outside any preparation commit.

### Phase 1 — prepare target identity surfaces

For one worker at a time, and without deleting the old identity:

1. Provision the target OpenBao policy, AppRole, and KV branch through the
   canonical scripts; verify least privilege and target-path reads.
2. Create the target credential material from the authoritative source without
   printing or rotating the value unnecessarily.
3. Prepare the target mTLS certificate through the canonical PKI/materialization
   path and verify CN/SAN, issuer, validity, and chain metadata.
4. Design the target registry/inventory change around a dedicated, reviewed
   migration transaction/workflow. `sync-worker-nodes --apply` must not be
   treated as an identity rename and is not authorized by this plan; do not use
   ad-hoc SQL or manual inventory edits.

The old policy, AppRole, KV branch, credential record, and runtime identity
remain intact during this phase.

### Phase 2 — serial host cutover

For each worker, in a reviewed order:

```text
drain
→ verify active_tasks=0
→ materialize target AppRole and certificate
→ set target VELOX_WORKER_ID through the canonical host path
→ start canonical runtime through its approved owner
→ verify authenticated registration under target ID
→ verify CONNECTED, session_active=true, and fresh heartbeat
→ observe the worker and record evidence
→ resume only after the target gate is green
```

Stop immediately on any failure. Do not start the next worker while the current
worker is unresolved.

### Phase 3 — post-cutover verification

Require all four:

- target IDs are unique and present in the supported registry;
- no active task/session still points at an unresolved cutover;
- OpenBao target reads and least-privilege checks pass;
- mTLS certificate identity and chain checks pass;
- fleet, restart/recovery, and audit gates are green;
- mapping evidence is persisted without secret values.

Cleanup of old records and paths is a separate, destructive change window.

## Rollback plan

The old identity remains the rollback source until the post-cutover observation
window closes. Preserve, per worker:

- current Master registry and credential record;
- current OpenBao policy, AppRole, and KV branch;
- current `VELOX_WORKER_ID` and environment backup;
- current mTLS material metadata and prior certificate where still valid;
- previous pinned runtime digest and migration evidence;
- DB backup plus a tested restore procedure. The backup must be a SQLite
  online/read-only backup artifact retained outside the live DB, followed by
  `PRAGMA integrity_check` on the backup and a documented SHA-256 of the
  artifact. A future approved rehearsal should use the SQLite backup API (or
  the project's approved backup wrapper), verify the artifact checksum, and
  restore into a disposable path before comparing integrity and row counts.
  Restore is a separately approved maintenance action, performed against a
  stopped/quiesced Master and validated before service restart; it is never an
  ad-hoc overwrite of the live database.

If a target cutover fails:

```text
quarantine/drain target
→ stop the target runtime through its canonical owner
→ restore the previous VELOX_WORKER_ID and environment
→ restore current AppRole/path/credential material
→ restore the previous mTLS material
→ restore the previous pinned runtime
→ verify CONNECTED, session_active=true, and fresh heartbeat
→ record failure and stop the rollout
```

Do not delete the target or old surfaces during rollback until the worker is
stable and the evidence is complete. If the old credential or certificate is
not recoverable from the verified backup/source, stop and quarantine the worker;
do not invent or rotate replacement identity material during rollback. If
rollback fails, leave the worker quarantined and escalate; never continue to
the next worker.

## Explicit non-actions

This preparation does **not**:

- rename any DB row or runtime snapshot;
- create target OpenBao policy, AppRole, or secret paths;
- rotate or copy credentials or certificates;
- edit the operator inventory;
- run Ansible, SSH, Docker, systemd, or Master API mutations;
- remove legacy records, units, containers, or paths;
- claim certification or authorize cleanup.

## Validation

Repository-only validation:

```bash
python3 scripts/ci/validate-identity-migration-plan.py
```

Read-only DB metadata validation, when an operator intentionally provides a
local readable database path:

```bash
python3 scripts/ci/validate-identity-migration-plan.py \
  --db /path/to/velox.db
```

The validator reports only mapping, row-count, presence, collision, and
integrity metadata. It never prints hashes or secret values.
