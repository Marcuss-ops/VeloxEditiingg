# ADR: Single-writer tx contract for the worker_runtime cluster

- **Status**: Accepted
- **Date**: 2026-07-19
- **Scope**: `DataServer/internal/store/store_worker_*.go` (the `worker_runtime` cluster)
- **Supersedes**: (none — first formal record)
- **Related commits**:
  - `a6b293a` — `docs(store): document reconcileOnePartition BeginTx exception in worker_runtime shell`
  - `babcb81` — `refactor(store): isolate reconcileOnePartition in recovery_tx file`
  - `85e4969` — `chore(ci): add single-writer BeginTx audit to regression runner`
  - `3fcd94a` — `docs(store): extend ReconcileWorkerPartitions godoc with caller/no-tx/disjoint contract`

## (a) Contesto

The `DataServer/internal/store` package hosts the persistent mirror of the worker fleet. Two distinct writer paths mutate the `workers.connection_state` column:

1. **Heartbeat path** — invoked from each worker agent's heartbeat loop, transcribed via `PersistWorkerHeartbeat`. This is the latency-critical hot path: every heartbeat must persist within a tight budget, and the heartbeat path composes (i) a snapshot upsert, (ii) a state-machine transition (via `detectAndPersistPartitionTransition`), and (iii) a canonical `worker_events` audit row.

2. **Recovery path** — invoked from the master scheduler's cron loop, transcribed via `ReconcileWorkerPartitions`. This is the cron-style catch-up path: it surfaces workers whose heartbeat stream has stopped entirely (no `PersistWorkerHeartbeat` call fires for them, by definition), transitions them to a `PARTITIONED` state, and emits a corresponding audit row.

Both paths are first-class operations on the same `workers` table and the same `connection_state` column. Pre-`a6b293a`, the relationship between them was implicit and easy to break: any helper added to the recovery file could open its own `*sql.Tx`, silently violating the single-writer invariant and producing interleaved state transitions that no test would catch.

The recovery-loop extraction in `babcb81` physically moved the only recovery-path opener (`reconcileOnePartition`) out of the state-machine file, and `a6b293a` documented the WHY at the file level. The CI audit gate in `85e4969` made the contract enforceable on every regression run. The method-level godoc in `3fcd94a` made the developer-facing contract explicit on the public `ReconcileWorkerPartitions` method.

This ADR records the contract formally, in a discoverable place, so future maintainers do not need to triangulate the rationale across four commits and three files.

## (b) Decisione

The `worker_runtime` cluster (`DataServer/internal/store/store_worker_*.go`) permits **exactly two (2)** `s.db.BeginTx` call sites:

| # | Path      | File                              | Function                 | Owner  |
|---|-----------|-----------------------------------|--------------------------|--------|
| 1 | heartbeat | `store_worker_heartbeat.go`       | `PersistWorkerHeartbeat` | worker |
| 2 | recovery  | `store_worker_recovery_tx.go`     | `reconcileOnePartition`  | master |

Rules:

- **All other helpers** in the cluster either receive a `*sql.Tx` parameter from a caller (heartbeat-path detectors like `detectAndPersistPartitionTransition`) or use `s.db.Query` / `s.db.Exec` directly (read + post-commit side effects like `DeleteWorkerTaskRuntime`).

- **The two paths are disjoint**: site #1 writes `connection_state = PARTITIONED_SUSPECTED` (suspect signal); site #2 writes `connection_state = PARTITIONED` (unreachable signal). They never target the same value, so the single-writer-of-`connection_state`-at-a-time invariant holds despite the shared column.

- **The audit gate** (`scripts/ci/run-split-regression.sh`, group `audit-single-writer-begin-tx`) enforces this contract statically: the pattern `tx, err := .*s\.db\.BeginTx\(ctx, nil\)` must yield exactly 2 hits across the cluster, both in the allowed files. Any divergence escalates `OVERALL_RC=2`.

## (c) Conseguenze

The five rationale that justify the two-site exception. Each numbered to match the documentation in `store_worker_recovery_tx.go` and `store_worker_runtime_recovery.go`:

### 1. Recovery is a cron-style background loop

It is intentionally invoked from the master scheduler on a wall-clock cadence, NOT from a heartbeat. There is no `PersistWorkerHeartbeat` call available to piggyback on, even in principle — coupling the recovery loop to the heartbeat path would require the master to drive per-worker heartbeat calls, contradicting the recovery semantics. Master is structurally independent of the worker heartbeat stream.

### 2. Recovery detects the unreachable tail

The whole purpose of the loop is to surface workers whose heartbeat stream has stopped entirely. Coupling to the heartbeat path would defeat the recovery semantics: a worker who never fires a heartbeat would also never trigger the recovery write. The recovery path exists precisely because the heartbeat path cannot fire for the candidates it targets.

### 3. Per-candidate atomicity is a recovery-side contract

The `PARTITIONED` state transition plus the `WORKER_PARTITION_DETECTED` audit row must be written together so a partial failure on one candidate does not corrupt the audit trail of another. This requires a per-candidate `*sql.Tx` boundary that the heartbeat path's single tx does not provide for the recovery path's fan-out. The recovery contract is: state transition + audit row in one atomic write, per candidate.

### 4. Single-writer invariant is preserved via disjoint value sets

`connection_state = PARTITIONED_SUSPECTED` (heartbeat path) and `connection_state = PARTITIONED` (recovery path) are distinct values in the canonical state enum. Two writers targeting disjoint values do not violate the "only one writer of `connection_state` at a time" invariant — they are not concurrent on the same row, even if the underlying column is shared. The invariant is column-level (single value at a time), not file-level (single opener per file).

### 5. Per-worker tx isolation prevents cascading failures

Each candidate's failure triggers a per-tx rollback, NOT abort of the rest of the reconciliation pass. A single broken worker row cannot delay the recovery of the rest of the fleet — this contract requires per-candidate tx boundaries that the heartbeat path's single tx cannot satisfy for the recovery path's fan-out. The recovery loop is structured as `for _, c := range candidates { reconcileOnePartition(...) }` with each iteration opening its own `*sql.Tx`, so a per-candidate failure only affects that one worker's row.

### Benefits

- **Dashboard clarity**: Dashboards can distinguish "worker came back late" (`PARTITIONED_SUSPECTED`) from "worker stream stopped entirely" (`PARTITIONED`) by reading `connection_state` directly, with no need to parse `details_json` on the audit row.
- **CI enforcement**: The audit gate catches any future helper that opens a third `s.db.BeginTx` site — the regression runner escalates `OVERALL_RC=2` before any such code reaches `main`.
- **Reduced concurrency reasoning complexity**: At most two transactions can be active on the `workers` table at any time, and they target disjoint values.
- **Recoverability semantics**: The recovery path guarantees that a worker's transition from unreachable back to alive goes through `PARTITIONED → PARTITIONED_SUSPECTED → CONNECTED` (reconciliation write → next fresh heartbeat → heartbeat-time detector), which is observable in the audit trail.

### Trade-offs

- **Per-candidate fsync cost**: The recovery path opens a fresh `*sql.Tx` per candidate — 1 fsync per worker recovered. This is acceptable because the recovery loop runs at a much slower cadence than the heartbeat path (cron, not heartbeat-driven), and the per-candidate atomicity is contractually required.
- **Restrictive 2-site constraint**: Adding a third writer (e.g., a new "operator-driven override" path that writes a different state value) requires a new ADR entry and an audit gate update. The 2-site constraint is intentionally restrictive — the audit is the canonical SHAPE of the contract, and any divergence is a deliberate change that the audit must surface.

## (d) Confini

The two writer paths are **structurally disjoint**:

### Heartbeat path — `PersistWorkerHeartbeat` (site #1)

| Aspect            | Value |
|-------------------|-------|
| Owner             | worker agent (per worker process) |
| Cadence           | heartbeat interval (typically 5-15s per worker) |
| Caller            | worker-agent heartbeat loop |
| Tx scope          | heartbeat-scoped (snapshot upsert + `detectAndPersistPartitionTransition` + audit row emission) |
| State values written | `CONNECTED`, `STALE`, `PARTITIONED_SUSPECTED` |
| Crosses cluster boundary? | NO — purely intra-cluster |

### Recovery path — `ReconcileWorkerPartitions` (site #2 → `reconcileOnePartition` → `persistPartitionedStateTx`)

| Aspect            | Value |
|-------------------|-------|
| Owner             | master scheduler (master cron loop or background goroutine on master process) |
| Cadence           | master cron (typically 1-5 min) |
| Caller            | master scheduler cron handler |
| Tx scope          | per-candidate (one tx per worker transitioned) |
| State values written | `PARTITIONED` (bare, not `PARTITIONED_SUSPECTED`) |
| Crosses cluster boundary? | NO — purely intra-cluster |

### Disjoint invariant

```
heartbeat path writes PARTITIONED_SUSPECTED  (s.db.BeginTx #1)
recovery path  writes PARTITIONED           (s.db.BeginTx #2)

The two states are disjoint in the values enum, so the column-level
single-writer invariant holds.

Recovery transitions:
  PARTITIONED → PARTITIONED_SUSPECTED → CONNECTED  (via heartbeat path
                                                  on next fresh
                                                  heartbeat from the
                                                  worker)

The bare PARTITIONED write is RESERVED for the recovery path —
the heartbeat path NEVER writes PARTITIONED.
```

### Out of scope

- **Other domains in `DataServer/internal/store`**: The package has additional `s.db.BeginTx` call sites in `forwarding_claim.go`, `forwarding_transitions.go`, `store_darkeditor_folders.go`, `store_deliveries_lease.go`, etc. These are NOT part of the `worker_runtime` cluster and are governed by their own per-domain contracts. The audit gate (`scripts/ci/run-split-regression.sh`) explicitly scopes to `DataServer/internal/store/store_worker_*.go` (cluster-only) and does NOT apply to the broader package.

- **Other Go modules**: `RemoteCodex/native/worker-agent-go` has its own concurrency contracts (worker-agent heartbeat loop, lease renewal, etc.) and is governed separately. The `shared/` module is also out of scope.

- **Schema / DDL**: This ADR governs transaction ownership, not schema. The `workers` table, the `worker_events` table, and the `connection_state` enum are unaffected.

## Cross-references

- **Commits**:
  - `a6b293a` — first formal documentation of the exception (shell-level)
  - `babcb81` — physical extraction of `reconcileOnePartition` into `store_worker_recovery_tx.go`
  - `85e4969` — CI audit gate (single-writer enforcement)
  - `3fcd94a` — method-level godoc on `ReconcileWorkerPartitions`

- **Authoritative godoc**:
  - `(*SQLiteStore).ReconcileWorkerPartitions` (in `store_worker_runtime_recovery.go`) — recovery-path contract, with `# Canonical caller`, `# Why no shared tx with PersistWorkerHeartbeat`, `# Why PARTITIONED is disjoint from PARTITIONED_SUSPECTED`, `# Returns`, `# Argument validation`, `# Cross-references` sections.
  - `(*SQLiteStore).PersistWorkerHeartbeat` (in `store_worker_heartbeat.go`) — heartbeat-path contract (canonical owner of `*sql.Tx` #1).

- **File-level header docs** (architectural context):
  - `DataServer/internal/store/store_worker_runtime.go` — canonical single-writer contract statement + cross-references to both BEGIN-TX sites.
  - `DataServer/internal/store/store_worker_recovery_tx.go` — full rationale for the recovery-path opener + maintenance contract for future contributors.
  - `DataServer/internal/store/store_worker_runtime_recovery.go` — state-machine + heartbeat detector + public recovery entry-point; contract for future maintainers (no new BEGIN-TX sites).
  - `DataServer/internal/store/store_worker_heartbeat.go` — heartbeat path implementation.

- **CI audit gate**:
  - `scripts/ci/run-split-regression.sh` — `audit-single-writer-begin-tx` group (static grep over the cluster).
  - `docs/2026-07-19-post-0d2158d-regression-check.md` — regression report documenting the audit (Results row #8, "Single-writer tx contract audit" section).

## Change log

- **2026-07-19**: ADR accepted. The 2-site contract is the canonical state, enforced by the CI audit gate (commit `85e4969`). Method-level godoc for `ReconcileWorkerPartitions` updated in `3fcd94a` to surface the contract on the public API. This ADR is the discoverable index across the four commits.
