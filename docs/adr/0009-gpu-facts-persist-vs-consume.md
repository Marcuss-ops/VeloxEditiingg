# ADR: GPU capability facts — persist as read-model telemetry; placement consumption explicitly deferred

- **Status**: Accepted
- **Date**: 2026-09-06
- **Scope**: `DataServer` worker runtime snapshots (`worker_runtime_snapshots`), the admin capacity read model, and the placement engine (`internal/placement`).
- **Supersedes**: (none)
- **Related**: AGENTS.md §6 capability-state contract; audit finding A2-4 (write-only state on the admission hot path).

## (a) Contesto

The 2026-09 audit flagged that the master persists GPU/accelerator facts on
every worker Hello — `gpu_model`, `gpu_vram_bytes`, `nvenc_available`,
`nvdec_available`, `qsv_available` (`handler_stream.go` →
`GetOrCreateWorkerRuntimeSnapshot`) — but **no scheduling decision reads
them**. `placement.WorkerSnapshot` carries no hardware fields; the only
consumer is the admin read model (`GetWorkerCapacityReport` →
`GET /api/v1/admin/workers/{id}/capacity`).

This is the "accepted but never read" smell at schema level: per-connection
writes on the single-writer SQLite pool for state nothing acts on.

## (b) Decision

**KEEP persisting. DEFER placement consumption. Document the gap.**

1. **Persist** — the write stays on the Hello path. Rationale:
   - The snapshot is the *only* durable record of worker hardware; it is
     cheap (one INSERT OR IGNORE per Hello, not per heartbeat), and it
     feeds the admin capacity report that fleet operators use for
     placement planning and hardware retirement decisions today.
   - Dropping the columns would destroy the historical hardware inventory
     (which workers can take NVENC jobs?) for zero write-path savings that
     matter at current fleet size.
2. **Do NOT wire into placement yet** — GPU-aware placement requires a
   real executor-contract decision: which executors are `resource_class:
   gpu`, how does a requirement express "NVENC required" vs "GPU
   preferred", and what happens on mixed pools. That is a feature, not a
   refactor, and must arrive with tests + a capacity rollout plan.
3. **Guard against silent decay**: the read model is the proof of
   consumption. If a future audit finds both the placement path AND the
   admin read model no longer project these fields, the columns become a
   scheduled removal through the pre-removal gate.

## (c) Re-open triggers

This ADR must be revisited when ANY of the following becomes true:

- a second executor with `resource_class: gpu` lands (GPU/no-GPU pools
  diverge);
- a capacity cell in `cap-9` requires hardware-aware placement
  invariants;
- the admin capacity report is retired or replaced;
- the quarterly audit finds the columns read by nothing.

## (d) Acceptance criteria of this decision

- `worker_runtime_snapshots` keeps the accelerator columns (no migration).
- `placement.WorkerSnapshot` deliberately has NO hardware fields — a
  comment in `internal/placement/model.go` points here.
- The admin capacity read model continues to expose
  `gpu_model`/`nvenc_available`/… (`GET /api/v1/admin/workers/{id}/capacity`).
