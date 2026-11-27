# P2 — Profile SQL, allocations, rendering, I/O, and concurrency

**Status:** Verified — no optimization justified by current evidence
**Area:** DataServer and worker runtime hot paths
**Source:** Preliminary audit lists these areas as unmeasured.

## Problem

No profiling evidence was supplied for SQL/index cost, allocation pressure,
rendering, I/O, or concurrency. Optimizing based on file size or intuition can
add caching, batching, or abstraction that increases correctness risk without
improving the production bottleneck. Establish workloads and measurements
before proposing optimizations.

## Work plan

1. Select representative production-like workloads and record hardware,
   dataset shape, concurrency, data warm/cold state, and build version.
2. Capture baseline latency and throughput for end-to-end operations, then
   attribute time to SQL, filesystem/network I/O, media execution, queueing,
   and synchronization.
3. For SQLite, collect query plans and timings for high-volume/slow queries;
   check existing indexes and migration cost before proposing indexes.
4. For Go hot paths, use existing benchmarks, CPU/heap profiles, allocation
   counts, and race/trace tools where warranted. For render work, distinguish
   copy-only paths from decode/encode workloads and identify whether CPU, GPU,
   storage, or network is limiting.
5. Inspect cancellation and resource lifetimes under load: goroutines,
   subprocesses, open files, leases, DB rows/connections, temp files, and
   bounded queues.
6. Make one measured change at a time. Define target metric and guard against
   regressions in correctness, memory, throughput, and tail latency.

## Acceptance criteria

- Baseline workload and environment are reproducible and documented.
- At least one bottleneck is supported by a profile/query plan/trace rather
  than inferred from LOC or package size.
- Any SQL index change includes the relevant query plan and write/migration
  tradeoff; any allocator/concurrency change includes before/after evidence.
- Rendering results separate packet-copy from decode/encode scenarios.
- Optimizations retain bounded resource use and cancellation behavior.
- If measurements show no actionable bottleneck, record that result and stop
  instead of adding speculative optimization code.

## Verification

Use existing benchmark/profile commands and `docs/performance-gates.md`.
Repeat measurements enough to report variance and tail behavior. Run the
focused correctness and race tests relevant to changed code; performance
numbers alone do not establish correctness.

## Review result

The bounded local five-run copy-only baseline, repeated SQLite pool benchmark,
and current production cold/warm evidence are recorded in
[architecture-audit-2026-11-27.md](../benchmarks/architecture-audit-2026-11-27.md).
The evidence identifies worker readiness/lease wait and cold source transfer
as the dominant observed delays; it does not support speculative database,
allocation, or renderer changes. Copy-only measurements remain separate from
decode/encode rendering and the full production matrix in #40.

The benchmark harness now generates a valid current copy-only fixture and
records CPU, peak RSS, and output bytes. The bounded run used the existing
engine binary and completed five times. No SQL index or pool-size change was
made; the small synthetic SQLite pool results overlap and are not production
query-plan evidence.
