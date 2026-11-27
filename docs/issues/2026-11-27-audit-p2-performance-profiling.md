# P2 — Profile SQL, allocations, rendering, I/O, and concurrency

**Status:** Open — production workload profile unavailable
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

Existing August 2026 benchmark/profile documents provide historical reference
data, but they do not profile the current November worker and renderer build.
The current baseline script was attempted in this session, but it produced no
complete artifact, workload record, or repeatable result. No current
production-like assets, worker hardware profile, or concurrency run was
available in the workspace. The OOM report from the earlier run makes another
unbounded benchmark inappropriate without a bounded workload and explicit
resource limits. No performance optimization is justified from the available
evidence; this ticket needs a bounded representative run before it can meet its
acceptance criteria.
