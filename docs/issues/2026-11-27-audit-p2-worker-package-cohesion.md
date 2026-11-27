# P2 — Review worker package cohesion and responsibility boundaries

**Status:** Verified — retain current boundary
**Area:** `RemoteCodex/native/worker-agent-go/internal/worker`
**Source:** Preliminary audit reported 95 non-test source files; count alone
does not establish an architectural defect.

## Problem

The worker package is a candidate for responsibility and dependency review.
The audit has not yet shown whether its files form a cohesive runtime owner or
mix lifecycle, protocol, execution, resource management, and persistence
concerns. A package split that only relocates files would increase navigation
and dependency cost without simplifying behavior.

## Work plan

1. Reproduce the production Go file count with the exact tracked-file and
   generated/test exclusion rules.
2. Group files by lifecycle: bootstrap/configuration, worker identity and
   registration, task/lease processing, executor dispatch, heartbeat,
   telemetry, resource cleanup, and protocol transport.
3. For each group, record public/internal APIs, callers, shared mutable state,
   goroutines/channels, resource ownership, and dependency direction.
4. Trace representative end-to-end operations from gRPC admission through
   execution, result reporting, cancellation, and cleanup.
5. Identify concrete pain: duplicated decisions, circular conceptual
   ownership, frequent unrelated co-changes, or lifecycle/resource hazards.
6. Recommend no change or a narrowly scoped boundary adjustment. Keep runtime
   orchestration together when its ordering and cancellation behavior are
   easier to reason about in one place.

## Acceptance criteria

- The reported count is reproducible and its filter is recorded.
- A responsibility map names the owner and callers of each major worker
  lifecycle concern.
- Resource and concurrency ownership (goroutines, contexts, channels, files,
  subprocesses, and credentials where relevant) is visible in the map.
- Any package split is justified by dependency/change evidence and has a
  one-way import direction; no split is made solely to reduce the count.
- The review yields either a concrete small follow-up or a documented
  retain-as-is decision.

## Verification

Use `go list` and the existing refactor metrics. For any code change, run the
worker module's focused tests, race checks where concurrency behavior changes,
and relevant repository CI guards. Do not introduce a new package-analysis
framework for this ticket.

## Evidence

The reproducible count, lifecycle/resource ownership map, one-way import
direction, high-frequency co-change examples, and retain-as-is decision are in
[`2026-11-27-package-cohesion-review.md`](../architecture/2026-11-27-package-cohesion-review.md).
Worker session orchestration shares cancellation, lease, active-task, and
result-reporting state; no independent lifecycle boundary was demonstrated
that would justify a package split.
