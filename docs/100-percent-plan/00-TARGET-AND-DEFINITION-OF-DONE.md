# Velox — Target 100% and Definition of Done

Status: canonical execution plan  
Owner: Velox architecture  
Last reconciliation: 2026-06-26

This directory is the only completion roadmap for Velox. Historical audits, temporary PR plans and snapshot ticket lists must not be used as active implementation contracts.

A checkbox may be marked complete only when the change is merged into `main`, the required tests pass and the evidence is reproducible. Code that merely compiles is not complete.

## What Velox must become

Velox is a headless, deterministic, server-side distributed video generation and composition runtime.

The completed system must:

- accept a project or generation request and compile it into an immutable render contract;
- expand one Job into a persistent Task DAG;
- execute independent work in parallel on CPU-first remote workers;
- select workers from canonical executor capabilities, current resources, measured cost and data locality;
- reuse verified content-addressed artifacts and precompositions;
- recover automatically from master restart, worker crash and network partition;
- declare success only after the final artifact is present, verified and READY;
- expose enough telemetry to explain every placement, retry, failure and performance result;
- promote the same signed image digest from staging to production without rebuilding;
- remain usable without a graphical editor, browser renderer or GPU dependency.

## Non-negotiable invariants

- One state owner, one writer and one mutation path for every important responsibility.
- The master plans, schedules and owns Task lifecycle state.
- Workers execute assigned contracts and never invent graph edges or reinterpret projects.
- Registries own executor, resolver, provider, compiler, estimator and sampler selection.
- SQLite stores authoritative state and metadata; BlobStore stores binary payloads.
- Memory structures are reconstructible and never authoritative.
- Job `SUCCEEDED` is written only by verified artifact finalization.
- Task execution identity is the tuple `task_id`, `attempt_id`, `worker_id`, `lease_id` and revision where applicable.
- No silent fallback, dual-write, hidden retry path or parallel renderer implementation.
- CPU-only workers are first-class; GPU support is an explicit capability, never an implicit requirement.
- Production gRPC is mTLS fail-closed.
- Every production release is traceable to commit, version, image digest, SBOM, provenance and signature.

## Completion policy

For every item below, evidence must include the relevant combination of:

- merged commit or pull request;
- targeted unit and invariant tests;
- integration or E2E output;
- database assertions;
- logs and metrics;
- rendered fixture, `ffprobe` output and SHA-256;
- image digest, SBOM, provenance and signature;
- recovery or failure-injection report.

Do not check an item based only on a document, mock or manual observation.

## Master gates

### Gate 1 — Runtime correctness and recovery

Detailed checklist: [01-RUNTIME-CONSISTENCY-AND-RECOVERY.md](01-RUNTIME-CONSISTENCY-AND-RECOVERY.md)

- [ ] `main` passes the canonical verification command from a clean checkout.
- [ ] Job, Task, TaskAttempt, lease and artifact state cannot diverge after partial failure.
- [ ] Expired leases always produce a durable retry or a durable terminal result.
- [ ] Late or duplicate reports cannot replace the winning attempt.
- [ ] Master restart, worker crash and network partition recover automatically.
- [ ] Drain and shutdown leave no orphan process or READY partial artifact.
- [ ] Final artifact verification remains the only success gate.

### Gate 2 — CI, E2E and release integrity

Detailed checklist: [02-CI-TESTING-AND-RELEASE.md](02-CI-TESTING-AND-RELEASE.md)

- [ ] Go, C++, architecture, migration and security checks run in the canonical CI path.
- [ ] Native C++ tests are executed, not only compiled.
- [ ] Real gRPC workload E2E is a required check.
- [ ] Production-like mTLS E2E is a required release check.
- [ ] Missing mandatory dependencies fail CI instead of skipping critical tests.
- [ ] Master and worker images are reproducible, signed and promoted by digest.
- [ ] Branch protection blocks merge without all required checks.

### Gate 3 — Production operations and security

Detailed checklist: [03-PRODUCTION-OPERATIONS-AND-SECURITY.md](03-PRODUCTION-OPERATIONS-AND-SECURITY.md)

- [ ] Every worker passes a machine-readable production doctor.
- [ ] Worker identity, certificate and allowlist membership are unambiguous.
- [ ] Liveness and readiness are separate and used correctly by containers and operators.
- [ ] Capacity and admission control prevent CPU, memory and disk overcommit.
- [ ] Metrics, alerts and correlated logs cover the complete runtime.
- [ ] PKI monitoring, rotation, revocation, rollout and rollback are tested.
- [ ] Every supported hardware class passes canary and soak certification.

### Gate 4 — Distributed rendering, performance and scale

Detailed checklist: [04-DISTRIBUTED-RENDERING-PERFORMANCE-AND-SCALE.md](04-DISTRIBUTED-RENDERING-PERFORMANCE-AND-SCALE.md)

- [ ] One Job expands into a real persistent multi-Task DAG.
- [ ] Independent layers execute before unrelated slow inputs are available.
- [ ] Verified precompositions are reused by deterministic content hash.
- [ ] Placement uses capability, resources, cost and locality.
- [ ] Safe work can be temporally sharded and recomposed without a full rerender.
- [ ] Critical path, parallel efficiency, cache hit ratio and estimation accuracy are measured.
- [ ] Representative workloads meet defined correctness and performance SLOs.

## Required execution order

1. Restore and enforce a clean green baseline.
2. Close runtime consistency and recovery gaps.
3. Make real E2E and native tests required.
4. Complete production doctor, security and operational certification.
5. Complete distributed DAG, cache, scheduling, sharding and performance work.
6. Run release candidate recovery suite and soak tests.
7. Promote one canary worker by digest.
8. Expand by hardware class only after the canary evidence is green.

A later phase may be developed in parallel only when it does not change an owner, schema or contract required by an earlier phase.

## Pull request rules

- Start from current `origin/main` after `git fetch origin`.
- One responsibility and one focused set of files per PR.
- Search for existing code before adding new code.
- Extend canonical registries, resolvers, samplers and repositories; do not duplicate them.
- Rebase frequently on `origin/main`.
- Run targeted tests plus the canonical verification gate.
- Do not commit generated videos, output directories, caches or build products.
- After push, inspect `git log -n 5 --oneline` and the remote diff.
- Do not merge a documentation claim unless the code and evidence agree with it.

## Final 100% verdict

Velox reaches 100% only when every master gate is checked and the final release candidate demonstrates all of the following in one reproducible run:

```text
Doctor = READY
mTLS workload E2E = PASS
Job = SUCCEEDED
All required Tasks = SUCCEEDED
Winning TaskAttempts = SUCCEEDED
Final Artifact = READY
Artifact SHA-256 = verified
Master restart recovery = PASS
Worker crash recovery = PASS
Network partition recovery = PASS
Drain and shutdown = PASS
Duplicate final artifacts = 0
Lost jobs = 0
Orphan terminal tasks = 0
Production fallback count = 0
24-hour soak = PASS
Image digest promotion and rollback = PASS
```
