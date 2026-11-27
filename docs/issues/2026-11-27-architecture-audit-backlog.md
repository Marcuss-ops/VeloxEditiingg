# Architecture audit follow-up tickets — 2026-11-27

These tickets turn the preliminary audit at commit `4d42269c` into bounded,
reviewable work. The audit identified candidate defects and hotspots; each
ticket must verify current call paths and contracts before changing code.
File counts are inspection signals, not reasons by themselves to split a
package or file.

## Priority order

| Priority | Ticket | Outcome | Status |
| --- | --- | --- | --- |
| P0 | [Canonical publication success validation](2026-11-27-audit-p0-publication-success-validation.md) | Every path accepting `PUBLISHED` enforces the same evidence contract. | Verified |
| P0 | [Completion must respect pending deliveries](2026-11-27-audit-p0-job-success-with-pending-deliveries.md) | A job with required delivery work is not exposed as terminal success. | Verified |
| P1 | [Unify completion delivery creation policy](2026-11-27-audit-p1-delivery-creation-policy.md) | Both creation paths agree on identity, retry budget, and conflict handling. | Verified |
| P1 | [Map lifecycle state writers](2026-11-27-audit-p1-lifecycle-writer-map.md) | Each state has one documented owner and authorized transition path. | Verified inventory; no duplicate decision remains in reviewed lifecycle paths |
| P1 | [Type runtime finalizer contracts](2026-11-27-audit-p1-runtime-finalizer-types.md) | Required dependencies and internal binding data are explicit and typed. | Verified at decision boundary; persisted wire map retained |
| P1 | [Review store package cohesion](2026-11-27-audit-p1-store-package-cohesion.md) | Store ownership and internal boundaries are evidence-based and clearer. | Verified; retain current boundary |
| P2 | [Review worker package cohesion](2026-11-27-audit-p2-worker-package-cohesion.md) | Worker package responsibilities and candidate boundaries are documented. | Verified; retain current boundary |
| P2 | [Measure complexity and change coupling](2026-11-27-audit-p2-change-coupling-metrics.md) | Refactor priorities use reproducible code and history metrics. | Verified; report in `docs/metrics/2026-11-27-architecture-audit-metrics.md` |
| P2 | [Verify and remove unreachable legacy code](2026-11-27-audit-p2-dead-code.md) | Only proven unreachable code is removed under repository rules. | Verified; current-head review and safe removals recorded |
| P2 | [Profile persistence and runtime hot paths](2026-11-27-audit-p2-performance-profiling.md) | Optimizations target measured bottlenecks with repeatable evidence. | Verified; no optimization justified by current evidence |
| P3 | [Consolidate residual duplication and needless abstractions](2026-11-27-audit-p3-simplification.md) | Confirmed duplication is reduced without adding abstraction layers. | Verified for confirmed lifecycle/runtime findings |

## Suggested sequence

The reviewed P0/P1 lifecycle and ownership tickets are verified. The dead-code
candidate list was reconciled with current `main`; proven retired paths were
removed and the remaining analyzer hits were classified. Package reviews and
metrics support retaining the current store and worker boundaries.
The current production profiling decision is recorded; it identifies readiness
wait and cold transfer as the observed delays and does not justify speculative
code changes. The analyzer output is retained as historical evidence; future
candidates must be generated and reviewed against the current tree.

## Repository constraints

- Keep work on `main`, use atomic commits, and push each completed modification
  as required by `AGENTS.md`.
- Before pushing a commit that removes an exported symbol or cross-package
  helper, run `bash scripts/ci/pre-removal-verify.sh` successfully.
- Apply the soft-deprecate/removal decision in `AGENTS.md` and ADR 0008 to
  externally reachable symbols.
- Do not treat the reported file counts as a mandate to split packages.
- No ticket authorizes production schema changes, new frameworks, or broad
  rewrites without a demonstrated contract or measured need.
