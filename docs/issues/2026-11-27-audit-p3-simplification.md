# P3 — Consolidate confirmed duplication and remove needless abstractions

**Status:** Verified for confirmed lifecycle/runtime findings
**Area:** Repository-wide, after P0/P1 contracts and evidence work
**Source:** Preliminary audit recommends eliminating abstractions with no
useful role after invariants are consolidated.

## Problem

The audit's end state is fewer independent business decisions and less
cognitive overhead, not more layers or smaller files at any cost. Some
similar-looking code may intentionally enforce different provider, storage,
or protocol contracts. This ticket is a final evidence-based cleanup pass,
not permission to deduplicate code mechanically.

## Work plan

1. Revisit confirmed findings from the P0/P1 tickets after their canonical
   contracts land. Identify remaining duplicated predicates, retry rules,
   serializers, state constants, and adapters.
2. For each candidate, document whether behavior, transaction boundary,
   failure classification, and caller contract are actually equivalent.
3. Search for existing owners/helpers before introducing a new abstraction.
   Prefer removing duplicated decision-making or unused indirection at the
   current owner over adding a generic framework.
4. Keep provider-specific or boundary-specific behavior separate when the
   contracts differ; make the difference explicit in names/docs/tests.
5. Keep cohesive large files together. Split only when the new boundary
   reduces change coupling, makes ownership clearer, or isolates a lifecycle
   that can be understood and tested independently.
6. Measure the refactor with the existing `refactor_metrics.py` and inspect
   the actual diff to ensure complexity was removed, not moved into wrappers.

## Acceptance criteria

- Each simplification has a concrete before/after description of the
  duplicated decision or indirection removed.
- Semantically different provider and lifecycle contracts remain explicit and
  are not collapsed into a false common rule.
- No abstraction is added unless it reduces the number of business authorities
  or gives a real boundary a clear owner.
- No file/package split is justified solely by line or file count.
- Existing behavior is pinned by focused tests before/after the refactor.
- Metrics and code review show reduced or better-localized complexity, with no
  equivalent duplication introduced elsewhere.

## Verification

Run the focused tests for the affected contract and the repository's existing
refactor metrics against a known base ref. If an exported/cross-package helper
is retired, follow ADR 0008 and run `bash scripts/ci/pre-removal-verify.sh`.

## Evidence

- The duplicate publication acceptance predicate now has one owner, and the
  delivery callback/reconciliation paths share one transaction roll-up.
- `jobs.FinalizationTargetStatus` centralizes the finalization choice without
  moving transaction boundaries. Runtime source binding comparisons use a
  typed projection while the wire map remains intact.
- The refactor metrics and their limits are recorded in
  [`2026-11-27-architecture-audit-metrics.md`](../metrics/2026-11-27-architecture-audit-metrics.md).
  The report measures the earlier committed lifecycle fixes; no claim is made
  that this small follow-up reduced repository-wide LOC or complexity.
- Focused tests passed for the affected jobs, delivery roll-up, completion,
  artifact, and runtime pipeline paths. No package split or speculative
  abstraction was introduced.
