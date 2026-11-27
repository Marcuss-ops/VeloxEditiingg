# P1 — Review `internal/store` package cohesion and ownership

**Status:** Backlog
**Area:** `DataServer/internal/store`
**Source:** Preliminary audit reported 156 non-test source files; this is a
hotspot for inspection, not proof of poor cohesion.

## Problem

The package contains many persistence files and compatibility surfaces. A
large file count can reflect one cohesive SQLite adapter, several separate
bounded contexts, or historical facades. Splitting by file count alone could
add import edges and move SQL without reducing shared knowledge or change
risk. First identify actual ownership and coupling.

## Work plan

1. Reproduce the count with an explicit command/filter: tracked Go production
   files only, excluding tests and generated files. Record the exact result and
   date; do not mix all package files with source-only count.
2. Group files by entity/state ownership and responsibilities: migrations,
   jobs/tasks/attempts, artifacts/uploads, deliveries/publications, telemetry,
   configuration, and compatibility facades.
3. For each group, map exported contracts, concrete SQL writers, callers,
   transaction helpers, shared DB handles, and cross-group imports.
4. Use current `OWNERSHIP.md`, ADRs, and store package docs to distinguish
   canonical adapters from compatibility aliases. Verify each listed alias
   still has callers before proposing removal.
5. Review the top changed files over a representative history window and
   identify files that repeatedly change together for unrelated reasons.
6. Propose the smallest change that reduces coupling: delete a proven dead
   facade, move a contract only when ownership is wrong, or keep the cohesive
   package intact. State the cost and import-direction consequences.

## Constraints

- Do not split the package solely to reduce its file count.
- Do not move SQL across transaction owners or weaken atomicity/CAS behavior.
- Do not create a parallel persistence interface for an existing canonical
  entity.
- Any exported/cross-package symbol migration must follow the ADR 0008
  soft-deprecate/removal rule and the full-module pre-removal gate.

## Acceptance criteria

- The file count and counting filter are reproducible.
- A compact ownership map identifies which responsibilities share the package
  and why.
- Cross-package dependencies, high-fan-in contracts, compatibility facades,
  and known change-coupling hotspots are documented with examples.
- Each proposed refactor has evidence that it reduces coupling or duplicate
  authority; proposals based only on LOC/file count are rejected.
- The outcome is either a small implementation ticket with clear boundaries
  or an explicit decision to retain the package structure.

## Verification

Use existing `go list`/`rg` tooling and `scripts/metrics/refactor_metrics.py`
where useful. Do not add a new analyzer for the survey. For implementation,
run focused store tests and the module gate required by the change scope.
