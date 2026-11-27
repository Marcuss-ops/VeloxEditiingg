# P1 — Map lifecycle state writers and transition ownership

**Status:** Verified inventory; no separate duplicate writer confirmed
**Area:** Jobs, tasks, attempts, artifacts, publications, deliveries
**Source:** Preliminary audit at `4d42269c`; repository ownership rules in
`docs/architecture/OWNERSHIP.md` and state contracts in domain packages.

## Problem

The preliminary audit found potentially competing job-success paths and
requested a map of all writers for Job, Task, Attempt, Artifact, and
Publication states. Without a current inventory, reviewers cannot reliably
tell whether a direct SQL update is a legitimate persistence adapter, a
second business authority, or an obsolete path. The map should identify
semantics and call paths, not merely grep matches.

## Work plan

1. Inventory status constants and transition contracts in domain packages.
2. Search production Go and SQL/migration code for state writes, including
   `UPDATE ... status`, insert-time status defaults, compare-and-swap helpers,
   trigger-like logic, and state changes hidden in query builders.
3. For each write, record entity, old/new state, file/function, caller or
   scheduler, transaction boundary, guards/CAS, emitted event, and canonical
   owner.
4. Trace the call graph from HTTP/gRPC/background entrypoints to the writer;
   distinguish adapters from independent business decisions.
5. Compare each transition against the relevant state-machine docs and
   `docs/architecture/OWNERSHIP.md`. Resolve stale docs instead of preserving
   contradictory authorities.
6. Classify findings as: canonical, adapter-only, duplicate decision,
   undocumented transition, dead/unreachable, or uncertain. Confirm uncertain
   items before editing.
7. For confirmed duplicate decisions, open a narrowly scoped follow-up and
   migrate callers to the canonical owner before removing the old writer.

## Deliverable

Add or update a concise lifecycle ownership table under
`docs/architecture/OWNERSHIP.md` or the relevant state-machine document. Link
each row to the canonical domain contract and persistence adapter. Keep the
inventory useful for review; do not paste an unbounded grep dump.

## Acceptance criteria

- Jobs, tasks, attempts, artifacts, publications, and deliveries each have a
  documented state owner and persistence writer boundary.
- Every production state mutation found by the inventory has a known caller,
  transaction/CAS guard, and event behavior, or is explicitly marked for
  investigation.
- Duplicate business decisions and unauthorized writers are converted into
  separate implementation tickets with reproduction evidence.
- Domain docs and ownership docs agree on terminal and nonterminal states.
- A repeatable search/check is documented; no new static-analysis framework
  is added for this inventory.

## Verification

Review the inventory against `rg` results for all status columns and state
constants in the relevant packages. For code changes, run focused lifecycle
tests; any helper removal follows `AGENTS.md`'s full-module gate.

## Evidence and limits

- The ownership map is in
  [`2026-11-27-lifecycle-writer-map.md`](../architecture/2026-11-27-lifecycle-writer-map.md)
  and the canonical job rows in `OWNERSHIP.md`.
- The review identifies transaction adapters that independently persist
  cross-entity state but share explicit domain decisions and evidence gates;
  it does not claim one SQL statement or one transaction owner for every
  entity.
- The repeatable `rg` review command and its dynamic-SQL/test limitations are
  recorded in the map. No additional independent job-success decision was
  found after consolidating finalization target selection and the
  `DELIVERING` roll-up.
- Focused lifecycle verification passed for `internal/jobs`, `internal/store`,
  `internal/completion`, `internal/artifacts`, and `internal/artifactsstore`.
