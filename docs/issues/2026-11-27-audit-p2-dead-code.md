# P2 — Verify and remove unreachable legacy code

**Status:** Verified — current-head candidate review and safe removals complete
**Area:** Repository-wide, prioritized by production call paths
**Source:** Preliminary audit lists dead code and legacy paths as unverified.

## Problem

The audit has not certified dead code. A symbol with no obvious in-repository
caller may still be invoked through registration, reflection, build tags,
plugins, generated code, external APIs, scripts, configuration, or operational
procedures. Removing code without checking those paths can break a supported
contract or leave a partial migration.

## Work plan

1. Start from one candidate at a time and classify it: exported symbol,
   cross-package helper, route/CLI, unexported function, compatibility facade,
   generated artifact, script, or data/schema path.
2. Search all source, tests, build tags, CI/workflows, config, docs, scripts,
   generated registries, and runtime registration points for references.
3. For externally reachable candidates, check telemetry and operator/customer
   usage evidence. Apply C1/C2 and the decision rule in `AGENTS.md` and ADR
   0008: soft-deprecate when both conditions hold; otherwise remove atomically.
4. Check startup/bootstrap wiring and indirect dispatch, not only direct text
   references. For routes and CLI commands, inspect registration tables and
   deployment scripts.
5. For a removal, update docs/config/tests in the same logical layer, remove
   dead compatibility callers first when needed, and preserve historical audit
   records where repository policy requires.
6. Run the required full-module removal gate before pushing any commit that
   retires an exported symbol or cross-package helper.

## Acceptance criteria

- Every removed candidate has recorded evidence that it is unreachable or
  outside the supported contract, including indirect entrypoints checked.
- Customer/operator-facing behavior has either a documented migration and
  sunset or a justified atomic removal under ADR 0008.
- No dead reference remains in live source, CI, scripts, config, or current
  docs; historical records are classified rather than blindly rewritten.
- Removal is atomic and passes `bash scripts/ci/pre-removal-verify.sh` when
  required by `AGENTS.md`.
- No broad cleanup commit mixes unrelated removals.

## Verification

Use `rg`, `go list`, build tags, route/command registration inspection, and
runtime telemetry where available. Run focused tests for each removal and the
mandatory full DataServer gate for qualifying removal commits. Record any
external-use uncertainty instead of claiming dead code prematurely.

## Review result — 2026-11-27

The raw analyzer output is preserved in
[`deadcode-candidates-2026-11-27.txt`](../architecture/evidence/deadcode-candidates-2026-11-27.txt).
It is a candidate list, not proof: its snapshot predates several removals
already present on `main`, and it reports interface implementations and test
doubles as unreachable. Each candidate was checked against current production
references, interface contracts, route registration, test-only use, and the
repository's `internal` package boundaries. The decision log is in
[`dead-code-review-2026-11-27.md`](../architecture/dead-code-review-2026-11-27.md).

Confirmed dead production paths removed:

- The retired `POST /api/v1/pipeline-runs` creation handler, service, and
  bootstrap wiring. `routes.go` already retired the route; no production or
  test caller used the creation service.
- The unused per-worker `current_task` SQL loader and API adapter. The active
  worker response uses the distinct registry-backed `current_task_id` field.
- Three unused fleet surfaces: the SSH constructor that bypassed the canonical
  worker registry, the unused runtime-preflight string helper, and the
  superseded `FreshSmokeRunner` path.
- The unused config-based Cosign constructor. Production bootstrap uses the
  base constructor and applies the typed settings in its wiring.

Each removal was committed atomically and passed
`bash scripts/ci/pre-removal-verify.sh` (full-module vet, build, and tests)
before push. Commits: `ec70fc6c`, `8ed374b3`, `40fee3dc`, `be1695f1`.

Other candidate lines were classified rather than removed mechanically:

- Already absent on the reviewed `main`: `internal/jobs/ingress/registry.go`
  (`90cf4c9e`), `http_response_compat.go` (`be8b3f04`), and
  `DataLayerAuditor.IsLegacyAllowed` (`4735bbf3`).
- Live through real callers or composition: `BlobStore.RemoveStaging`,
  `LoadCurrentTaskRow` before its removal, `ConfigFromRuntime`, and the
  forwarding, delivery, and task lifecycle methods wired by bootstrap or
  called through domain interfaces.
- Required Go contract methods, including `Error()` implementations and
  methods satisfying `BackoffPolicy` or runtime interfaces, are not dead just
  because a text search finds no concrete receiver call.
- Test stubs and clock/drive fakes are test support and remain in place.
- `SQLiteStore.observeDBTransaction` was confirmed live during the original
  review and remains present.

No public route or published-module API was found among the removed symbols;
their packages are under `internal/`. Under `AGENTS.md` §2 / ADR 0008, C2
therefore fails and no soft-deprecation period was required. This closes the
audit finding; new candidates must be reviewed against the current tree rather
than reusing the stale analyzer snapshot.
