# P2 — Verify and remove unreachable legacy code

**Status:** Open — no safe removal certified
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

## Review result

No production symbol was removed by this audit. The available dead-code
candidate output was not sufficient to prove unreachability across bootstrap,
reflection, generated registration, scripts, and external callers; a direct
reference check confirmed at least one apparent candidate,
`SQLiteStore.observeDBTransaction`, is live. Under `AGENTS.md` §2 and ADR 0008,
uncertain candidates must be retained rather than turning an analyzer warning
into an unsafe removal. A candidate-by-candidate inventory with complete
indirect-call evidence remains necessary before this ticket can close.
