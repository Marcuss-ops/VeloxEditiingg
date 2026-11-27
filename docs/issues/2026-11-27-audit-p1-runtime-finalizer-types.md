# P1 — Type runtime finalizer dependencies and internal bindings

**Status:** Partial; typed dependency fixed, binding schema remains
**Area:** `DataServer/internal/handlers/server/pipeline/runtime_finalize.go`
**Source:** Preliminary audit at `4d42269c`; dynamic dependency field,
reflection nil checks, type assertions, and map-based bindings.

## Problem

The runtime finalization processor stores `assets` as `interface{}` even
though its constructor accepts a typed planner. It uses reflection to detect
typed nil values and type assertions to require additional planner
capabilities. Runtime bindings are represented as `[]map[string]interface{}`
and state keys are strings. These patterns move contract errors from compile
time to runtime and spread the binding schema across orchestration code.

Some dynamic values may be intentional at JSON request/persistence boundaries.
The goal is to type internal contracts without inventing wrappers for values
that are genuinely wire data.

## Required investigation

1. List every method called through `assets`, all type assertions, and all
   consumers/producers of runtime binding maps.
2. Identify which structures are persisted or transported as JSON and which
   exist only inside the processor.
3. Check bootstrap construction and tests for nil or typed-nil implementations.
4. Identify string keys that are part of a stable wire contract versus private
   orchestration state. Do not rename/change externally persisted keys as part
   of a typing-only change.
5. Review package dependency direction and existing contract types before
   introducing new types.

## Implementation plan

1. Define the smallest interface that contains all required planner methods
   already consumed by the processor, and accept/store that interface
   directly. Prefer an existing package contract if one fits.
2. Remove redundant runtime type assertions that the constructor can make
   impossible. Keep the typed-nil check only if it protects a real boundary;
   otherwise simplify it rather than building a general reflection utility.
3. Introduce a typed binding struct only for internal fields whose shape is
   stable and used semantically by this processor. Convert to/from JSON maps
   at the existing boundary and preserve unknown-field behavior if required.
4. Replace private string state keys with constants or a small typed enum only
   where that reduces accidental key drift. Avoid a broad state framework.
5. Add compile-time interface assertions for concrete implementations where
   useful, and tests that malformed boundary input is rejected clearly.

## Acceptance criteria

- Required processor dependencies are fully represented by constructor types;
  normal production operation does not depend on unchecked `interface{}`
  assertions.
- Internal binding fields have a named schema where they drive business
  behavior; JSON conversion remains at persistence/transport boundaries.
- Stable wire/persisted keys and compatibility behavior are unchanged unless
  separately versioned and approved by their owning contract.
- Typed-nil handling is explicit and limited to necessary boundaries.
- Changes reduce runtime branching/assertions without adding generic
  reflection or wrapper layers.
- Focused runtime finalization tests cover valid, absent, and malformed inputs.

## Verification

Run pipeline finalization tests and any contract serialization tests affected
by the boundary conversions. If the work retires an exported or cross-package
helper, run the full pre-removal gate before push.

## Implementation evidence

- `RuntimeFinalizationProcessor.assets` and its constructor now require the
  stock-window planner, source-certification planner, and source-certificate
  enricher as one typed dependency contract.
- Runtime type assertions for these required operations were removed; bootstrap
  and unit-test implementations are checked by the compiler.
- Focused verification passed: `go test -count=1
  ./internal/handlers/server/pipeline ./internal/assets`; full-module vet,
  build, and tests passed in `bash scripts/ci/pre-removal-verify.sh`.
- The persisted `[]map[string]interface{}` binding schema remains open for a
  separate boundary-focused change; this ticket is not complete until that
  representation is either typed or justified as wire-boundary data.
