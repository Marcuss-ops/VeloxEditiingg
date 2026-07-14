# Contributing to VeloxEditingg

> **Single source of truth for the file-size policy:**
> [`docs/metrics/loc-baseline.md` §11](docs/metrics/loc-baseline.md#11-threshold-policy-proposed).
> This document is the contributor-facing summary; the canonical
> numbers live in §11 and are enforced by the CI gates in §1.2 below.
> Re-run the §12 methodology after every refactor round and append a
> `§<Round N>` delta so the audit log stays complete.

This file captures the day-to-day engineering practices and the
mechanical gates that protect the repository from second-system
effects. For architecture / agent contract / ownership, see:

- [`docs/architecture/CURRENT-TO-TARGET-ARCHITECTURE.md`](docs/architecture/CURRENT-TO-TARGET-ARCHITECTURE.md) — OTG architecture target
- [`docs/architecture/AGENT-CONTRACT.md`](docs/architecture/AGENT-CONTRACT.md) — agents, roles, contracts
- [`docs/architecture/OWNERSHIP.md`](docs/architecture/OWNERSHIP.md) — package-level single-owner map

---

## 1. File-size policy (enforced in CI)

### 1.1 Thresholds

| File kind | Warn at | Refactor required at |
| --- | ---: | ---: |
| Production Go (`*.go` excluding `*_test.go`) | **600** | **900** |
| Test Go (`*_test.go`) | **900** | **1 200** |
| Shell scripts (`*.sh`, `*.bash`) | 400 | 700 |
| Documentation (`*.md`, excluding `docs/archive/`) | 800 | 1 200 |
| CI / Ansible YAML (`*.yml`, `*.yaml`, excluding `.github/workflows/`) | 400 | 800 |

- The **warn** tier triggers a `::warning` annotation in the CI
  output. It is informational; no build break.
- The **refactor required** tier is the hard cap (`::error`). A new
  file over this threshold fails the CI gate unless allow-listed
  (§1.3).
- Production Go's warn at 600 is also enforced as the
  [`golangci-lint` `funlen` `lines: 600`](.golangci.yml) rule, which
  surfaces findings directly in the IDE / PR UI.

### 1.2 Exclusions (do **not** count toward the threshold)

The CI gate and the `funlen` lint rule explicitly skip these path
patterns. They may be arbitrarily long:

- **Generated code.** Files whose first line is a `Code generated …
  DO NOT EDIT.` header — proto output under
  `shared/controltransport/pb/`, mockgen output, deep-copy
  generators, anything under `*.pb-cache/`.
- **Fixture / seed data.** Large seed JSON / SQL fixtures checked
  into the repo (e.g. `cmd/seed-velox-db-fixture/...`). Mark as a
  fixture in the per-package godoc so reviewers know it's
  intentional.
- **Archived docs.** Anything under `docs/archive/`. The archive is
  the intended overflow sink for stale architectural notes that have
  moved to a current document.
- **CI workflows themselves.** Anything under `.github/workflows/`.
  These define the verification pipeline and may grow as the
  pipeline grows.
- **Test infra shell scripts.** `tests/e2e/**` runbook shell scripts
  that orchestrate end-to-end setup/teardown — keep readable, do
  not let them grow past 632 LOC without splitting.

### 1.3 CI enforcement — two prongs

1. **`funlen` (`golangci-lint` rule).** [`.golangci.yml`](.golangci.yml)
   enables `funlen: lines: 600` for production Go. Functions longer
   than 600 lines surface as lint findings but do not block the
   build (warn-only).
2. **`scripts/ci/check-loc-thresholds.sh`.** Runs in
   [`.github/workflows/ci.yml`](.github/workflows/ci.yml) as the
   `LOC threshold gate` step (`if: always()` — runs even if earlier
   steps fail). Exits 1 if any non-allow-listed tracked source file
   exceeds its warn threshold. Annotates with:
   - `::warning file=<path>::…` — known carry-over (allow-listed).
   - `::error file=<path>::…` — new violation. CI fails on first push.

The script anchors at the repo root via
`git rev-parse --show-toplevel` and normalises `find`'s `./X` output
to `X` before matching against the allow-list, so a single entry
covers both relative and absolute resolutions.

### 1.4 KNOWN_VIOLATIONS allow-list + workflow

A small number of files are kept on the allow-list because their
refactor is scheduled in the ROADMAP but not eligible to land in the
current round (legacy archives, third-party scripts, deferred
work). Each allow-list entry is paired with a tracking-ref in
[`docs/metrics/loc-baseline.md` §15.2](docs/metrics/loc-baseline.md#152-known_violations-allow-list-9-entries-2-sub-arrays):

- 3 **legacy baselines** — pre-date the gate by years; explicit
  follow-up in §13 roadmap. Examples:
  `docs/architecture/CURRENT-TO-TARGET-ARCHITECTURE.md`,
  `deploy/runtime/checklist-verify.sh`,
  `scripts/cert/certify-worker-2c-2d.sh`.
- 6 **round-1 surfacings** — flagged by the first full-tree scan
  after the cd-anchor fix on 2025; one explicit atomic refactor per
  entry.

When you split one of these files:

1. Remove the allow-list entry from
   `KNOWN_VIOLATIONS_*` in
   [`scripts/ci/check-loc-thresholds.sh`](scripts/ci/check-loc-thresholds.sh).
2. Append a `§<Round N>` delta to
   [`docs/metrics/loc-baseline.md`](docs/metrics/loc-baseline.md)
   with file → before → after → commit SHA → tracking-ref.
3. Re-run the §12 methodology and refresh the §10a / §10b hotspot
   tables.

### 1.5 Refactor workflow (one atomic commit per extracted file)

Each refactor round follows the project rules. The CI gate is part
of the iteration loop:

1. **Refactor locally.** Split the file by responsibility, keep
   the public API identical. Same package split (multiple
   `<name>.go` files in the same directory) is preferred over
   sub-package splits because it keeps cross-file private-symbol
   access for free.
2. **Typecheck and test.** For DataServer:
   ```bash
   cd DataServer
   go build ./...            # every package must compile
   go vet ./internal/...     # iface-mismatch + dead-code surface
   go test -count=1 -timeout 180s ./internal/<domain>/...
   ```
   For worker-agent-go, swap `DataServer` for the worker module
   directory.
3. **Run the LOC gate locally first.**
   ```bash
   bash scripts/ci/check-loc-thresholds.sh
   ```
   Should exit 0 with the same number of `::warning` annotations
   that CI emits (the new commit does not introduce an entry).
4. **Commit on `main`** as ONE atomic commit naming the file
   extracted (no PRs, no branches, no `--amend`, no `git rebase`
   once on `main`). Conventional-commits prefix: `refactor(<pkg>):`.
5. **Push immediately** to `origin/main`. CI runs on the push and
   emits annotations back to the PR / commit UI.
6. **Append the round delta** to
   `docs/metrics/loc-baseline.md` as a SECOND atomic commit on
   `main` + push (no PRs, no branches, no `--amend`).

The CI gate prevents the next commit from being green if a NEWLY
added file breaches the threshold unless it is added to
`KNOWN_VIOLATIONS`. The gate does **not** retroactively flag files
that were on the allow-list; it only fires on new violations.

---

## 2. Engineering conventions (short list)

- **No branch.** Work lands on `main` directly. Branch protection
  is exercised via the `make verify` pipeline + the LOC gate, not
  via PR review.
- **No `--amend`, no force-push.** Once on `main`, history is
  linear; the round delta (`docs/metrics/loc-baseline.md` §<n>) is
  the audit log.
- **One atomic commit per file extracted.** Splits run sequentially
  as separate commits, each verifying the package compiles + tests
  pass + the gate stays green. The composer-aggregate commit
  happens at the round boundary in `docs/metrics/loc-baseline.md`,
  not on the source tree.
- **`velox-server/...` import paths.** The DataServer Go module is
  rooted at `velox-server/`; never introduce `DataServer/...`
  import paths in code.
- **Package godoc + ownership.** Every top-level package has a
  package-level godoc naming the single owner per
  [`docs/architecture/OWNERSHIP.md`](docs/architecture/OWNERSHIP.md).
- **Always preflight.** Run `make verify` before push. CI runs the
  same script and the same `golangci-lint` config locally; the
  surface-level drift between local and CI is zero.

---

## 3. Where to put new code

| Kind | Path | Notes |
| --- | --- | --- |
| Production Go (server-side) | `DataServer/internal/<domain>/` | Per [`OWNERSHIP.md`](docs/architecture/OWNERSHIP.md) |
| Worker-side Go | `RemoteCodex/native/worker-agent-go/internal/<x>/` or `pkg/<x>/` | `pkg/` = public cross-module API surface |
| Shared types / proto | `shared/<module>/` | Across both DataServer + worker-agent-go |
| Scripts | `scripts/<ci\|cert\|operator>/` | Match purpose |
| Docs | `docs/<archive\|architecture\|api\|operations\|roadmap\|rw-prod\|100-percent-plan\|audit\|pr>/` | `archive/` is the overflow sink |
| Generated proto | `shared/controltransport/pb/<X>.pb.go` | Compiled by `scripts/gen-proto.sh`, committed for deterministic builds |

When in doubt: open `OWNERSHIP.md` first, ask the named owner, then
place the package. Naming conventions are spelled out per domain in
`docs/architecture/AGENT-CONTRACT.md`.

---

## 4. References

| Topic | Path |
| --- | --- |
| File-size policy & methodology (canonical) | [`docs/metrics/loc-baseline.md`](docs/metrics/loc-baseline.md) |
| CI gate implementation | [`scripts/ci/check-loc-thresholds.sh`](scripts/ci/check-loc-thresholds.sh) |
| Lint config (`funlen` enabled at lines=600) | [`.golangci.yml`](.golangci.yml) |
| Architecture target | [`docs/architecture/CURRENT-TO-TARGET-ARCHITECTURE.md`](docs/architecture/CURRENT-TO-TARGET-ARCHITECTURE.md) |
| Agent contract (roles, contracts) | [`docs/architecture/AGENT-CONTRACT.md`](docs/architecture/AGENT-CONTRACT.md) |
| Ownership (package-level single-owner map) | [`docs/architecture/OWNERSHIP.md`](docs/architecture/OWNERSHIP.md) |
| Repo top-level intro | [`README.md`](README.md) |
| CI workflow | [`.github/workflows/ci.yml`](.github/workflows/ci.yml) |
