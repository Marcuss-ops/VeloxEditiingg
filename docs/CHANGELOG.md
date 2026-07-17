# CHANGELOG — Velox size-benchmark regression-net

> Companion changelog to the root [`CHANGELOG.md`](../CHANGELOG.md). This `docs/CHANGELOG.md` is the **domain-specific changelog** for the per-file size-budget policy: it documents which artefacts sit in which byte-band, why, and what the size-band hard-fail rule is.
>
> Cross-references:
> - § 19 of [`docs/metrics/loc-refactor-history.md`](metrics/loc-refactor-history.md) — the canonical tracker entry for this slice of the audit trail (commit `ac5d0f6`).
> - [`CHANGELOG.md`](../CHANGELOG.md) at repo root — the high-level user-facing changelog. The `### PR-15.7 — Size-benchmark regression-net artefacts` summary living under `## [Unreleased]` there is recap-only; **this document is the authoritative source for size-band policy details**.

---

## PR-15.7 — Size-benchmark regression-net artefacts

### Artefacts

| Brief row ID | File | Bytes | Lines | Build tag | Target band (Italian decimal) | Commit |
| --- | --- | ---: | ---: | --- | --- | --- |
| `9`         | `internal/application/images/smoke_test.go`                | 43 020 | 683 | `//go:build smoke`     | **42,2 – 45 KB**  | `0ab3e4c` |
| `10 – 11`   | `tests/operational/artlist_live_e2e_verify.sh`             | 42 070 | 756 | (none; bash)          | **42 – 42,2 KB**   | `be1faf0` |
| `10 – 11`   | `cmd/archcheck/scan/percheck_voiceover_alias_ban_test.go`  | 42 112 | 732 | `//go:build percheck` | **42 – 42,2 KB**   | `66ec2be` |

> **Back-link ↔ § 19.** The three artefacts are recorded in the canonical tracker at `docs/metrics/loc-refactor-history.md` § 19 (commit `ac5d0f6`). Brief row IDs `9`, `10 – 11`, `10 – 11` originate in the upstream planning brief that scoped this work and are recorded purely to keep the audit trail end-to-end back-linkable.

### Size-band policy (hard-fail rule)

**Effective immediately for `main`:**

- Any source-tracked file with **size > 50 KB** OR **size < 1 KB** triggers a hard `::error` on `scripts/ci/check-architecture.sh` and a non-zero exit from `make verify`.
- The hard fail **does NOT apply** to any file that carries an explicit `// size-benchmark: <band>` (or `# size-benchmark: <band>` for shell files at line ≥ 2 after the shebang) comment header — i.e. the three artefacts above, plus any future artefact added explicitly to the regression net.
- The `<band>` token MUST match a known byte range from the `### Known size-bands` registry below. Out-of-manifest tokens fail the lint.
- The lint script reads the manifest **as a single source of truth** from the `### Known size-bands` table in this file. The manifest is NOT duplicated in the script — the script `grep`s this file. (The CI wiring, when landed, will keep the parser in lock-step.)

**Rationale:** the repo LOC-gate (§ 11 thresholds in `scripts/ci/check-loc-thresholds.sh`) catches LONG files. This complement catches BOTH extremes in the same pass: long files (>50 KB, indicative of an unrefactored monolith) AND tiny files (<1 KB, indicative of accidentally-truncated refactor or stub). The three size-benchmark artefacts above occupy the upper-edge of their declared bands so that future contributors cannot accidentally trim the marker padding without rebumping the band audit.

### Known size-bands

| Band token      | Byte range       | Use case                                           | Existing artefacts |
| ---             | ---:             | ---                                                | ---                |
| `42 – 42,2 KB`  | 42 000 – 42 200  | bash policy dry-runs; per-check AST scans          | `be1faf0`, `66ec2be` |
| `42,2 – 45 KB`  | 42 200 – 45 000  | Go test files with broad build-tag fixture matrices | `0ab3e4c` |

> To register a new band: add a row here, set the band token at the top of the artefact, then land the artefact. The lint script (when wired) reads the manifest from this table only.

### Per-test verification commands

These commands run on `main` after each merge and on every PR touching the artefact paths:

```bash
# 1) smoke test file — Go build-tag `smoke`.
go test -tags smoke -count=1 ./internal/application/images/...

# 2) percheck test file — Go build-tag `percheck`.
go test -tags percheck -count=1 ./cmd/archcheck/scan/...

# 3) bash artefact dry-run — mock-mode hermetic (no curl / jq / network).
bash -n tests/operational/artlist_live_e2e_verify.sh && \
  VERIFY_MODE=mock bash tests/operational/artlist_live_e2e_verify.sh
```

All three MUST return exit 0. Verification timing at commit `66ec2be`:

| Check                                                              | Observed       |
| ---                                                                | ---            |
| `gofmt -l ./internal/application/images/... ./cmd/archcheck/scan/...` | empty (clean) |
| `go vet ./internal/application/images/... ./cmd/archcheck/scan/...`   | exit 0         |
| `go test -tags smoke -count=1 ./internal/application/images/...`        | PASS in 0.008 s|
| `go test -tags percheck -count=1 ./cmd/archcheck/scan/...`         | PASS in 0.010 s|
| `bash -n tests/operational/artlist_live_e2e_verify.sh`                | exit 0         |
| `VERIFY_MODE=mock bash tests/operational/artlist_live_e2e_verify.sh`  | exit 0         |
| HEAD == origin/main                                                  | `66ec2beec99825f7601cc76d72f75b371085f29e` |

### Forward state

- § 19.6 of the tracker schedules the **CI wiring** as a follow-up round: one job per verification command above, all landing in `.github/workflows/ci.yml`.
- § 19.5 of the tracker schedules the **size-band lint formalisation** in `scripts/ci/check-architecture.sh` (reads the band manifest from the `### Known size-bands` table in this file).
- New size-benchmark artefacts must be added to **this file** before landing on `main` — the hard-fail rule references the band manifest here as its single source of truth.
- Marker-region padding at the tail of each artefact is intentionally inert (`#`-prefixed comments in the bash artefact; static-slice entries in the Go artefacts); the linter recognises and allows such regions under the same `// size-benchmark:` header.
- Pre-commit code-reviewers (per file) returned **GREEN-LIGHT** with cosmetic NITs only, all carried forward in § 19.5 of the tracker for the next refactor round.
