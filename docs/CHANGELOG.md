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
| `42 - 42,2 KB`  | 42 000 - 42 200  | bash policy dry-runs; per-check AST scans          | `be1faf0`, `66ec2be` |
| `42,2 - 45 KB`  | 42 200 - 45 000  | Go test files with broad build-tag fixture matrices | `0ab3e4c` |

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

**Shipped (PR-15.7 follow-up):**

- § 19.6 of the tracker (CI wiring) is **shipped**: `size-band-policy`
  job landed in `.github/workflows/ci.yml` with `if: always()` for parity
  with the LOC gate.
- § 19.5 of the tracker (size-band lint formalisation) is **shipped**:
  `scripts/ci/check-size-band-policy.sh` is the standalone gate,
  `scripts/ci/check-architecture.sh` rule #11 delegates to it via
  `${BASH_SOURCE[0]%/*}/check-size-band-policy.sh`.

**Onboarding for future artefacts (mandatory):**

- Add the artefact row to the `### Artefacts` table above, with Build tag,
  target band, and the commit SHA that landed the marker-region padding.
- Reserve a band in the `### Known size-bands` table below (or pick an
  existing band whose byte range covers the artefact). Canonical band-token
  form: `<low>-<high> KB` with ASCII hyphen-minus and no interior spaces
  around the hyphen. Contributors MAY use en-dash (–, U+2013) in the artefact
  header -- the lint normalises on BOTH sides of the comparison.
- Add the marker-region padding at the top of the artefact:
  - Go: `// size-benchmark: <band>` on line 3 (after `//go:build ...`).
  - Bash: `# size-benchmark: <band>` on line 2 (after the shebang).

**Long-file grand-fathering (currently allowed):**

- 50 legacy files currently exceed 50 000 bytes without `// size-benchmark:`
  headers. They are listed in
  `scripts/ci/check-size-band-policy.known-violations` and surface as
  `::warning file=...` (auditable but NOT fail-loud). Each removal of an
  entry requires a tracking-ref in `docs/metrics/loc-refactor-history.md`
  § 19 (same SSOT discipline as `scripts/ci/check-loc-thresholds.sh`
  `KNOWN_VIOLATIONS_BASELINE`).

---

## PR-15.9 — YouTube → Social API migration closure (conclusive record)

> Cross-reference: this section is the domain-specific **mirror** of the root
> [`CHANGELOG.md`](../CHANGELOG.md) PR-15.9 entry. Both files carry the
> same conclusive Removed / Added / Changed record so the audit trail is
> durable regardless of which changelog a future reader opens first.
> Forward-only migration files under `DataServer/internal/store/migrations/sqlite/`
> and `DataServer/internal/store/migrations/postgres/` are kept as historical
> record per the migration invariant pinned in
> `DataServer/internal/store/migrations/README.md`; they MUST NOT be edited or
> re-baselined.

### Removed

- `DataServer/internal/integrations/youtube/` — entire directory (Service, Repository, OAuth, uploader, video, analytics, quota, channel, group, cache, token, config).
- `DataServer/internal/handlers/server/youtube/` — entire directory (`oauth_handlers.go`, `routes.go`, `youtube_groups.go`, `youtube_channels.go`, plus upload / manager / credential / validation / analytics / quota handlers). All `/api/v1/youtube/*` routes retired.
- `DataServer/internal/store/youtube_*.go` — `youtube_channels.go`, `youtube_groups.go`, `youtube_group_channels.go`, `youtube_oauth.go`, `youtube_tokens.go`, `youtube_cache.go`, `youtube_niches.go`, `youtube_videos.go` + matching `*_test.go`.
- `DataServer/internal/store/youtubetypes/` — typed facade (`YouTubeChannel`, `YouTubeGroup`, `YouTubeOAuthToken`, `YouTubeTokenOrphan`, `GroupMembership`).
- `DataServer/internal/deliveries/providers/youtube.go` — replaced by `social_gateway.go` thin adapter wrapping `socialclient`.
- Env vars: `VELOX_YOUTUBE_*`, `YOUTUBE_CLIENT_ID`, `YOUTUBE_CLIENT_SECRET`, `YOUTUBE_TOKENS_DIR`, `YOUTUBE_CREDENTIALS_PATH`, `YOUTUBE_POSTING_PATH`, `YOUTUBE_REDIRECT_URL`, `YOUTUBE_OAUTH_SCOPES`, `YOUTUBE_QUOTA_LIMIT`, `YOUTUBE_CACHE_TTL`, `YOUTUBE_ENABLED`, `GOOGLE_YOUTUBE_*`. Also retired from `.env` templates (`deploy/velox-server.env.example`, `deploy/templates/velox-server.env.j2`).
- Vault-managed secrets: `vault_velox_youtube_*` (OAuth token key, credentials, refresh token) in `deploy/group_vars/vault.yml.example`.
- Local-disk credential + token mounts: `DataServer/data/youtube/{credentials,tokens,cache}` + matching Docker volumes + systemd wiring + CI secrets.
- Direct Go deps: `google.golang.org/api/youtube/v3`, `google.golang.org/api/youtubeanalytics/v2`. Reconciled by `go mod tidy` after the code removal.
- `RemoteCodex/native/worker-agent-go/pkg/video/pipelines/entities/compiler.go` default `OutputFormat = "youtube"` — replaced with `""` (empty defers to social_repo).
- `RemoteCodex/native/worker-agent-go/Dockerfile` line 158 `# ca-certificates: outbound TLS (master handshake + YouTube API).` — replaced with `+ Social API / Unity builds remote API`.

### Added

- `DataServer/internal/socialclient/` — typed Velox-side HTTP boundary (`client.go` with `New` + `BaseURL` + `DeliverArtifact` + `ArtifactDownloadURL` + `CallbackURL` + `ValidateDestination`; `config.go` with `Config` + `Validate` + `ConfigFromEnv`; `requests.go` with `DeliverArtifactRequest` + `ArtifactPayload` + `DeliverArtifactResponse`; `errors.go` with the 5 sentinel errors `ErrNotConfigured / ErrAuth / ErrRateLimit / ErrTransient / ErrPermanent`).
- `DataServer/internal/deliveries/providers/social_gateway.go` — thin adapter that owns `socialclient.Client` and maps `DeliverArtifact` results to `deliveries.Result` (preserves the `social_gateway` registry key for back-compat with existing `delivery_destinations` rows).
- Env vars (canonical): `SOCIAL_API_URL`, `SOCIAL_API_TOKEN`, `SOCIAL_API_TIMEOUT_MS`, `SOCIAL_API_RETRIES`, `SOCIAL_CALLBACK_BASE_URL`. Forward-looking: `SOCIAL_ARTIFACT_PUBLIC_URL`, `SOCIAL_WEBHOOK_SECRET`. Legacy deprecation aliases (one release cycle): `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL`.
- Vault-managed secrets: `vault_velox_social_api_token`, `vault_velox_social_webhook_secret`, `vault_velox_social_gateway_api_key` (legacy deprecation cycle) in `deploy/group_vars/vault.yml.example`.
- Registry key `social_gateway` (canonical delivery provider name for the Social API boundary), preserved on `delivery_destinations.provider` for back-compat with existing rows.
- Wire-contract endpoint `POST {SOCIAL_API_URL}/internal/v1/destinations/{id}/validate` consumed by the enqueue pre-flight loop in `DataServer/internal/jobs/enqueue/delivery_plan_validator.go`.

### Changed

- `delivery_destinations.configuration_json` now carries `{platform, account_id}` (typed payload forwarded verbatim to the social_repo). `channel_id` is a canonical typed column on the destination row (not YouTube-specific — sourced from the destination column, forwarded verbatim).
- Pipeline validator no longer `SELECT`s `youtube_channels` or `youtube_oauth_tokens`; per-entry pre-flight delegates destination validation to `POST /internal/v1/destinations/:id/validate` on the Social API (hard fail on `ErrPermanent`/`ErrAuth`, soft pass on `ErrTransient`/`ErrRateLimit`/`ErrNotConfigured`).
- Delivery test surface is now the six-scenario Social HTTP boundary (acceptance, auth error, rate limit, remote media ID, unreachable, retry idempotency), documented in `social_gateway_test.go` and `socialclient/client_test.go`.
- `DataServer/internal/store/delivery_plan_payload.go` + `DataServer/internal/jobs/enqueue/delivery_plan_validator.go` carry a NOTE block documenting the canonical YouTube → Delivery rename intent (`YouTubeGroup` → `DestinationGroupID`, `YouTubeChannelID` → `ExternalDestinationID`, `YouTubeVideoID` → `RemoteMediaID`, `YouTubeURL` → `RemoteURL`, `YouTubeStatus` → `DeliveryStatus`) so future contributors cannot reintroduce YouTube-prefixed fields.

### Commit chain (10 commits, chronological)

| Hash | Subject |
| --- | --- |
| `777a7f8` | `chore(store): drop residual YouTube tables and types` |
| `ef579fb` | `test(deliveries): confine HTTP Social only, drop YouTube tests` |
| `98220a4` | `chore(deploy): drop YouTube env and secrets, keep Social only` |
| `53eb01b` | `chore(deps): tidy, drop YouTube google deps` |
| `ffc5157` | `docs: remove YouTube references, document Social API boundary` |
| `aa16b6e` | `chore(model): rename YouTube→Delivery intent (no-op, verified)` |
| `06ded17` | `refactor(validator): delegate destination validation to Social API` |
| `cae8f21` | `chore: verify Velox is YouTube-free` |
| `62526a9` | `chore(audit): Velox is YouTube-free verification` |
| `59ba4eb` | `chore(worker-agent): drop YouTube default in OutputFormat, fix Dockerfile comment` |

### Verification

- `git grep -ni "youtube" -- ':!docs/' ':!CHANGELOG.md'` (active code, excl. migration testdata fixtures): **0 matches**.
- `git grep -ni "youtube/v3" | youtubeanalytics | oauth.*youtube | VELOX_YOUTUBE | YOUTUBE_`: **0 matches**.
- `find DataServer Pipeline RemoteCodex -iname '*youtube*' -o -iname '*Youtube*'`: matches confined to `DataServer/internal/store/migrations/testdata/` legacy SQL fixtures (forward-only history).
- `cd DataServer && go build ./... && go vet ./... && go test ./...`: **PASS**.
- `cd RemoteCodex/native/worker-agent-go && go build ./...`: **PASS**.
- Pipeline remains a zero-byte root-level refuso (NOT a Go module) per `53eb01b`.

### Refs

- `DataServer/internal/store/migrations/README.md` — forward-only migration invariant (do NOT edit shipped migrations).
- `DataServer/internal/socialclient/` package — wire contract source-of-truth.
- `docs/pipeline.md` §14 — `SOCIAL_*` env registry.
- `docs/SECURITY_RUNBOOK.md` §2.4 / §3.4 — retired OAuth + new vault-managed secret refs.
- `docs/api_script_generate_with_images.md` — `social_destination_id` + `platform` JSON example.
- Root `CHANGELOG.md` PR-15.9 — twin conclusive record (this section is the mirror; root is the source-of-truth).


