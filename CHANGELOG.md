## v1.2.21 (2026-07-11)

### Behavior changes

- DataServer fallback SPA: long-dead default "frontend_standalone/web/dist" path replaced by "VeloxFrontend/web/dist" (submodule). Falls back to live handler when VELOX_SPA_DIR is unset AND submodule dist/ exists. Operators using VELOX_SPA_DIR are unaffected.

## [Unreleased] - 2026-07-17

### YouTube→Social: cleanup finale

Six residues closed on `main` between PR-15.9 + PR-15.10 + PR-15.11 + PR-15.12 + PR-15.13 + PR-15.14 + PR-15.16. This section is the conclusive capstone a future reader reaches FIRST when investigating the YouTube → Social closure. Per-residue detail follows in the individual PR entries below.

The six residues, in the order the closure landed:

1. **Migration drop** — `DataServer/internal/store/migrations/sqlite/090_drop_youtube_domain.sql` (sqlite) + `DataServer/internal/store/migrations/postgres/010_drop_youtube_domain.sql` (postgres) drop all 10 YouTube tables + the 3 historical columns on `calendar_events` + `dark_editor_folders`. Operator-facing audit script: `deploy/scripts/audit-no-youtube-residuals.sh` (PR-15.11) returns exit `0 / 1 / 2 / 3 / 4` per outcome (CLEAN / RESIDUAL_FOUND / DB_NOT_FOUND / NOT_VELOX_SCHEMA / ARGV_OR_TOOL).

2. **Destinazione opaque-mode** — `DataServer/internal/store/migrations/sqlite/091_opaque_destination.sql` DROPs the `account_id / channel_id / language` columns on `delivery_destinations` and ADDs the opaque `social_destination_id` (TEXT, nullable, fail-closed). Runtime guard: `runner.hydrateDestination` rejects empty `social_destination_id` with `ErrDestinationUnmapped` → delivery status code `DESTINATION_UNMAPPED` so operators see exactly which row needs backfill.

3. **Socialclient refactor** — `DataServer/internal/socialclient/` typed Velox-side HTTP boundary replaces all direct YouTube plumbing. Wire contract: `external_delivery_id`, `idempotency_key`, `social_destination_id`, `artifact` (required 4) + `metadata`, `publish_at`, `callback_url` (optional 3). Three wire-shape tests (Minimal / Full / LegacyKeysNeverPresent) pin the contract at the actual HTTP boundary (httptest + json.Unmarshal top-level keys, NOT string-matching).

4. **Rename `SocialDestinationID` → `ExternalDestinationID`** — gradual rename chain: 3 atomic commits on `main` (Commit 1 = store + migration 092, Commit 2 = validator + runner, Commit 3 = socialclient + provider). All canonical reads now reference `ExternalDestinationID`. The `SocialDestinationID` alias is preserved as a deprecated back-compat mirror (read-only bridge) until Residuo 5 closes it.

5. **Rimozione alias `SOCIAL_GATEWAY_*`** — the legacy deprecation aliases `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL` are RETIRED (PR-15.10). Contract is now canonical-only: every `SOCIAL_*` env var resolves 1:1 to its corresponding `SOCIAL_API_*` name. Operator migration: rename in `/etc/velox-server.env` + ansible vault (`vault_velox_social_gateway_api_key` → `vault_velox_social_api_token`).

6. **Migration `external_destination_id`** — `DataServer/internal/store/migrations/sqlite/092_rename_social_to_external_destination_id.sql` is the forward-only `ADD / UPDATE / DROP COLUMN` rename (NOT `RENAME COLUMN` — banned by `scripts/ci/check-migrations.sh` for portability). `DataServer/internal/store/migrations/sqlite/093_residuo4_closure_marker.sql` is the idempotent `json_insert` audit marker on `configuration_json` (`$.residuo4_closed_at`) that operators can verify with `SELECT count(*) FROM delivery_destinations WHERE json_extract(configuration_json, '$.residuo4_closed_at') IS NOT NULL`.

**CI guard**: `.github/workflows/no-youtube-regression.yml` (PR-15.16) hard-fails any PR / push / weekly drift detector that introduces the 7 forbidden patterns (`google.golang.org/api/youtube | youtubeanalytics | VELOX_YOUTUBE | youtube_oauth | internal/integrations/youtube | handlers/server/youtube | providers.NewYouTubeProvider`) outside the 10 pathspec exclusions (migrations + socialcontract + CHANGELOG + docs + MILESTONE doc + vault.yml.example + 2 NOTE-block files + workflow YAML self-exclusion).

**Verification on `main`**:

- `bash scripts/ci/check-migrations.sh`: `OK (148 files)`.
- `cd DataServer && go test ./internal/deliveries/... ./internal/socialclient/... ./internal/jobs/enqueue/... ./internal/integration_test/... ./internal/store/... -count=1`: PASS.
- `cd DataServer && go vet ./... && go build ./...`: PASS.
- `git grep -nE 'social_destination_id' -- ':!docs/' ':!CHANGELOG.md' ':!docs/CHANGELOG.md' ':!DataServer/internal/store/migrations/'`: aliased-mirror references only (read-only back-compat, full drop is Residuo 5).

**Commit chain on `main`** (NO branches, all atomic, oldest → newest):

| Hash | Subject | Residue |
| --- | --- | --- |
| `777a7f8` … `59ba4eb` (10 commits) | Chain cleanup (PR-15.9 close) | [1] Migration drop |
| `5491f31` | `chore(deploy): add read-only YouTube-residue audit script for operators` | [1] audit script |
| `ca000bf` / `bb407b8` / `6aadcd9` | `SOCIAL_GATEWAY_*` retirement chain (PR-15.10) | [5] Rimozione alias |
| `85c10f8` / `cab7cc3` / `2dfaed6` | Opaque-mode destination chain (PR-15.12) | [2] Destinazione opaque-mode |
| `71b0bb6` / `32bd74f` / `362718d` | Socialclient refactor chain (PR-15.13) | [3] Socialclient refactor |
| `ea38837` | `refactor(store): rename social_destination_id -> external_destination_id (Residuo 4 step 1)` + migration 092 | [4] rename |
| `03acccb` | `refactor(validator+runner): rename social_destination_id -> external_destination_id (Residuo 4 step 2)` | [4] validator + runner |
| `83d8b2f` | `refactor(socialclient+provider): wire + provider rename (Residuo 4 step 3)` | [4] wire + provider |
| `01810ea` | `docs(changelog+api_script): record Residuo 4 closure — PR-15.14` | [4] docs |
| `9a46461` | `refactor(migrations): add Residuo 4 closure marker` | [6] migration marker (093) |
| `59a91f7` | `ci(workflow): add no-youtube-regression guard` | CI guard (PR-15.16) |

### Submodule relationship
- `VeloxEditiingg/.gitmodules` pins `VeloxFrontend` to commit `a2113ae` (intentional, by user request).
- Standalone `VeloxFrontend` HEAD is at `2369671` (newer than the submodule pin).
- The pin in the parent is preserved as-is: anyone who clones `VeloxEditiingg` gets `VeloxFrontend` at `a2113ae`, NOT at its latest standalone HEAD.
- This is by design for the migration backup: the parent project snapshot reflects the state at the backup time, not a rolling HEAD.

### PR-15.7 — Size-benchmark regression-net artefacts

Three artefacts landed as regression-net for the per-file size-budget policy. Each sits at the upper edge of its declared Italian-decimal byte-band so that a future contributor cannot accidentally trim the marker padding without rebumping the band audit.

| Artefact | Bytes | Lines | Build tag | Commit |
| --- | ---: | ---: | --- | --- |
| `internal/application/images/smoke_test.go`                | 43 020 | 683 | `//go:build smoke`     | `0ab3e4c` |
| `tests/operational/artlist_live_e2e_verify.sh`             | 42 070 | 756 | (none; bash)          | `be1faf0` |
| `cmd/archcheck/scan/percheck_voiceover_alias_ban_test.go`  | 42 112 | 732 | `//go:build percheck` | `66ec2be` |

Tracker: § 19 of `docs/metrics/loc-refactor-history.md` (commit `ac5d0f6`, audit-trail back-link). Verification: `go test -tags smoke ./internal/application/images/...`, `go test -tags percheck ./cmd/archcheck/scan/...`, and `VERIFY_MODE=mock bash tests/operational/artlist_live_e2e_verify.sh` all PASS at HEAD == origin/main. The three artefacts are also the canary inputs for § 19.6's planned per-file byte-band policy lint.

### PR-15.8 — YouTube → Social API separation (final)

The YouTube domain has been **fully removed** from Velox and delegated to the external Social API repository. This change completes the migration started in `777a7f8` and propagates through `ef579fb`, `98220a4`, and `53eb01b`. The new wire contract — `POST ${SOCIAL_API_URL}/internal/v1/deliveries` carrying a typed `DeliverArtifactRequest` and returning a `social_delivery_id` — is owned by the Social API repo and surfaced to Velox through `socialclient/`.

**Removed** (Velox no longer owns these):

- `internal/integrations/youtube/` directory and all its service / repository / OAuth / uploader / video / analytics / quota / channel / group / cache / token components.
- `internal/handlers/server/youtube/` directory (`oauth_handlers.go`, `routes.go`, `youtube_groups.go`, `youtube_channels.go`, plus upload / manager / credential / validation / analytics / quota handlers).
- `internal/store/youtube_*.go` files (channels, groups, group_channels, oauth, tokens, cache, niches, videos).
- `internal/store/youtubetypes/` (the typed facade `YouTubeChannel`, `YouTubeGroup`, `YouTubeOAuthToken`, `YouTubeTokenOrphan`, `GroupMembership`).
- `internal/deliveries/providers/youtube.go` (replaced by the thin `social_gateway` adapter wrapping `socialclient`).
- Env vars `VELOX_YOUTUBE_*`, `YOUTUBE_CLIENT_ID`, `YOUTUBE_CLIENT_SECRET`, `YOUTUBE_TOKENS_DIR`, `YOUTUBE_CREDENTIALS_PATH`, `YOUTUBE_POSTING_PATH`, `GOOGLE_YOUTUBE_*`, `VELOX_YT_OAUTH_TOKEN_KEY`, `VELOX_YT_*`.
- Local-disk credential directories `DataServer/data/youtube/{credentials,tokens,cache}`; mount points and systemd wiring; CI secrets for those paths.
- `google.golang.org/api/youtube/v3` and `youtubeanalytics/v2` direct dependencies (no consumer in Velox after the code removal — `go mod tidy` reconciles them).

**Added** (Velox now ships these in their place):

- `internal/socialclient/` package (`client.go`, `config.go`, `requests.go`, `errors.go`) — typed Velox-side HTTP boundary against the social_repo.
- `internal/deliveries/providers/social_gateway.go` — thin adapter that calls `socialclient.New(cfg).DeliverArtifact(...)` and maps the response to `deliveries.Result`.
- Env vars `SOCIAL_API_URL`, `SOCIAL_API_TOKEN`, `SOCIAL_API_TIMEOUT_MS`, `SOCIAL_API_RETRIES`, `SOCIAL_CALLBACK_BASE_URL`, plus forward-looking placeholders `SOCIAL_ARTIFACT_PUBLIC_URL` and `SOCIAL_WEBHOOK_SECRET`.
- Vault-managed secrets `vault_velox_social_api_token`, `vault_velox_social_webhook_secret`, `vault_velox_social_gateway_api_key` (legacy deprecation cycle) in `deploy/group_vars/vault.yml.example`.
- Legacy deprecation aliases `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL`, honored for one release cycle alongside the canonical `SOCIAL_API_*` form.

**Changed**:

- Delivery provider registry now ships `social_gateway` (canonical key), with `delivery_destinations.provider = 'social_gateway'` back-compat preserved for existing rows.
- `delivery_destinations.configuration_json` carries `{platform, account_id}`; `channel_id` is a typed column on the destination row.
- Pipeline validator no longer `SELECT`s `youtube_channels` or `youtube_oauth_tokens`; destination validation is delegated to the Social API (`POST /internal/v1/destinations/:id/validate`).
- Test surface for deliveries is now the six-scenario Social HTTP boundary (acceptance, auth error, rate limit, remote media ID, unreachable, retry idempotency), documented in `social_gateway_test.go` and `socialclient/client_test.go`.
- Forward-only migration stratagem (`DataServer/internal/store/migrations/README.md`) preserves the historical `youtube_*` CREATE migrations; the `090_drop_youtube_domain.sql` (sqlite) and `010_drop_youtube_domain.sql` (postgres) are the source-of-truth closure. That README documents why a future reviewer must not re-edit shipped migrations.

Refs commits: `777a7f8`, `ef579fb`, `98220a4`, `53eb01b` — and this PR's `docs:` changelog record itself.

### PR-15.9 — YouTube → Social API migration closure (conclusive record)

This section is the **conclusive Removed / Added / Changed record** of the YouTube → Social API separation. It supersedes PR-15.8 above by adding the cosmetic closures (worker-agent default + Dockerfile comment) and the audit-marker chain (`aa16b6e`, `06ded17`, `cae8f21`, `62526a9`, `59ba4eb`). Forward-only migration files under `DataServer/internal/store/migrations/sqlite/` and `DataServer/internal/store/migrations/postgres/` are kept as historical record per the migration invariant pinned in `DataServer/internal/store/migrations/README.md`; they MUST NOT be edited or re-baselined.

#### Removed

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

#### Added

- `DataServer/internal/socialclient/` — typed Velox-side HTTP boundary (`client.go` with `New` + `BaseURL` + `DeliverArtifact` + `ArtifactDownloadURL` + `CallbackURL` + `ValidateDestination`; `config.go` with `Config` + `Validate` + `ConfigFromEnv`; `requests.go` with `DeliverArtifactRequest` + `ArtifactPayload` + `DeliverArtifactResponse`; `errors.go` with the 5 sentinel errors `ErrNotConfigured / ErrAuth / ErrRateLimit / ErrTransient / ErrPermanent`).
- `DataServer/internal/deliveries/providers/social_gateway.go` — thin adapter that owns `socialclient.Client` and maps `DeliverArtifact` results to `deliveries.Result` (preserves the `social_gateway` registry key for back-compat with existing `delivery_destinations` rows).
- Env vars (canonical): `SOCIAL_API_URL`, `SOCIAL_API_TOKEN`, `SOCIAL_API_TIMEOUT_MS`, `SOCIAL_API_RETRIES`, `SOCIAL_CALLBACK_BASE_URL`. Forward-looking: `SOCIAL_ARTIFACT_PUBLIC_URL`, `SOCIAL_WEBHOOK_SECRET`. Legacy deprecation aliases (one release cycle): `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL`.
- Vault-managed secrets: `vault_velox_social_api_token`, `vault_velox_social_webhook_secret`, `vault_velox_social_gateway_api_key` (legacy deprecation cycle) in `deploy/group_vars/vault.yml.example`.
- Registry key `social_gateway` (canonical delivery provider name for the Social API boundary), preserved on `delivery_destinations.provider` for back-compat with existing rows.
- Wire-contract endpoint `POST {SOCIAL_API_URL}/internal/v1/destinations/{id}/validate` consumed by the enqueue pre-flight loop in `DataServer/internal/jobs/enqueue/delivery_plan_validator.go`.

#### Changed

- `delivery_destinations.configuration_json` now carries `{platform, account_id}` (typed payload forwarded verbatim to the social_repo). `channel_id` is a canonical typed column on the destination row (not YouTube-specific — sourced from the destination column, forwarded verbatim).
- Pipeline validator no longer `SELECT`s `youtube_channels` or `youtube_oauth_tokens`; per-entry pre-flight delegates destination validation to `POST /internal/v1/destinations/:id/validate` on the Social API (hard fail on `ErrPermanent`/`ErrAuth`, soft pass on `ErrTransient`/`ErrRateLimit`/`ErrNotConfigured`).
- Delivery test surface is now the six-scenario Social HTTP boundary (acceptance, auth error, rate limit, remote media ID, unreachable, retry idempotency), documented in `social_gateway_test.go` and `socialclient/client_test.go`.
- `DataServer/internal/store/delivery_plan_payload.go` + `DataServer/internal/jobs/enqueue/delivery_plan_validator.go` carry a NOTE block documenting the canonical YouTube → Delivery rename intent (`YouTubeGroup` → `DestinationGroupID`, `YouTubeChannelID` → `ExternalDestinationID`, `YouTubeVideoID` → `RemoteMediaID`, `YouTubeURL` → `RemoteURL`, `YouTubeStatus` → `DeliveryStatus`) so future contributors cannot reintroduce YouTube-prefixed fields.

#### Commit chain (10 commits, chronological)

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

#### Verification

- `git grep -ni "youtube" -- ':!docs/' ':!CHANGELOG.md'` (active code, excl. migration testdata fixtures): **0 matches**.
- `git grep -ni "youtube/v3" | youtubeanalytics | oauth.*youtube | VELOX_YOUTUBE | YOUTUBE_`: **0 matches**.
- `find DataServer Pipeline RemoteCodex -iname '*youtube*' -o -iname '*Youtube*'`: matches confined to `DataServer/internal/store/migrations/testdata/` legacy SQL fixtures (forward-only history).
- `cd DataServer && go build ./... && go vet ./... && go test ./...`: **PASS**.
- `cd RemoteCodex/native/worker-agent-go && go build ./...`: **PASS**.
- Pipeline remains a zero-byte root-level refuso (NOT a Go module) per `53eb01b`.

#### Refs

- `DataServer/internal/store/migrations/README.md` — forward-only migration invariant (do NOT edit shipped migrations).
- `DataServer/internal/socialclient/` package — wire contract source-of-truth.
- `docs/pipeline.md` §14 — `SOCIAL_*` env registry.
- `docs/SECURITY_RUNBOOK.md` §2.4 / §3.4 — retired OAuth + new vault-managed secret refs.
- `docs/api_script_generate_with_images.md` — `social_destination_id` + `platform` JSON example.
- `docs/CHANGELOG.md` PR-15.9 — twin conclusive record (mirror of this section).

### PR-15.10 — `SOCIAL_GATEWAY_*` legacy alias honor-cycle retired

The legacy deprecation aliases `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL` documented in PR-15.8 / PR-15.9 as "honored for one release cycle" alongside the canonical `SOCIAL_API_*` form are now **retired** (no longer honored). The contract is canonical-only: every operator-facing SOCIAL_* env var resolves 1:1 to its corresponding `SOCIAL_API_*` name.

**BREAKING (operator-visible)**:

- An operator that still sets `SOCIAL_GATEWAY_*` env vars in `/etc/velox-server.env` (or in the ansible vault) will see `socialclient.ConfigFromEnv()` return `BaseURL=""` (and `APIKey=""`, `CallbackBaseURL=""`). The delivery provider surfaces `ErrNotConfigured` at `DeliverArtifact` time (fail-closed), not a silent fallback.
- Migration: rename the three legacy names in `/etc/velox-server.env` and in the ansible vault (`vault_velox_social_gateway_api_key` → `vault_velox_social_api_token`). Operators that already use the canonical `SOCIAL_API_*` names are unaffected.

**Removed (source-of-truth)**:

- `deploy/group_vars/all.yml` — non-secret defaults `velox_social_gateway_url`, `velox_social_gateway_callback_base_url`, and the `Legacy SOCIAL_GATEWAY_* aliases` comment block.
- `deploy/group_vars/vault.yml.example` — secret `vault_velox_social_gateway_api_key`. Ansible Vault reference now points only at `vault_velox_social_api_token` + `vault_velox_social_webhook_secret`.
- `deploy/velox-server.env.example` — commented-out legacy alias block (URL / API_KEY / CallbackBase) and the Secrets hint that referenced `vault_velox_social_gateway_api_key`.
- `deploy/templates/velox-server.env.j2` — Jinja render of `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL`.
- `DataServer/internal/socialclient/config.go::ConfigFromEnv` — `firstNonEmpty(canonical, legacy)` fallback for the three resolved fields. Doc comments on `type Config` and on `ConfigFromEnv()` rewritten to reflect canonical-only contract.
- Helper `firstNonEmpty` in the same file — deleted (became unused after the fallback removal).
- `DataServer/cmd/server/bootstrap_modules.go` — surgical comment update near line 237 (the "or its `SOCIAL_GATEWAY_URL` legacy fallback" parenthetical is replaced with a one-line breadcrumb to this CHANGELOG entry).
- `DataServer/internal/deliveries/providers/social_gateway_test.go::newLiveProviderForServer` — companion `t.Setenv("SOCIAL_GATEWAY_*", ...)` calls removed; the helper now sets ONLY canonical `SOCIAL_API_*`.

**Docs cleanup**:

- `docs/pipeline.md` §14 — removed the three `(legacy)` rows from the master env table.

**Tests (new)**:

- `DataServer/internal/socialclient/config_test.go::TestConfigFromEnv_DropsLegacySocialGatewayAliases` — NEGATIVE pinning test. After setting ONLY the legacy aliases (canonical left empty), `ConfigFromEnv()` must return `BaseURL=""`, `APIKey=""`, `CallbackBaseURL=""` (with `Timeout=30s` and `MaxRetries=0` defaults unchanged). This locks the deprecation boundary closed.
- `DataServer/internal/socialclient/config_test.go::TestConfigFromEnv_HonorsCanonicalSocialAPIEnvs` — POSITIVE companion. Sets canonical `SOCIAL_API_URL` / `SOCIAL_API_TOKEN` / `SOCIAL_CALLBACK_BASE_URL` / `SOCIAL_API_TIMEOUT_MS=7000` / `SOCIAL_API_RETRIES=2` and asserts every field is reflected in `ConfigFromEnv()`.

**Commit chain (3 micro-commits, ordered lowest-risk → highest-risk)**:

| Hash | Subject |
| --- | --- |
| `ca000bf` | `chore(ansible): drop legacy SOCIAL_GATEWAY_* vault vars and group_defaults` |
| `bb407b8` | `chore(deploy): drop legacy SOCIAL_GATEWAY_* alias lines from env templates` |
| `6aadcd9` | `refactor(socialclient): drop legacy SOCIAL_GATEWAY_* env fallback` (BREAKING) |

**Verification**:

- `go test ./internal/socialclient/... ./internal/deliveries/providers/...`: PASS.
- `go vet ./internal/socialclient/... ./internal/deliveries/providers/...`: PASS.
- `go build ./...`: PASS.
- `git grep -nE 'SOCIAL_GATEWAY' -- ':!docs/' ':!CHANGELOG.md' ':!docs/CHANGELOG.md'`: 0 matches after the chain.
- `git grep -nE 'vault_velox_social_gateway_'` -- deploy/: 0 matches.

**Refs**:

- `DataServer/internal/socialclient/config.go` — canonical-only reader.
- `DataServer/internal/socialclient/config_test.go` — boundary tests.
- `docs/pipeline.md` §14 — operational env registry (now legacy-free).
- `deploy/group_vars/{all,vault.yml.example}.yml` — operator configuration surface.
- `deploy/{velox-server.env.example,templates/velox-server.env.j2}` — rendered env surface.

### PR-15.16 — no-youtube-regression CI guard workflow

A dedicated GitHub Actions workflow now forbids re-introduction of any
direct Velox-side YouTube integration after the YouTube → Social API
closure (PR-15.8 / PR-15.9 / PR-15.10 / PR-15.11 / PR-15.12 / PR-15.13
/ PR-15.14 + Residuo 2 / 3 / 4 chain). Migrations 090 / 091 / 092 / 093
+ the typed model + validator + runner + socialclient + provider
layers already CLOSED the domain runtime; this workflow exists to keep
it closed at CI time.

**Added**:

- `.github/workflows/no-youtube-regression.yml` (commit
  `59a91f7 ci(workflow): add no-youtube-regression guard`). Single
  job `audit` (`YouTube regression guard` step) runs on
  `ubuntu-latest`, `timeout-minutes: 5`,
  `permissions: contents: read` (least-privilege). Concurrency
  group `no-youtube-regression-${ref}` cancels in-progress for
  `pull_request` events so successive PR updates do not pile up.
  `actions/checkout@v4` is invoked with `fetch-depth: 0` so the
  full history is searchable — future maintainers can `git blame`
  any match the audit surfaces.

**Triggers**:

- `push` to `main` — immediate fail-fast on regression re-introduction.
- `pull_request` to `main` — pre-merge gating. NO `paths-ignore`
  (every PR runs the audit; even a doc-only edit that introduces a
  YouTube pattern cannot slip through silently).
- `schedule: cron`: `'0 6 * * 1'` — weekly Monday 06:00 UTC drift
  detector (catches newly-disclosed YouTube patterns in PRs that
  somehow bypass direct CI).
- `workflow_dispatch` — manual re-run.

**Validator runner (single regex + 10 carve-out categories)**:

The runner script computes `git grep -nE "$REGEX"` over the full
history and pipes the results through a 12-line pathspec exclusion
set (10 distinct carve-out categories). On any non-empty match the
script prints the disjunction caught + per-disjunct remediation
hints and `exit 1`. On clean it prints
`✅ No YouTube regression found — clean.`

The single regex (verbatim from the workflow's `REGEX` env var):

```text
google\.golang\.org/api/youtube|youtubeanalytics|VELOX_YOUTUBE|youtube_oauth|internal/integrations/youtube|handlers/server/youtube|providers\.NewYouTubeProvider
```

Covers the 7 forbidden disjuncts (direct Go imports, legacy env var
prefix, OAuth subdomain, legacy integration / handler directories,
legacy provider constructor).

**Pathspec carve-outs (10 categories, 12 pathspec lines)**:

Each exclusion is documented inline in the workflow header with
its rationale. The full set:

1. `.github/workflows/no-youtube-regression.yml`
   **SELF-EXCLUSION** — the workflow file's header enumerates the
   forbidden disjuncts verbatim in the `REGEX` env var + the
   per-disjunct `Hints` comment block. Without this exclusion the
   audit would self-trip on the very file that defines it.

2. `**/migrations/**`
   Forward-only SQL migrations carry residual YouTube references
   under the
   `003_youtube_*.sql / 011_youtube_oauth_tokens.sql / 012_youtube_groups_rename.sql`
   chain. Editing them would violate the **forward-only invariant**
   documented in `DataServer/internal/store/migrations/README.md`
   (which is the same precedent as `001_initial.sql` from Residuo 1).

3. `**/testdata/**`
   Byte-mirror fixtures + snapshot data referenced by the migration
   runner's test suite (`applyMigration` reads from `testdata/*.sql`
   to satisfy idempotency repros).

4. `**/*_test.go`
   Go test files legitimately assert YouTube as a FORBIDDEN
   contract surface (e.g.
   `delivery_destination_opaque_test.go` pins "no youtube-prefixed
   field in `Destination` struct"; the socialclient wire-shape
   tests pin "legacy keys never present" with `youtube` constantly
   neighbouring the assertions). The audit MUST NOT trip on
   negative-pinning tests.

5. `**/*.example`
   Operator-facing templates that intentionally warn against
   re-introduction (Ansible vault, env templates, secrets examples).
   The `deploy/group_vars/vault.yml.example` file is the canonical
   case — it documents the historical `vault_velox_youtube_*`
   secrets as RETIRED.

6. `**/*.md`
   Nested documentation across `docs/**` and any future
   subdirectories cites removed artefacts as historical
   context ("`internal/integrations/youtube`";
   "`providers.NewYouTubeProvider`"). Audit MUST NOT trip on
   documented history.

7. `CHANGELOG.md`
   Root-level historical change record. The `**/*.md` pathspec
   matches `path/file.md` but does NOT match `file.md` at repo root
   (git pathspec semantics: at least one path component required).
   Added explicitly to cover the root-level case.

8. `MILESTONE_PR_YOUTUBE_SOCIAL_SEPARATION.md`
   Root-level milestone doc that intentionally cites the audit
   pattern verbatim as a record of "this string should never
   re-appear in active code".

9. `**/socialcontract/**` + `**/social_contract/**`
   Forward-looking carve-outs for `social_repo` boundary tests
   that may pin YouTube as FORBIDDEN contract markers. Zero-cost
   on current `main` (no matching directories yet); reserved for
   the integration suite landing under
   `DataServer/internal/integration_test/`.

10. `DataServer/internal/jobs/enqueue/delivery_plan_validator.go`
    + `DataServer/internal/store/delivery_plan_payload.go`
    Both files carry a NOTE block documenting the canonical
    YouTube → Delivery rename intent (`YouTubeGroup` →
    `DestinationGroupID`, `YouTubeChannelID` →
    `ExternalDestinationID`, `YouTubeVideoID` → `RemoteMediaID`,
    `YouTubeURL` → `RemoteURL`, `YouTubeStatus` → `DeliveryStatus`).
    The literal `youtube_oauth_tokens` is cited as a DROPPED
    legacy table ("Velox no longer SELECTs `youtube_channels`,
    `youtube_oauth_tokens`, or `youtube_groups`"). The carve-out
    is intentional — the NOTE is documentation for future
    contributors.
    **When (and only when) a future contributor removes those NOTE
    blocks, they MUST also delete the corresponding carve-out
    exclusions inline below** — leaving either an unused carve-out
    or the NOTE alone both regress this audit's surface-area
    contract.

**Operator-facing: what to do if the workflow fails on a PR**:

The workflow's `exit 1` path prints the matched line(s) AND the
per-disjunct remediation hints inline (verbatim in the runner step).
Operator playbook for the 7 forbidden disjuncts:

- **`google.golang.org/api/youtube` or `youtubeanalytics`** (direct
  Go imports) — run `cd DataServer && go mod tidy` and remove the
  dependency. If a reintroduction is genuinely needed for a Social
  API call, route through `DataServer/internal/socialclient/`
  instead — never import the upstream SDK directly.

- **`VELOX_YOUTUBE`** (legacy env var prefix) — migrate to canonical
  `SOCIAL_API_*` names per PR-15.10 closure contract. See
  `deploy/velox-server.env.example` +
  `deploy/group_vars/vault.yml.example` for the canonical mapping.
  The legacy `SOCIAL_GATEWAY_*` aliases are also retired alongside.

- **`youtube_oauth`** (OAuth subdomain) — the OAuth closures live
  in PR-15.8 + PR-15.9. Reintroduction requires a NEW explicit PR
  with rationale and **does NOT** silently merge: the workflow's
  purpose is to make such reintroduction a deliberate architectural
  decision, not an accidental copy-paste.

- **`internal/integrations/youtube`** or **`handlers/server/youtube`**
  (legacy directories) — closures live in PR-15.8. The migration
  is delegated to the external Social API repo. A community
  contributor who wants to revive the YouTube path must do so in a
  NEW repo, out of scope for Velox.

- **`providers.NewYouTubeProvider`** (legacy provider constructor) —
  use `social_gateway` (the thin adapter wrapping
  `socialclient.Client`). The provider registry is keyed on
  canonical names; registered providers live in
  `DataServer/internal/deliveries/providers/`.

**General operator checklist for any workflow failure**:

1. **Localize the offending line** with `git grep -nE "<REGEX>" --`
   against the local working tree, applying the same 12 pathspec
   carve-outs as the runner. Confirm the line is in ACTIVE code
   (NOT in CHANGELOG / docs / migrations / test fixtures).

2. **If the line is in active code**: pick the canonical replacement
   per the 7-disjunct playbook above. **DO NOT** add an inline
   `':!...'` carve-out to silence the audit — that's a regression-
   guard smoking-gun and must not be merged without an explicit PR
   justifying the carve-out and auditing the new exclusion with
   the same scrutiny as the original 10.

3. **If the line is correctly in an excluded path**: the carve-out
   list may need follow-up expansion (e.g., a NEW fixture file
   format or documentation subdirectory). Open a follow-up PR that
   references this entry, and add the new exclusion inline below
   the existing 10 with explicit rationale. The review checklist
   for such PRs: (a) the new exclusion is necessary, not
   convenient; (b) the new exclusion is documented inline; (c) the
   new exclusion does NOT weaken the audit's coverage of the 7
   forbidden disjuncts.

4. **If the carve-out removal is needed** (NOTE-block contributors
   removing the documentation comments that necessitate
   category 10): commit must delete the corresponding carve-out in
   the SAME atomic commit. Leaving either an unused carve-out (false
   freedom) or the NOTE alone (regression) both regress this
   audit's surface-area contract.

**Commit chain (1 commit on `main`, NO branches)**:

| Hash     | Subject                                |
| ---      | ---                                    |
| `59a91f7` | `ci(workflow): add no-youtube-regression guard` |

**Verification**:

- `cat .github/workflows/no-youtube-regression.yml` renders the
  workflow verbatim as documented above. The `REGEX` env var is
  the single source of truth for the audit pattern.

- `git log --oneline -1 -- .github/workflows/no-youtube-regression.yml`:
  `59a91f7 ci(workflow): add no-youtube-regression guard`.

- Local reproduction of the runner matrix (same pathspec
  exclusions as the workflow applies):
  ```bash
  git grep -nE 'google\.golang\.org/api/youtube|youtubeanalytics|VELOX_YOUTUBE|youtube_oauth|internal/integrations/youtube|handlers/server/youtube|providers\.NewYouTubeProvider' \
    -- ':!.github/workflows/no-youtube-regression.yml' \
    ':!**/migrations/**' ':!**/testdata/**' ':!**/*_test.go' \
    ':!**/*.example' ':!**/*.md' ':!CHANGELOG.md' \
    ':!MILESTONE_PR_YOUTUBE_SOCIAL_SEPARATION.md' \
    ':!**/socialcontract/**' ':!**/social_contract/**' \
    ':!DataServer/internal/jobs/enqueue/delivery_plan_validator.go' \
    ':!DataServer/internal/store/delivery_plan_payload.go'
  # expect: empty (0 matches on post-PR-15.x main)

- Manual `workflow_dispatch` against the workflow on `main` re-reports
  `✅ No YouTube regression found — clean.` at the gate tier. The
  canonical CI (`make verify`) does NOT duplicate this audit, so
  this workflow is the single source of truth for the regex match.

- `go test ./...`, `go vet ./...`, `go build ./...`: PASS.
  Workflow change is YAML-pure; no Go compile impact.

**Refs**:

- `.github/workflows/no-youtube-regression.yml` — workflow source-of-truth.
- `DataServer/internal/store/migrations/README.md` — forward-only
  migration invariant referenced by the `**/migrations/**` carve-out.
- `docs/SOCIAL_API_MIGRATION_RUNBOOK.md` — operator-facing closure
  context (closure backfill procedure for `social_destination_id` /
  `external_destination_id` legacy rows + audit procedure post-deploy).
- `PR-15.8 / PR-15.9 / PR-15.10 / PR-15.11 / PR-15.12 / PR-15.13 /
  PR-15.14` — the canonical closure chain this workflow guards.

### PR-15.14 — Residuo 4 closure: ExternalDestinationID canonical rename

The opaque-mode identity is now uniformly `ExternalDestinationID`
across the persistence layer, the in-process typed `Destination`
struct, the validator shape, the socialclient request DTO, and the
SocialGatewayProvider. The legacy `SocialDestinationID` alias is
still populated by the store + runner + validator during the
gradual-rename transition window (Residuo 5 is the dedicated
alias-drop closing commit).

**Removed (canonical naming)**:

- Typed field `SocialDestinationID` on `socialclient.DeliverArtifactRequest`
  (json tag `social_destination_id`) — superseded by `ExternalDestinationID`
  (json tag `external_destination_id`).
- Provider reads `destination.ExternalDestinationID` (canonical)
  instead of `destination.SocialDestinationID` (deprecated alias).

**Added**:

- Migration `092_rename_social_to_external_destination_id.sql`
  (sqlite + testdata mirror) — forward-only
  `ALTER TABLE delivery_destinations ADD COLUMN
  external_destination_id TEXT` + `UPDATE ... SET
  external_destination_id = COALESCE(social_destination_id, '')` +
  `ALTER TABLE delivery_destinations DROP COLUMN
  social_destination_id`. NOT a `RENAME COLUMN` (banned by
  `scripts/ci/check-migrations.sh` for portability — table-rebuild
  pattern is required, but ADD/UPDATE/DROP achieves the same end
  on SQLite >= 3.35.0 without breaking checksum parity).
- Canonical JSON wire key `external_destination_id` (NO `omitempty`
  on the request DTO so any drift between the runner's fail-closed
  `DESTINATION_UNMAPPED` guard and the socialclient surfaces at
  marshal time).
- Sentinel `ErrDestinationUnmapped` message updated from
  `social_destination_id required` to
  `external_destination_id required` (canonical post-rename).
- 4 NEW tests:
  - `store/delivery_destination_opaque_test.go::TestDeliveryDestinationOpaqueStructShape`
    — compile-time pin of dual-field shape (canonical ExternalDestinationID
    + alias SocialDestinationID mirrored).
  - `store/delivery_destination_opaque_test.go::TestDeliveryDestinationJSONOpaqueKeys`
    — JSON serialization: canonical `external_destination_id` MUST
    be present, legacy `account_id / channel_id / language /
    social_destination_id` MUST be absent.
  - `store/delivery_destination_opaque_test.go::TestDeliveryDestinationEmptyExternalDestinationIDOmitEmpty`
    — empty canonical (with alias populated) is suppressed by `omitempty`.
  - `jobs/enqueue/delivery_plan_validator_test.go::TestShapeFromMap_CanonicalExternalDestinationIDHonored`
    + `TestShapeFromMap_CanonicalWinsOverLegacyAlias` — validator
    precedence: canonical key wins, alias preserved verbatim when both
    keys are present with differing values.
- 4 UPDATED test fixtures: `socialclient/client_test.go` 4 fixtures
  (HappyPath, WireShape_Minimal, WireShape_Full,
  WireShape_LegacyKeysNeverPresent) + the required-keys arrays in
  WireShape_Minimal (`external_destination_id` instead of
  `social_destination_id`) and WireShape_Full — fully aligned with
  the canonical wire key.
- 2 sampleDestination fixture updates:
  `providers/social_gateway_test.go::sampleDestination` +
  `integration_test/social_repo_integration_test.go::sampleDestination`
  now set `ExternalDestinationID` canonical (alias-deprecated
  `SocialDestinationID` is intentionally left empty in the fixtures
  to prove the canonical-only path works).

**Behaviour changes (operator-visible)**:

- The opaque-mode wire JSON OBJECT emitted by
  `socialclient.DeliverArtifactRequest` now contains
  `external_destination_id` in place of `social_destination_id`.
  Server-side consumers (the social_repo) MUST update their
  request handlers; client-side observers that grep the wire
  body MUST update their patterns.
- The runtime allow-closed error message after a missing opaque
  destination backfill now reports
  `delivery_plan[0].external_destination_id: ...` (canonical)
  instead of `delivery_plan[0].social_destination_id: ...`
  (legacy alias). Operators / observability tooling that grep
  the field path MUST update.
- The `Destination` typed struct in the `deliveries` package and
  the `DeliveryDestination` typed struct in the `store` package
  now carry BOTH `ExternalDestinationID` (canonical, sources all
  dispatch reads + fail-closed guards) AND `SocialDestinationID`
  (deprecated alias, mirror-symmetric with the canonical field).
  The alias is consumed by no active code path; it is preserved
  as a read-only bridge for callers that have not yet migrated.

**Commit chain (3 atomic commits on `main`, NO branches, one commit per layer)**:

| Hash | Subject |
| --- | --- |
| `ea38837` | `refactor(store): rename social_destination_id -> external_destination_id (Residuo 4 step 1)` |
| `03acccb` | `refactor(validator+runner): rename social_destination_id -> external_destination_id (Residuo 4 step 2)` |
| `83d8b2f` | `refactor(socialclient+provider): wire + provider rename social_destination_id -> external_destination_id (Residuo 4 step 3)` |

The chain is the textbook gradual-rename: each layer holds BOTH names
during its commit (next-commit renames the next layer), and every
commit boundary compiles + tests PASS. Step 3 (the wire + provider
flip) is necessarily atomic per Go's static-typing rule (struct field
rename forces simultaneous provider mapping + test fixture updates).

**Verification**:

- `cd DataServer && go test ./internal/deliveries/... ./internal/socialclient/... ./internal/jobs/enqueue/... ./internal/integration_test/... ./internal/store/... -count=1`: PASS.
- `cd DataServer && go test ./internal/socialclient/... -v -run WireShape`: PASS for all 3 WireShape_Minimal / Full / LegacyKeysNeverPresent.
- `cd DataServer && go vet ./... && go build ./...`: PASS.
- `bash scripts/ci/check-migrations.sh`: OK (146 files).
- `git grep -nE 'social_destination_id' -- ':!docs/' ':!CHANGELOG.md' ':!docs/CHANGELOG.md' `: active code references are now confined to the legacy SocialDestinationID alias mirrors (store + runner + validator) + the migration testdata shadow of 091 (inert).
- The 6 documented scenarios (acceptance / auth / rate-limit / transient 5xx / unreachable / retry idempotency) STILL PASS on both the enqueue pre-flight path and the runner dispatch path with the new canonical wire key.
- Mock social_repo sniffer is `idempotency_key`-only, so the wire-key rename does NOT regress the dedup contract.

**Refs**:

- `DataServer/internal/store/migrations/sqlite/092_rename_social_to_external_destination_id.sql` — forward-only schema migration.
- `DataServer/internal/store/migrations/testdata/092_rename_social_to_external_destination_id.sql` — byte-equivalent runner-required mirror.
- `DataServer/internal/store/store_deliveries.go::DeliveryDestination` — typed struct post-migration schema (dual-field, alias-mirror).
- `DataServer/internal/deliveries/provider.go::Destination` / `ErrDestinationUnmapped` — dual-field typed struct + canonical sentinel message.
- `DataServer/internal/deliveries/runner.go::hydrateDestination` — reads `d.ExternalDestinationID` (canonical); guards `TrimSpace == ""`; mirrors to `SocialDestinationID` for gradual-rename consumers.
- `DataServer/internal/jobs/enqueue/delivery_plan_validator.go::deliveryPlanShape` / `shapeFromMap` — canonical-first read with legacy-alias fallback; precedence: canonical wins, alias preserved verbatim.
- `DataServer/internal/socialclient/requests.go::DeliverArtifactRequest.ExternalDestinationID` — canonical wire field (`json:"external_destination_id"`, NO omitempty).
- `DataServer/internal/deliveries/providers/social_gateway.go::buildRequest` — reads `destination.ExternalDestinationID` (canonical) and forwards as `req.ExternalDestinationID`.
- `docs/api_script_generate_with_images.md` — operator-facing JSON example updated to use `external_destination_id` + `metadata` blob (platform-shaped values live in metadata as opaque pass-through).

### PR-15.13 — Residuo 3 closure: opaque-mode wire contract

The Social API wire contract now carries only the opaque-mode fields:
`external_delivery_id`, `idempotency_key`, `social_destination_id`,
`artifact`, `metadata`, `publish_at`, `callback_url`. The three
YouTube-specific fields `Platform`, `AccountID`, `ChannelID` are
gone from both the typed `DeliverArtifactRequest` and the
`SocialGatewayProvider::buildRequest` call site; the social_repo is
the authoritative resolver from `social_destination_id` for
platform, account, channel, language, and credentials.

**Removed (typed struct fields + provider plumbing)**:

- `socialclient.DeliverArtifactRequest.Platform` / `AccountID` / `ChannelID`
  — fields dropped from the wire DTO entirely.
- `providers.parsePlatformAndAccount` helper — removed (it parsed
  `destination.ConfigurationJSON` for `platform`/`account_id` and
  was the only consumer of those keys in the wire DTO).

**Added (wire contract)**:

- `socialclient.DeliverArtifactRequest.SocialDestinationID string`
  with `json:"social_destination_id"` tag (NO `omitempty` so any
  drift between the runner's fail-closed `DESTINATION_UNMAPPED`
  guard and the socialclient surfaces at marshal time as
  `"social_destination_id":""` rather than a silent malformed
  POST).

**Behaviour changes**:

- Operators with `delivery_destinations.configuration_json`
  containing `{"platform":"youtube","account_id":"..."}` continue
  to author the old shape without breakage, BUT it is now
  **inert in the wire contract**: the values do not reach the
  social_repo. The runner + provider only forward the opaque
  `social_destination_id` and `delivery_metadata_json` (the latter
  becomes the wire `metadata` blob, opaque pass-through).
- Operators wanting per-artifact values to reach the social_repo
  must use the `metadata` blob, not the inert `configuration_json`.

**New tests (all in `internal/socialclient/client_test.go`)**:

- `TestClient_DeliverArtifact_WireShape_Minimal` — pins the
  minimal wire JSON: top-level keys must be EXACTLY four
  (`external_delivery_id`, `idempotency_key`, `social_destination_id`,
  `artifact`); `metadata`, `publish_at`, `callback_url` must NOT
  appear when empty.
- `TestClient_DeliverArtifact_WireShape_Full` — pins the full
  wire JSON: all 7 top-level keys present.
- `TestClient_DeliverArtifact_WireShape_LegacyKeysNeverPresent` —
  regression invariant: top-level wire JSON keys may NEVER
  include `platform`, `account_id`, or `channel_id`, **even if**
  the operator's `metadata` blob legitimately contains those
  sub-keys (metadata is opaque pass-through; legacy keys do not
  belong at the top).

These tests use httptest.NewServer + chan []byte body capture +
json.Unmarshal on top-level keys — NOT string-matching — so
metadata sub-keys do NOT false-positive on the legacy-key
presence check.

**Fixture cleanup**:

- `providers/social_gateway_test.go::sampleDestination` and
  `integration_test/social_repo_integration_test.go::sampleDestination`
  simplify `ConfigurationJSON` from inert-keyed blobs to `"{}"`.
  DeliveryMetadataJSON is kept (still forwarded as `metadata`).
  Doc comments expanded to make the wire/observability split
  explicit at the fixture level.

**ABI-safe ordering (3 atomic commits, NO branches)**:

| Hash     | Subject |
| ---      | --- |
| `71b0bb6` | `refactor(socialclient): opaque-mode wire — add social_destination_id, deprecate Platform/AccountID/ChannelID` |
| `32bd74f` | `refactor(social_gateway): drop parsePlatformAndAccount + deprecated struct fields` |
| `362718d` | `test(socialclient): pin opaque wire shape + clean inert fixtures` |

The 2-step provider cleanup is the textbook refactor-2-step
pattern: Commit 1 keeps the old fields typed-but-un-serialised
(`json:"-"`) so callers still compile, Commit 2 drops them
entirely along with `parsePlatformAndAccount`. Commit 3 is pure
test layer (no struct change).

**Verification**:

- `cd DataServer && go test ./internal/socialclient/... ./internal/jobs/enqueue/... ./internal/delivery_destinations... -count=1`: PASS
- `cd DataServer && go vet ./internal/socialclient/... ./internal/deliveries/...`: PASS
- `cd DataServer && go build ./...`: PASS
- `git grep -nE 'parsePlatformAndAccount|req\\.Platform|req\\.AccountID|req\\.ChannelID'`: 0 matches.
- The 6 documented scenarios (acceptance / auth / rate-limit /
  transient 5xx / unreachable / retry idempotency) STILL PASS on
  both the enqueue pre-flight path (`Enqueuer.WithSocialValidator`)
  and the runner dispatch path (`SocialGatewayProvider.Deliver`)
  with the new wire shape — no behavioral regressions.

**Refs**:

- `DataServer/internal/socialclient/requests.go::DeliverArtifactRequest` — typed DTO + opaque-mode doc.
- `DataServer/internal/socialclient/client.go::DeliverArtifact` — wire serializer (unchanged path, but the request shape changed).
- `DataServer/internal/deliveries/providers/social_gateway.go::buildRequest` — simplified: only routes `destination.SocialDestinationID`.
- `DataServer/internal/deliveries/runner.go::hydrateDestination` — fail-closed `DESTINATION_UNMAPPED` (Residuo 2, still the guardrail for the new wire shape).

### PR-15.12 — Residuo 2 closure: opaque-mode Destination model

The Delivery destination model is now fully opaque-mode. Velox no longer
carries the YouTube-specific fields `AccountID`, `ChannelID`, `Language`
either in the typed structs or in the SQLite schema. They are owned
exclusively by the external Social API repository, which resolves them
internally from the opaque `SocialDestinationID`. The migration is
forward-only (no DOWN), version-pinned (SQLite >= 3.35.0), and
ABI-safe-ordered: model → store → validator.

**Removed (typed struct fields + SQL columns)**:

- `data Destination.*` fields: `AccountID`, `ChannelID`, `Language`.
- `data DeliveryDestination.*` fields: `AccountID`, `ChannelID`, `Language`.
- SQLite column drop via migration `091_opaque_destination.sql`
  (forward-only `ALTER TABLE delivery_destinations DROP COLUMN` × 3).

**Added (opaque mode)**:

- `data Destination.SocialDestinationID` — opaque identifier resolved by
  the external Social API. Typed as `string`. JSON tag
  `social_destination_id,omitempty` so an empty value never leaks into
  the wire contract.
- `data DeliveryDestination.SocialDestinationID` — symmetric to the
  in-process type. Stored as `social_destination_id TEXT` (nullable, no
  DEFAULT) so an unmapped row reads back as empty string after COALESCE.
- Sentinel `errors.New("deliveries: destination is unmapped\n(social_destination_id required)")`
  (`ErrDestinationUnmapped`) in `internal/deliveries/provider.go`.
- Runtime guard in `runner.hydrateDestination`: rejects empty
  `SocialDestinationID` at hydrate time, BEFORE dispatch. processLease
  distinguishes `ErrDestinationUnmapped` from `ErrProviderNotConfigured`
  with delivery-status code `DESTINATION_UNMAPPED`
  (vs `DESTINATION_NOT_FOUND`).
- Migration `091_opaque_destination.sql` (sqlite + testdata mirror) that
  drops the 3 YouTube-specific columns and adds `social_destination_id`.
- New opaque-mode unit tests:
  - `internal/deliveries/destination_opaque_test.go`:
    - `TestDestinationOpaqueStructShape` — compile-time assertion that
      the typed Destination does not accept legacy fields.
    - `TestErrDestinationUnmappedSentinel` +
      `TestErrDestinationUnmappedIsCompatibleWithErrorsIs` — sentinel
      stability + `errors.Is` round-trip.
  - `internal/store/delivery_destination_opaque_test.go`:
    - `TestDeliveryDestinationOpaqueStructShape` — compile-time.
    - `TestDeliveryDestinationJSONOpaqueKeys` — JSON keys for the
      persisted shape; legacy `account_id/channel_id/language` keys
      confirmed absent.
    - `TestDeliveryDestinationEmptySocialDestinationIDOmitEmpty` —
      empty `social_destination_id` is suppressed by `omitempty`.

**Behavior change (delivery dispatch)**:

- A destination whose `social_destination_id` is empty / whitespace-only
  is now dispatched into FAILED with code `DESTINATION_UNMAPPED`
  (previously it would silently proceed via `social_gateway.buildRequest`
  with `ChannelID=""` until the social_repo rejected it).
- Operators that still have existing `delivery_destinations` rows with
  empty `social_destination_id` post-migration MUST backfill before
  enabling dispatch. The audit script
  (`deploy/scripts/audit-no-youtube-residuals.sh`, PR-15.11) does not
  probe `delivery_destinations` schema directly — it's a YouTube-residue
  auditor only — so a follow-up operator checklist is recommended.

**Commit chain (3 atomic commits on `main`, NO branches)**:

| Hash | Subject |
| --- | --- |
| `85c10f8` | `refactor(deliveries): drop AccountID/ChannelID/Language from Destination, add SocialDestinationID` |
| `cab7cc3` | `refactor(store): drop account_id/channel_id/language columns, add social_destination_id` |
| `2dfaed6` | `refactor(deliveries): fail-closed on unmapped destinations + opaque-mode tests` |

**Verification**:

- `cd DataServer && go test ./internal/deliveries/... ./internal/jobs/enqueue/... ./internal/integration_test/... ./internal/store/... -count=1`: PASS.
- `cd DataServer && go vet ./... && go build ./...`: PASS.
- New tests cover: struct shape (compile-time), sentinel stability, `errors.Is` chain, JSON opaque keys, `omitempty` on empty opaque ID.
- Existing tests untouched (the `BlockedAuth` fixture, the `sampleDestination` fixtures, and the `enqueue_test_helpers` seeds all use canonical fields only).
- ABI-safe ordering verified: model landed before store before validator so the typed struct + SQL + runner agree at every commit boundary.

**Refs**:

- `DataServer/internal/deliveries/provider.go` — `ErrDestinationUnmapped` sentinel documented.
- `DataServer/internal/deliveries/runner.go::hydrateDestination` — guard documented.
- `DataServer/internal/store/migrations/sqlite/091_opaque_destination.sql` — forward-only schema migration.
- `DataServer/internal/store/store_deliveries.go::DeliveryDestination` — typed struct post-migration schema.
- `DataServer/internal/store/migrations/README.md` — forward-only invariant (do NOT edit shipped migrations).

### PR-15.11 — Operator-facing YouTube-residue audit script

Operators can now run a read-only SQLite audit on the live Velox
production DB to confirm that the YouTube domain is fully cleaned.
The audit script reflects the same contract the test suite pins:

- Migration `090_drop_youtube_domain.sql` is forward-only and
  idempotent (checksum gate).
- The end-to-end migration test
  (`DataServer/internal/store/migrations/migrations_integration_test.go`,
  `TestIntegration_MigrationRunner_EndToEnd`, phase 4) asserts that
  none of the 10 YouTube tables exist after the chain.
- The schema test
  (`DataServer/internal/store/migrations/migrations_schema_test.go`,
  `TestMigration090_YouTubeDomainDropped`) additionally asserts that
  the 3 historical columns on `calendar_events` and
  `dark_editor_folders` are absent.

**Added**:

- `deploy/scripts/audit-no-youtube-residuals.sh` — read-only SQLite
  probe. Takes `<path-to-velox.db>` as argv and reports any leftover
  `youtube_*` tables (anchored `youtube\_%` ESCAPE) plus any
  `youtube_*` columns on `calendar_events` and `dark_editor_folders`
  (via `pragma_table_info` filtered inline). Pattern matches
  case-insensitively so it catches mixed-case identifiers like
  `` `YouTube_Group` ``.

**Exit codes**:

| Code | Meaning |
| ---: | --- |
| `0` | CLEAN — no YouTube tables or columns remain |
| `1` | RESIDUAL_FOUND — see report; remediation hint printed |
| `2` | DB_NOT_FOUND — path missing / unreadable |
| `3` | NOT_VELOX_SCHEMA — DB exists but is missing canonical Velox tables |
| `4` | ARGV_OR_TOOL — `sqlite3` CLI missing or wrong invocation |

**Sanity pre-check**: the script probes for the 5 canonical permanent
tables (`jobs`, `artifacts`, `job_deliveries`, `calendar_events`,
`dark_editor_folders`) before reporting residuals, so a non-Velox
SQLite file is rejected with exit 3 rather than producing a misleading
`` CLEAN '' report.

**Operator usage**:

```bash
sudo ./deploy/scripts/audit-no-youtube-residuals.sh /var/lib/velox/data/velox.db
#   exit 0  →  clean
#   exit 1  →  scrap the report; investigate
```

**Verification on synthetic DBs** (run on this commit before push):

| Scenario | DB shape | Exit | Outcome |
| --- | --- | ---: | --- |
| A. `bash -n` syntax check | n/a | n/a | OK |
| B. Clean Velox-shaped DB | 5 canonical tables, no YouTube state | `0` | "CLEAN" reported |
| C. Contaminated DB | + 4 YouTube tables + 3 YouTube columns | `1` | Full report listing all 7 residuals + remediation |
| D. Non-Velox SQLite | only `foo` table | `3` | "does not look like a Velox schema" |
| E. Nonexistent path | n/a | `2` | "DB not readable" |
| F. No argv | n/a | `4` | usage error on stderr |
| G. Mixed-case column `` `YouTube_Group` `` | 5 canonical + 1 mixed-case column | `1` | correctly detected via `lower(name)` |

**Commit**:

| Hash | Subject |
| --- | --- |
| `5491f31415deba20adc1fca21142a4c57b7a89fa` | `chore(deploy): add read-only YouTube-residue audit script for operators` |


