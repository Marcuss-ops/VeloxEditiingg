# Velox Changelog Archive — 2026-07-27 and Earlier

> Historical release notes moved from the root [`CHANGELOG.md`](../../CHANGELOG.md).
> Entries are preserved verbatim and remain in their original chronological order.
> Use the compatibility anchors in the root changelog when an older link still
> targets `CHANGELOG.md#...`.

<a id="unreleased-2026-07-27"></a>
## [Unreleased] - 2026-07-27

<a id="validator-extensibility-data-driven-per-route-invariants"></a>
### Validator extensibility — data-driven per-route invariants

`scripts/api/validate_openapi.py` is no longer a brittle strict-equality
script. `ROUTE_INVARIANTS` is the single source of truth for what the
spec must contain: each entry declares
`{path, method, operationId, parameters:[...], responses:{code: $ref}}`
and the validator:

- emits `FAIL` if any required route is missing;
- silently tolerates EXTRA routes (the v3 fragility that broke every
  time a new endpoint landed is closed);
- emits `FAIL` on `operationId` drift, on a dropped `$ref` for any
  declared parameter, on a wrong/changed `$ref` for any declared
  response code.

The new endpoint group — `POST /api/v1/jobs`, operationId `submitJob`,
schemas `SubmitJobRequest` / `SubmitScene` / `SubmitDeliveryPlanEntry` /
`SubmitJobAcceptedResponse`, response codes `202` (→`SubmitJobAcceptedResponse`)
and `400/401/422/500` (→`ErrorEnvelope`) — is now fully covered by the
validator alongside the existing creator-push and creator-assets
invariants.

Round-2 cleanups layered on the rewrite:

- Dead code removed: `X_FLAT_TO_DTO_GO_FILE` (was defined but unused),
  and `ACCEPTED_RESPONSE_SCHEMA_REF` (the 202 $ref is now inline per
  route invariant).
- `_missing_required` annotation corrected to `Any` (it was annotated
  `expected: str` but called with lists, ints, and floats).
- Per-route `AuthorizationHeader` `$ref` is now enforced for the two
  authenticated POSTs (was previously only `XRequestIDHeader`-checked).

**Verified on `main`** after the rewrite (round 1) and the round-2
cleanup (this commit on top):

- `python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml`:
  `--- TOTAL PASS: 1 openapi file(s) meet all invariants ---` (exit 0).
- `python3 -m py_compile scripts/api/validate_openapi.py`: PASS.
- `python3 -c "import ast; ast.parse(open('scripts/api/validate_openapi.py').read())"`: PASS.

**Refs**: `scripts/api/validate_openapi.py`.

<a id="payload-hash-idempotency-409-on-idempotencykeyreused"></a>
### Payload-hash idempotency: 409 on `idempotency_key_reused`

`POST /api/v1/jobs` now returns **HTTP 409 `idempotency_key_reused`** when
the same `idempotency_key` is replayed with a **different payload**.
Replays with the **same** payload continue to converge on the existing
job (202 `created:false`) — the contract change closes a silent-clobber
window that previously existed on the new endpoint.

The verification lives in `creatorflow.Resolver.checkIdempotencyFastPath`,
NOT only in the SubmitJob handler: it compares the existing
`creator_forwardings.payload_sha256` to the SHA of the freshly-rebuilt
(URL-rewritten) worker payload and returns the new sentinel
`creatorflow.ErrIdempotencyKeyReused` on mismatch. The SubmitJob handler
maps that sentinel to HTTP 409 with `details: [{path: idempotency_key,
issue: hash_mismatch}]`.

The same check applies to `POST /api/v1/creator/jobs` so a creator
machine that POSTs differently-built JSON for the same `remote_job_id`
is also caught (the resolver is the single writer for both intake
paths).

**Idempotency-key log privacy**: the `API_V1_JOBS_ACCEPTED` log line
no longer emits the raw `idempotency_key`. It now emits
`idempotency_hash=<12 hex chars of SHA-256(key)>`. The raw key can carry
emails / customer refs / accidental tokens / log-injection payloads;
the full hash is still persisted inside `creator_forwardings`,
correlatable via `pipeline_creator_intake_accepted_total{path="api_v1_jobs"}`.

**OpenAPI + validator**:

- `ErrorCode.enum` now lists `idempotency_key_reused`.
- `POST /api/v1/jobs` declares a new `409` response with an
  `ErrorEnvelope` example (`{ok:false, error:idempotency_key_reused,
  details:[{path:idempotency_key, issue:hash_mismatch}]}`).
- `scripts/api/validate_openapi.py` `EXPECTED_ERROR_CODES` and
  `ROUTE_INVARIANTS[/api/v1/jobs].responses["409"]` updated.

**Verified on `main`** (committed on top of commit `4fcb46b`):

- `cd DataServer && go vet ./internal/creatorflow/... ./internal/handlers/server/pipeline/...`: PASS.
- `cd DataServer && go build ./internal/creatorflow/... ./internal/handlers/server/pipeline/...`: PASS.
- `cd DataServer && go test ./internal/handlers/server/pipeline/... -count=1 -run 'TestSubmitJob|TestCreatorPush'`: PASS.
- `python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml`: TOTAL PASS (exit 0).

**Refs**: `DataServer/internal/creatorflow/{resolver.go,resolver_idempotency.go,resolver_types.go}`,
`DataServer/internal/handlers/server/pipeline/{job_submit.go,creator_push.go}`,
`DataServer/api/openapi.yaml`, `scripts/api/validate_openapi.py`.

<a id="v1221-2026-07-11"></a>
## v1.2.21 (2026-07-11)

<a id="behavior-changes"></a>
### Behavior changes

- DataServer fallback SPA: long-dead default "frontend_standalone/web/dist" path replaced by "VeloxFrontend/web/dist" (submodule). Falls back to live handler when VELOX_SPA_DIR is unset AND submodule dist/ exists. Operators using VELOX_SPA_DIR are unaffected.

<a id="unreleased-2026-07-17"></a>
## [Unreleased] - 2026-07-17

<a id="youtubesocial-cleanup-finale"></a>
### YouTube→Social: cleanup finale

Six residues closed on `main` between PR-15.9 + PR-15.10 + PR-15.11 + PR-15.12 + PR-15.13 + PR-15.14 + PR-15.16. This section is the conclusive capstone a future reader reaches FIRST when investigating the YouTube → Social closure. Per-residue detail follows in the individual PR entries below.

The six residues, in the order the closure landed:

1. **Migration drop** — `DataServer/internal/store/migrations/sqlite/090_drop_youtube_domain.sql` (sqlite) + `DataServer/internal/store/migrations/postgres/010_drop_youtube_domain.sql` (postgres) drop all 10 YouTube tables and the historical YouTube columns on `calendar_events`; the shipped sqlite migration also removes the historical `youtube_group` column from the legacy `dark_editor_folders` table when that table exists. The legacy editor tables themselves are removed separately, forward-only, by migration 128; neither is an active Velox runtime surface. Operator-facing audit script: `deploy/scripts/audit-no-youtube-residuals.sh` (PR-15.11) returns exit `0 / 1 / 2 / 3 / 4` per outcome (CLEAN / RESIDUAL_FOUND / DB_NOT_FOUND / NOT_VELOX_SCHEMA / ARGV_OR_TOOL).

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

<a id="submodule-relationship"></a>
### Submodule relationship
- `VeloxEditiingg/.gitmodules` pins `VeloxFrontend` to commit `a2113ae` (intentional, by user request).
- Standalone `VeloxFrontend` HEAD is at `2369671` (newer than the submodule pin).
- The pin in the parent is preserved as-is: anyone who clones `VeloxEditiingg` gets `VeloxFrontend` at `a2113ae`, NOT at its latest standalone HEAD.
- This is by design for the migration backup: the parent project snapshot reflects the state at the backup time, not a rolling HEAD.

<a id="pr-157-size-benchmark-regression-net-artefacts"></a>
### PR-15.7 — Size-benchmark regression-net artefacts

Three artefacts landed as regression-net for the per-file size-budget policy. Each sits at the upper edge of its declared Italian-decimal byte-band so that a future contributor cannot accidentally trim the marker padding without rebumping the band audit.

| Artefact | Bytes | Lines | Build tag | Commit |
| --- | ---: | ---: | --- | --- |
| `internal/application/images/smoke_test.go`                | 43 020 | 683 | `//go:build smoke`     | `0ab3e4c` |
| `tests/operational/artlist_live_e2e_verify.sh`             | 42 070 | 756 | (none; bash)          | `be1faf0` |
| `cmd/archcheck/scan/percheck_voiceover_alias_ban_test.go`  | 42 112 | 732 | `//go:build percheck` | `66ec2be` |

Tracker: § 19 of `docs/metrics/loc-refactor-history.md` (commit `ac5d0f6`, audit-trail back-link). Verification: `go test -tags smoke ./internal/application/images/...`, `go test -tags percheck ./cmd/archcheck/scan/...`, and `VERIFY_MODE=mock bash tests/operational/artlist_live_e2e_verify.sh` all PASS at HEAD == origin/main. The three artefacts are also the canary inputs for § 19.6's planned per-file byte-band policy lint.

<a id="pr-158-youtube-social-api-separation-final"></a>
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

<a id="pr-159-youtube-social-api-migration-closure-conclusive-record"></a>
### PR-15.9 — YouTube → Social API migration closure (conclusive record)

This section is the **conclusive Removed / Added / Changed record** of the YouTube → Social API separation. It supersedes PR-15.8 above by adding the cosmetic closures (worker-agent default + Dockerfile comment) and the audit-marker chain (`aa16b6e`, `06ded17`, `cae8f21`, `62526a9`, `59ba4eb`). Forward-only migration files under `DataServer/internal/store/migrations/sqlite/` and `DataServer/internal/store/migrations/postgres/` are kept as historical record per the migration invariant pinned in `DataServer/internal/store/migrations/README.md`; they MUST NOT be edited or re-baselined.

<a id="removed"></a>
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

<a id="added"></a>
#### Added

- `DataServer/internal/socialclient/` — typed Velox-side HTTP boundary (`client.go` with `New` + `BaseURL` + `DeliverArtifact` + `ArtifactDownloadURL` + `CallbackURL` + `ValidateDestination`; `config.go` with `Config` + `Validate` + `ConfigFromEnv`; `requests.go` with `DeliverArtifactRequest` + `ArtifactPayload` + `DeliverArtifactResponse`; `errors.go` with the 5 sentinel errors `ErrNotConfigured / ErrAuth / ErrRateLimit / ErrTransient / ErrPermanent`).
- `DataServer/internal/deliveries/providers/social_gateway.go` — thin adapter that owns `socialclient.Client` and maps `DeliverArtifact` results to `deliveries.Result` (preserves the `social_gateway` registry key for back-compat with existing `delivery_destinations` rows).
- Env vars (canonical): `SOCIAL_API_URL`, `SOCIAL_API_TOKEN`, `SOCIAL_API_TIMEOUT_MS`, `SOCIAL_API_RETRIES`, `SOCIAL_CALLBACK_BASE_URL`. Forward-looking: `SOCIAL_ARTIFACT_PUBLIC_URL`, `SOCIAL_WEBHOOK_SECRET`. Legacy deprecation aliases (one release cycle): `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL`.
- Vault-managed secrets: `vault_velox_social_api_token`, `vault_velox_social_webhook_secret`, `vault_velox_social_gateway_api_key` (legacy deprecation cycle) in `deploy/group_vars/vault.yml.example`.
- Registry key `social_gateway` (canonical delivery provider name for the Social API boundary), preserved on `delivery_destinations.provider` for back-compat with existing rows.
- Wire-contract endpoint `POST {SOCIAL_API_URL}/internal/v1/destinations/{id}/validate` consumed by the enqueue pre-flight loop in `DataServer/internal/jobs/enqueue/delivery_plan_validator.go`.

<a id="changed"></a>
#### Changed

- `delivery_destinations.configuration_json` now carries `{platform, account_id}` (typed payload forwarded verbatim to the social_repo). `channel_id` is a canonical typed column on the destination row (not YouTube-specific — sourced from the destination column, forwarded verbatim).
- Pipeline validator no longer `SELECT`s `youtube_channels` or `youtube_oauth_tokens`; per-entry pre-flight delegates destination validation to `POST /internal/v1/destinations/:id/validate` on the Social API (hard fail on `ErrPermanent`/`ErrAuth`, soft pass on `ErrTransient`/`ErrRateLimit`/`ErrNotConfigured`).
- Delivery test surface is now the six-scenario Social HTTP boundary (acceptance, auth error, rate limit, remote media ID, unreachable, retry idempotency), documented in `social_gateway_test.go` and `socialclient/client_test.go`.
- `DataServer/internal/store/delivery_plan_payload.go` + `DataServer/internal/jobs/enqueue/delivery_plan_validator.go` carry a NOTE block documenting the canonical YouTube → Delivery rename intent (`YouTubeGroup` → `DestinationGroupID`, `YouTubeChannelID` → `ExternalDestinationID`, `YouTubeVideoID` → `RemoteMediaID`, `YouTubeURL` → `RemoteURL`, `YouTubeStatus` → `DeliveryStatus`) so future contributors cannot reintroduce YouTube-prefixed fields.

<a id="commit-chain-10-commits-chronological"></a>
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

<a id="verification"></a>
#### Verification

- `git grep -ni "youtube" -- ':!docs/' ':!CHANGELOG.md'` (active code, excl. migration testdata fixtures): **0 matches**.
- `git grep -ni "youtube/v3" | youtubeanalytics | oauth.*youtube | VELOX_YOUTUBE | YOUTUBE_`: **0 matches**.
- `find DataServer Pipeline RemoteCodex -iname '*youtube*' -o -iname '*Youtube*'`: matches confined to `DataServer/internal/store/migrations/testdata/` legacy SQL fixtures (forward-only history).
- `cd DataServer && go build ./... && go vet ./... && go test ./...`: **PASS**.
- `cd RemoteCodex/native/worker-agent-go && go build ./...`: **PASS**.
- Pipeline remains a zero-byte root-level refuso (NOT a Go module) per `53eb01b`.

<a id="refs"></a>
#### Refs

- `DataServer/internal/store/migrations/README.md` — forward-only migration invariant (do NOT edit shipped migrations).
- `DataServer/internal/socialclient/` package — wire contract source-of-truth.
- `docs/pipeline.md` §14 — `SOCIAL_*` env registry.
- `docs/SECURITY_RUNBOOK.md` §2.4 / §3.4 — retired OAuth + new vault-managed secret refs.
- `docs/api_script_generate_with_images.md` — `social_destination_id` + `platform` JSON example.
- `docs/CHANGELOG.md` PR-15.9 — twin conclusive record (mirror of this section).

<a id="pr-1510-socialgateway-legacy-alias-honor-cycle-retired"></a>
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

- `DataServer/internal/socialclient/config_test.go::TestConfigFromEnv_DropsLegacySocialGatewayAliases` — NEGATIVE pinning test. After setting ONLY the legacy aliases (canonical left empty), `ConfigFromEnv()` must return `BaseURL=""`, `APIKey=""`, `CallbackBaseURL=""` with the canonical `Timeout=30s` default. This locks the deprecation boundary closed.
- `DataServer/internal/socialclient/config_test.go::TestConfigFromEnv_HonorsCanonicalSocialAPIEnvs` — POSITIVE companion. Sets canonical `SOCIAL_API_URL` / `SOCIAL_API_TOKEN` / `SOCIAL_CALLBACK_BASE_URL` / `SOCIAL_API_TIMEOUT_MS=7000` and asserts every field is reflected in `ConfigFromEnv()`. Retry budgeting remains owned by the delivery runner.

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

<a id="pr-1516-no-youtube-regression-ci-guard-workflow"></a>

---

Continued in [part 2](CHANGELOG-2026-07-27-and-earlier-part2.md) (PR-15.16 and
the residue-closure entries); the post-refactor state record lives in
[CHANGELOG-post-refactor-state.md](CHANGELOG-post-refactor-state.md).
