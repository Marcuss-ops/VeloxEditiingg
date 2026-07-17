## v1.2.21 (2026-07-11)

### Behavior changes

- DataServer fallback SPA: long-dead default "frontend_standalone/web/dist" path replaced by "VeloxFrontend/web/dist" (submodule). Falls back to live handler when VELOX_SPA_DIR is unset AND submodule dist/ exists. Operators using VELOX_SPA_DIR are unaffected.

## [Unreleased] - 2026-07-11

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


