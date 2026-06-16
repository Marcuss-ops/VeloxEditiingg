# YouTube SQLite-Only Migration Plan

Tracking the work to converge the YouTube integration from a triple
source-of-truth (`JSON secrets + RAM maps + SQLite`) onto a single
canonical SQLite model with encrypted OAuth secrets and **zero runtime
filesystem persistence for credentials**.

- **Source of truth**: the architectural verdict pasted in chat on
  16-jun-2026 (file-id `## Verdetto`).
- **Owner**: youtube module maintainers.
- **Status legend**: `DONE` · `IN PROGRESS` · `PARTIAL` · `TODO` · `BLOCKED`.
- **Priority legend**: `P0 Critical` · `P1 High` · `P2 Medium`.

> Read this file top-to-bottom on first review. The **dependency DAG**
> section dictates what order steps can land.

## Verdict → Step mapping

| Verdict claim ("logiche da taglierei") | Severity | Step(s) |
|---------------------------------------|----------|---------|
| Service + Storage paralleli | Critica | S11 |
| OAuth JSON + SQLite | Critica | S1–S6 |
| Callback OAuth senza transazione DB | Critica | S5a |
| StorageData RAM autoritativa | Alta | S11 |
| Tabelle legacy + canoniche | Alta | S9–S10 |
| Colonne normali + `metadata_json` | Alta | S7–S8 |
| `DeleteChannel` e `RevokeToken` separati | Alta | S5d |
| `save`, `SyncGroup`, `SaveData`, reconcile | Media | S11 |
| Molte directory possibili per OAuth | Media | S6 |

## Dependency DAG

```
                                  ┌──────────────────────────────────┐
                                  │ S11 (unify Service + Storage)    │
                                  │  blocks correctness S5+S6+S10    │
                                  └──────────────────────────────────┘
                                              ▲
                       ┌──────────────────────┴──────────────────────┐
                       │                                             │
   ┌────────────┐    ┌──▼───────────┐                                 │
   │ S1 (table) │───►│ S2 (encrypt) │──► S3 (backfill JSON→SQLite) ──►│ S4 (FK validation)
   └────────────┘    └──────┬───────┘                                 │
                            │                                         │
                            ├────► S5a (callback txn) ──┐             │
                            ├────► S5b (refresh o.go) ───┤             │
                            ├────► S5c (refresh Valid.) ─┤             │
                            └────► S5d (revoke) ─────────┤             │
                                                          ▼             │
                                            ┌────────────────┐          │
                                            │   S6 (no JSON) │          │
                                            └──────┬─────────┘          │
                                                   ▼                    │
                                       ┌────────────────────┐          │
                                       │ S7 (backfill JSON) │          │
                                       └─────────┬──────────┘          │
                                                 ▼                     │
                                       ┌────────────────────┐          │
                                       │ S8 (drop column)   │          │
                                       └────────────────────┘          │
                                                                        │
   ┌──────────┐                                ┌─────────────┐          │
   │ S9       │ ─────────────────────────────► │ S10 (rename)│ ─────────┘
   │ (legacy) │                                └─────────────┘
   └──────────┘

   S12 (CI guard) ◄── last gate, blocks any non-canonical write path
```

Edge direction = "depends on" (must be done before). S1+S2 are the
foundation; S3–S6 cover the encrypted OAuth pipeline; S7–S8 collapse the
domain metadata side; S9–S10 clean the schema; S11 is the runtime
unification; S12 enforces all the rest in CI.

## Step status

| # | Step | Pri | Status | Branch | Commit | Notes |
|---|------|-----|--------|--------|--------|-------|
| 1 | Create `youtube_oauth_tokens` table | P0 | **DONE** | feat/youtube-fix-oauth-callback | `e9e09e70` | FK cascade to `youtube_channels` |
| 2 | Encryption at-rest + external key | P0 | **DONE** | feat/youtube-fix-oauth-callback | `e9e09e70` | AES-256-GCM, env var key |
| 3 | Backfill `account_*.json` → `youtube_oauth_tokens` on startup | P0 | TODO | — | — | scan `<dataDir>/secrets/youtube/tokens/account_*.json` |
| 4 | Verify every token row has matching `youtube_channels.channel_id` | P0 | PARTIAL | feat/youtube-fix-oauth-callback | `e9e09e70` | FK enforces insert; startup orphan-audit TBD |
| 5a | `HandleOAuthCallback` writes to SQLite inside a txn | P0 | **DONE** | feat/youtube-fix-oauth-callback | `e9e09e70` | fail-closed on cipher / SQLite error |
| 5b | `oauth.go` `PersistedTokenSource.save` writes refreshed token to SQLite | P0 | **DONE** | feat/youtube-fix-oauth-callback | `e9e09e70` | preserves prior refresh-token blob |
| 5c | `auth_oauth.go` `ValidateToken` refresh writes to SQLite | P0 | TODO | — | — | separate code path, still JSON-only |
| 5d | `RevokeToken` via `YouTubeRepository.RevokeCredentials` | P1 | TODO | — | — | collapse `AuthManager`+`Service` Revoke pairs |
| 6 | Drop JSON readers + writers entirely | P1 | PARTIAL | feat/youtube-fix-oauth-callback | `e9e09e70` | writes still present (compat), readers `loadChannels` still work |
| 7 | Backfill useful fields from `metadata_json` → typed columns | P1 | TODO | — | — | audit first: which fields are still read? |
| 8 | Drop `metadata_json` column | P1 | TODO | — | — | after S7 |
| 9 | Drop legacy YouTube tables | P1 | DONE | — | migrations 008 + 009 | 4 tables dropped, data copied to canonical in S8's predecessor |
| 10 | Rename `youtube_groups_v2` → `youtube_groups` | P1 | TODO | — | — | migration `012_youtube_groups_rename.sql` |
| 11 | Unify `Service` + `Storage` into single runtime; drop `StorageData` + auth-maps | P0 | TODO | — | — | largest refactor; needs S5+S6+S10 already landed |
| 12 | CI guard: forbid `os.WriteFile`/`os.ReadDir` of tokens, `*_json` columns, legacy tables | P2 | TODO | — | — | scripts/ci_yt_guard.sh + `.golangci.yml` custom rule |

## Branch ladder (cumulative)

```
codex/drive-youtube-fixes   a2c539c4
       │
       ├──► feat/youtube-fix-refresh-metadata   504a42fb (Fix A: S4-already-done partial)
       │       │
       │       └──► feat/youtube-fix-oauth-callback  e9e09e70 (Fix B: S1, S2, S5a, S5b, S9)
       │               │
       │               ├──► docs/youtube-sqlite-migration-plan  (THIS PLAN FILE)
       │               ├──► feat/youtube-fix-validate-refresh   (next: S5c)
       │               ├──► feat/youtube-oauth-backfill         (next: S3, S4-strict)
       │               └──► feat/youtube-fix-revoke-diverge     (next: S5d, S6 partial)
       │
       └──► refactor/youtube-unify-runtime                     (later: S11)
                       │
                       └──► feat/youtube-deprecate-json         (last: S7, S8, S10, S12)
```

## Step details (acceptance criteria)

### S1. Create `youtube_oauth_tokens`

- [x] Migration `011_youtube_oauth_tokens.sql`
- [x] `channel_id` PK with FK cascade to `youtube_channels`
- [x] Columns: `access_token_encrypted BLOB NOT NULL`,
      `refresh_token_encrypted BLOB`, `token_type`, `expiry`,
      `scopes`, `key_version`, `revoked_at`, `created_at`, `updated_at`
- [x] Indexes on `revoked_at`, `key_version`
- [x] Test: `TestYouTubeOAuthTokenChannelFKDeleteCascade`

### S2. Encryption at-rest + external key

- [x] `internal/secrets/aesgcm` package, AES-256-GCM
- [x] BLOB shape: `nonce(12) || ciphertext || tag(16)`
- [x] Env resolution: `VELOX_YT_OAUTH_TOKEN_KEY` (base64 32B) OR `VELOX_YT_OAUTH_TOKEN_KEY_FILE`
- [x] `LoadFromEnv(requireIfMissing=false)` → degraded boot OK if cipher missing
- [x] Tests: round-trip, nonce uniqueness, tamper detection, wrong-key
      detection, nil-safety, all four env resolution paths

### S3. Backfill JSON → SQLite on startup

- [ ] `Service.SyncYouTubeOAuthTokensFromJSON(cipher, tokensDir)` walks `account_*.json`
- [ ] Encrypts each, UPSERTs into `youtube_oauth_tokens` (idempotent)
- [ ] Idempotent: re-running yields 0 changes
- [ ] Test: write 2 JSON files in tmpdir → backfill → assert 2 rows exist + decrypts
      back to original plaintext
- [ ] Operator hook: skip if cipher is nil (degraded mode keeps JSON as fallback)
- [ ] Logging: counts (`imported`, `updated`, `errored`)

### S4. Verify every token row has matching channel

- [x] FK constraint prevents insert with non-existent `channel_id` (`ON DELETE CASCADE`)
- [ ] Startup audit: log `WARN` if `youtube_oauth_tokens.channel_id NOT IN
      (SELECT channel_id FROM youtube_channels)` row count > 0
- [ ] Auto-cleanup path: DELETE orphan oauth rows OR upsert channel row from
      data we have on file (decision TBD)

### S5a. OAuth callback txn

- [x] `HandleOAuthCallback` encrypts + UPSERTs BEFORE any JSON/RAM update
- [x] Fail-closed: cipher error → return error; SQLite error → return error
      (no partial state observed on success return)
- [x] `log.Printf("[WARN]")` when cipher nil (JSON-only degraded mode)

### S5b. Refresh txn (oauth.go PersistedTokenSource.save)

- [x] Encrypts new access token, UPSERTs into SQLite
- [x] Preserves previously-stored refresh_token blob when new one is empty
      (so an access-only rotation doesn't wipe the refresh secret)
- [ ] Integration test with stub OAuth token source asserting the row update

### S5c. Refresh txn (auth_oauth.go ValidateToken)

- [ ] Same fix as S5b but in the separate `ValidateToken` refresh branch
      (currently `channels.go` → `auth_oauth.go:144-149`)
- [ ] Extract a shared helper `Service.persistRefreshedToken(channel, newToken)` so
      S5b and S5c can't diverge again
- [ ] Test: cover the `ValidateToken` refresh path explicitly

### S5d. RevokeToken via repository

- [ ] Single `Repository.RevokeCredentials(channelID)` on `YouTubeStore`
      (delegates to `MarkYouTubeOAuthTokenRevoked` + deletes from `s.channels`)
- [ ] Collapse `Service.RevokeToken` + `AuthManager.RevokeToken` into one
- [ ] HTTP handler `/api/v1/youtube/revoke` uses the unified path
- [ ] Test: revoked_at stamp + RAM map delete + Google revoke endpoint call

### S6. Drop JSON readers + writers (full)

- [ ] Remove `Service.loadChannels` and `Service.loadChannelFromToken` (or rewrite
      to read from SQLite + decrypt)
- [ ] Remove `Service.saveChannelToken`; remove `AuthManager.saveChannelToken`
- [ ] Remove `migration_consolidate_tokens.go` JSON helpers — superseded by S3
- [ ] Update `legacy_json_registry` migration: mark `youtube/tokens/*.json` as
      `deprecation_trail` (so S12 CI catches future reintroductions)
- [ ] Tests:
      - `loadChannels` integration test reads from SQLite + decrypts
      - `scripts/ci_yt_guard.sh` confirms zero `os.WriteFile`/`os.ReadDir` callers
        in `internal/integrations/youtube/`

### S7. Backfill useful `metadata_json` fields → typed columns

- [ ] Audit: `grep -n 'metadata_json' DataServer/internal/integrations/youtube/`
      to find which fields are still being read
- [ ] Decide which typed columns get populated (`notes`? `language` up to a max?)
- [ ] One-shot migration `013_metadata_json_backfill.sql` that copies intended
      fields then sets `metadata_json = '{}'`
- [ ] Test: backfill on existing test DB shows zero fields lost

### S8. Drop `metadata_json`

- [ ] Migration `014_drop_metadata_json.sql`:
      `ALTER TABLE youtube_channels DROP COLUMN metadata_json`
- [ ] Update `sqlite_youtube_entities.go` SELECT/UPSERT to remove the field
- [ ] Update all callers (handlers, audit endpoints, drive integration test)

### S9. Drop legacy YouTube tables

- [x] Migration `008_drop_legacy_tables.sql` data copy
- [x] Migration `009_drop_legacy_tables.sql` final DROP
- [x] Tables gone: `youtube_channel_metadata`, `youtube_groups`,
      `youtube_manager_channels`, `youtube_manager_groups`
- [ ] Docs/Tooling: update CHANGELOG, delete `storage_persistence.go` references
      to legacy tables

### S10. Rename `youtube_groups_v2` → `youtube_groups`

- [ ] Migration `012_youtube_groups_rename.sql`:
      `ALTER TABLE youtube_groups_v2 RENAME TO youtube_groups`
- [ ] Update `YouTubeStore` interface methods (`ListYouTubeGroupsV2` → `ListYouTubeGroups` etc.)
- [ ] Grep the codebase for any code still using the old table name

### S11. Unify runtime

The biggest refactor. Targets: kill `StorageData`, kill `Service.channels`/groups
maps as authoritative state, kill `Storage` struct.

- [ ] Define `YouTubeRepository` as single interface:
      channel, oauth-token, group, membership operations only — no `StorageData`
- [ ] Replace `Service.channels`/`Service.groups` maps with:
      - a read-through cache (`map[string]*AuthChannel`, TTL = infinity OR
        invalidated on each commit) — cache only, never authoritative
      - RAM cache invalidation hooks after every write
- [ ] Drop `Storage` struct entirely (or fold into `Service.init`)
- [ ] Drop `Service.UpdateChannelMetadata`'s `metadata_json` leak (Step S8 also)
- [ ] `YouTubeManager` no longer needs `m.youtubeStorage` — only `Service`
- [ ] `internal/modules/youtube/module.go.NewYouTubeHandlers` takes one Service
      argument
- [ ] Test: rebuild existing integration tests against the unified surface

### S12. CI guard

- [ ] `scripts/ci_yt_guard.sh`: static grep checks on
      `internal/integrations/youtube/`:
      - forbid new `os.WriteFile` / `os.Create` / `ioutil.WriteFile` for persistence
      - forbid new `os.ReadDir` of `tokens` dirs
      - forbid new column names matching `*_json`
      - forbid new `CREATE TABLE youtube_*` outside of migrations dir
- [ ] Forbid CRUD on `youtube_channel_metadata`, `youtube_groups`
      (old names), `youtube_manager_*`
- [ ] Wire into `.github/workflows/ci.yml` (or equivalent) so the guard runs on PRs

## Open questions for the team (block before S6/S11)

| # | Question | Blocking step | Suggested default if no answer |
|---|----------|---------------|--------------------------------|
| Q1 | Encryption key rotation policy — `key_version` declared; populate it from `LoadFromEnv` always, or wait for v2? | S2 polish | always populate=1; add v2 infra in S2 plus a follow-up |
| Q2 | S3 backfill: run automatically on every server startup, or a one-shot CLI command (`velox-server migrate-youtube-tokens`)? | S3 | run on startup but make the function exposed via a CLI flag for ops runs |
| Q3 | S7 metadata_json: which fields are still read by the team? Audit needed. | S7 | carry forward the same JSON content into typed `notes` column (lossy but conservative) |
| Q4 | S11 unification: backwards-compatible (`Service` keeps current method signatures, delegates) or a single big-bang rewrite? | S11 | backwards-compatible: keep signatures, route everything through the repository |
| Q5 | S6 JSON removal: keep a debug `--allow-legacy-youtube-json` flag for emergency rollback during rollout? | S6 | yes, behind an env var for one release |
| Q6 | `youtube_oauth_tokens.scopes` column: split into normalized table, or keep as space-separated string? | S2 polish | keep as string for v1; normalized later when we add per-scope features |

## Done so far

- **2026-06-16** — Commit `504a42fb` on `feat/youtube-fix-refresh-metadata`
  (Fix A: `RefreshChannelMetadata` now uses `UpdateYouTubeChannelMetadata`).
  Closes part of claim 4 and the JSON-leak window for refresh paths.
- **2026-06-16** — Commit `e9e09e70` on `feat/youtube-fix-oauth-callback`
  (Fix B: `youtube_oauth_tokens` table + AES-256-GCM at-rest +
  `HandleOAuthCallback` and `oauth.go` refresh now write to SQLite, fail-closed).
  Closes S1, S2, S5a, S5b, S9 (partial).
- **2026-06-16** — Commit `e9e09e70` validation: `go vet` ✓, `go build` ✓,
  `internal/secrets/...` tests ✓, `internal/store/...` tests ✓,
  `internal/integrations/youtube/...` tests ✓, `internal/modules/youtube/...` tests ✓.

## References

- Verdict document — chat-pasted 16-jun-2026 (file-id `## Verdetto` / 12 migration step list)
- `migrations/009_drop_legacy_tables.sql` — irreversible DROP of legacy YouTube tables
- `migrations/011_youtube_oauth_tokens.sql` — canonical OAuth credentials table
- `internal/secrets/aesgcm/encryptor.go` — AES-256-GCM at-rest cipher
- `internal/store/sqlite_youtube_entities.go` — `UpsertYouTubeOAuthToken` /
  `GetYouTubeOAuthToken` / `MarkYouTubeOAuthTokenRevoked`
- `internal/integrations/youtube/auth_oauth.go:66-99` — encrypted SQLite write path
- `internal/integrations/youtube/oauth.go:38-100` — refresh encrypted-write path
- `internal/integrations/youtube/service.go` — `YouTubeStore` interface,
  `Service.oauthBuf`, `SetOAuthSecretCipher`
- `internal/modules/youtube/module.go:62-74` — cipher resolution at startup

## Status update cadence

- After each step enters `DONE`: update the status table, add the commit SHA,
  and add an entry to the "Done so far" section.
- Reviewers: read this file top-to-bottom on PR review.
- Owner of this file: Whoever is in-flight on steps S5–S12.
