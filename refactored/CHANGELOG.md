# Changelog

## Unreleased

### 🔐 YouTube SQLite-only contract (S3 / S4 / S5a+ / S5c / S5d / S6 closed)

The YouTube integration is now SQLite-only for OAuth credentials and
runtime state. The "no runtime filesystem persistence for credentials"
verdict is in effect end-to-end. The contract every operator and code
path must honour:

- **Cipher mandatory**: `aesgcm.LoadFromEnv(true)` (`requireIfMissing=true`)
  gates boot. With no `VELOX_YT_OAUTH_TOKEN_KEY` (or `_FILE` sibling) the
  server fails closed — no degraded JSON fallback is available any more.
  `internal/modules/youtube/module.go` wires the cipher into the
  service before any OAuth route registers.
- **No JSON dual-write**: `Service.saveChannelToken` / `channels.go`'s
  JSON-compat writer / `migration_consolidate_tokens.go` are all gone.
  `RevokeToken`'s `os.Remove`-on-`account_*.json` step is removed;
  `RevokeCredentials` is the single repository op. There is no second
  write path that could race the canonical SQLite write and silently
  overwrite fresh credentials.
- **`ConnectChannelAtomic` narrow UPDATE**: re-auth on an existing
  channel only touches seed-owned columns (`title`, `thumbnail_url`,
  `last_sync_at`, `updated_at`). User-edited typed columns (`notes`,
  `language`, `view_count`, `subscriber_count`, `display_name`,
  `channel_url`, `added_at`, `created_at`) and the metadata blob are
  preserved verbatim across re-auth.
- **`DeleteChannel` is DB-first**: `Service.DeleteChannel` calls
  `store.DeleteChannelAtomic` (transactional cleanup of memberships +
  channel row + FK-cascade on the oauth row) BEFORE the in-RAM entry
  is deleted. A failed SQL leaves RAM untouched, so retry runs against
  the same state. Decrypted plaintext tokens never touch the network
  after this call.
- **Tests pinning the contract**: `TestConnectChannelAtomic_FirstTimeConnect`,
  `TestConnectChannelAtomic_PreservesUserEdits`,
  `TestLoadOAuthChannelsFromSQLiteHydratesCache`,
  `TestYouTubeOAuthTokenChannelFKDeleteCascade`. A regression on any
  of the four contract bullets above trips one of these tests.
- **Operator pre-flight for SQLite < 3.35.0**: migration `014_drop_metadata_json.sql`
  uses `ALTER TABLE … DROP COLUMN` which is unsupported before
  SQLite 3.35.0. Run the audit query documented in
  `internal/store/migrations/013_metadata_json_backfill.sql` before
  applying the 014 migration on a deployment pinned to an older
  system SQLite.

### 🧹 Legacy Cleanup (2026-06 final)

- **HTTP worker control plane fully removed**: the worker agent no longer ships `RegisterWorker`, `UnregisterWorker`, `SendHeartbeat`, `GetCommands`, `AckCommand`, `AckCommandByID`, `UpdateStatus`, or the non-v2 job methods (`GetJob`, `SubmitJobResult`, `CompleteJob`, `RenewJobLease`). All control-plane traffic is now exclusively the gRPC `WorkerControl` bidi stream (`DataServer/internal/grpcserver/`). The V2 HTTP endpoints (`GetJobV2`, `SubmitJobResultV2`, `CompleteJobV2`, `RenewJobLeaseV2`) are still used by the data-plane bridge but no longer fall back to the legacy `/api/jobs/*` paths. Result: the recurring `404 /api/workers/register` entries stop appearing in master logs.
- **Legacy Docker `velox-worker-host_*` script deprecated**: `scripts/local-workers.sh` → `scripts/local-workers.sh.deprecated` (now a loud-exit stub). Production and staging deploys use the systemd Go worker agent (`RemoteCodex/native/worker-agent-go`) on public IPs.
- **Tailscale references purged**: `DataServer/data/ansible/playbooks/inventory.example.ini` now uses `WORKER_PUBLIC_IP` as the placeholder (was `TAILSCALE_IP`); `RemoteCodex/scripts/cleanup-worker.sh` defaults to `http://127.0.0.1:8000` instead of the historical Tailscale peer IP; `DataServer/data/ansible/playbooks/tasks/installer_heartbeat.yml` no longer POSTs to the legacy `/api/workers/register` endpoint (preflight is now `/health` only).
- **2026-06-13 historical entry neutralized**: removed explicit Tailscale IP and per-host references from the historical CHANGELOG block — the operators' notes are kept without leaking real network topology.

### 🧹 Legacy Cleanup (2026-06 cleanup pass)
- **Orphan diagnostics endpoint removed**: `internal/handlers/server/diagnostics/diagnostics.go` deleted after a 4-step orphan verification (0 imports of `"velox-server/internal/handlers/server/diagnostics"` anywhere in the codebase, 0 module-registry references in `internal/app/registry.go`, 0 wiring in `cmd/server/router.go`, 0 path-string occurrences in any `*.go` file). The exposed `Legacy`/`LegacyExists` JSON telemetry had no downstream consumers outside the audit subsystem (which uses its own internal fields, not from this package). See commit `1ec7c411` for the full commit message and kept-with-reason notes.
- **Stale build artifacts reclaimed (~107MB)**: physically deleted `bin/velox-server` (56.5MB build from June 14) and `velox-server` (50.3MB build from June 10, project root). Both already covered by `DataServer/bin/` + `DataServer/velox-server` patterns in `.gitignore` (repo root) so fresh clones do not surface them as untracked noise.
- **Stale backups reclaimed**: `data/backups/` (~7 `*.tar.gz` from June 7-10) physically deleted. Already covered by the `DataServer/data/backups/` pattern in `.gitignore`.
- **Cleanup pass intent**: continued removal of legacy/dead code paths while preserving intentional fallback behavior. The cleanup explicitly **kept** unchanged: SQL migration files (`internal/store/migrations/0XX_*.sql`, sequential SQLite migrations required for fresh-DB installs); commented `legacy`/`backward compat` references in `config/config.go`, `integrations/youtube/models.go`, `workers/auth.go`, etc. (intentional fallbacks actively exercised by tests such as `data_layer_test.go`); `RemoteCodex/*` (separate codebase). Runtime state dirs (`DataServer/data/{analytics,dark_editor,drive,jobs,secrets,worker_downloads,youtube}/`, `completed_videos/`, `secrets/`, `drive/{credentials,tokens}/`) and database files (`*.db`/`*.db-shm`/`*.db-wal`) are already covered in `.gitignore`.
- **Validation gate**: `go build ./...` + `go vet ./...` + test sweep on `internal/audit/...` + `internal/handlers/...` + `internal/logging/...` + `internal/store/...` + `internal/workers/...` — all green pre/post deletion.

## 2026-06-14 — v1.1.0

### 🚀 SQLite Persistence Migration
- **JSON→SQLite legacy importer**: Created `legacy_importer.go` with SHA-256 checksum idempotency, automatic timestamped backups (`.bak`), and tracking via `legacy_imports` table
- **velox-migrate-json CLI**: New command with `inventory`, `dry-run`, `apply` subcommands for managing legacy JSON data migration
- **Queue persistence**: DLQ, EventLogger, Orchestrator now use SQLite as source of truth with JSON file fallback (migrations 007-008)
- **SQLite persistence tables**: Created `queue_jobs`, `orchestrator_jobs`, `job_events`, `dlq_jobs`, `legacy_imports` tables

### 🔐 Ansible Secret Resolver
- **SSHPassword→secret_ref migration**: Credential secrets written to `secrets/ansible/ssh_host_*` files (0600 permissions), never stored in plaintext in SQLite
- **SecretResolver**: New `secrets.go` with `StoreSSHPassword()`, `BuildSecretRef()`, `Resolve()` — used by `manager_computers.go` at save time
- **Inventory generation**: `manager_runs.go` now resolves `secret_ref` at runtime instead of embedding passwords in inventory files

### 🎬 YouTube Canonical Model
- **Service init order fixed**: `NewService()` now accepts `YouTubeStore` directly — data loaded immediately from constructor; `SetStore()` is a no-op if store already provided
- **Canonical tables**: Groups/channels loaded exclusively from `youtube_groups_v2` + `youtube_group_channels` (canonical). Legacy fallback removed — tables dropped by migration 008.
- **StorageStore**: Unified `load()` from canonical tables + legacy fallback; `save()` now propagates errors with `fmt.Errorf` instead of silently swallowing them

### 🛡️ Data-Layer Audit & CI
- **DataLayerAuditor**: New `audit/data_layer.go` checks for legacy files, duplicate sources of truth, naming consistency, missing primary files, and database integrity
- **JSON guard**: Integrated into `dod_check.sh` — checks 8 legacy JSON sources, validates data integrity via `velox-migrate-json dry-run`, monitors `legacy_imports` tracking table
- **Ansible syntax fix**: Quoted `name` string in `install_workers.yml` to fix YAML colon-space parsing error

### 🧪 Testing
- **Audit tests**: 22 tests for `data_layer.go` covering duplicate sources, naming consistency, database checks, AllowLegacy, FailOnError, String output
- **Legacy importer tests**: 24 tests for `legacy_importer.go` covering `countJSONRecords`, `createJSONBackup`, `toInt64`, `sanitizeFilename`, import functions (workers, youtube channels/groups), idempotency

### 🔧 Code Quality
- **Error propagation**: All SQLite errors in `storage.go`, `groups.go`, `manager_runs.go`, `manager_computers.go` — no more `_ =` ignored errors
- **Channels type fix**: `row["channels"].(string)` → `row["channels"].([]string)` in handler `groups.go`; fixed `ListGroups()` misuse in `youtube_groups.go` (second return is tracked niches, not error)
- **DoD scripts unified**: Merged `dod_check.sh` + `dod-check.sh` → single unified `dod_check.sh` (8 gates); extracted common framework → `scripts/lib/common.sh`

## 2026-06-13

### 🐛 Bug Fixes
- **SPA Frontend**: Fixed `frontend/module.go` — now serves SPA from default path `frontend_standalone/web/dist` when `VELOX_SPA_DIR` is not set
- **NoRoute handler**: Fixed `proxy/noroute.go` — corrected condition `Size() >= 0` → `Size() > 0` so SPA responses are properly detected

### 🚀 Infrastructure
- **Systemd env**: Created `/etc/velox-server.env` with `VELOX_ADMIN_TOKEN`, `VELOX_MASTER_PORT`, `VELOX_RUNTIME_DIR` for persistent configuration
- **Data sync**: All YouTube data (84 channels, 49 OAuth tokens, channels.json, ChannelsSaved.json) synced to `.velox/` with correct `velox:velox` ownership
- **Frontend symlink**: Created permanent symlink `frontend_standalone → refactored/frontend_standalone`
- **Git**: Code fixes committed and pushed to `origin/main`

### 🔧 Worker Management
- **Worker connectivity**: Connected remote workers to the master over public IP/DNS (no VPN mesh). Worker access now goes via the gRPC `WorkerControl` stream on the master (see `DataServer/internal/grpcserver/`).
- **Worker configs fixed**: Updated `master_url` on the three remote workers from prior environment-specific endpoints to `51.91.11.36:8000` (historical entry — keep for context).
- **Docker hosts decommissioned**: The earlier Docker-based per-host worker containers were replaced by a single Go worker-agent binary running under systemd; the legacy per-host containers have been disabled across the fleet.

### ✅ Testing
- **Job template**: Created `docs/api/job-template-endpoint.md` with reusable job payload template
- **E2E test**: Successfully submitted and completed a content generation job to Worker 77 (via tunnel)
  - Generated script, 5 AI scenes, voiceovers for 6 languages (EN, PT, PL, FR, DE, RU)
  - Uploaded assets to Google Drive
  - Created Drive folder and Google Doc

### 💡 Docs
- **API docs**: Created `docs/api/job-template-endpoint.md` with full payload reference for worker content generation API
