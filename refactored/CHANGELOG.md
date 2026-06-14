# Changelog

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
- **Canonical tables**: Groups/channels loaded from `youtube_groups_v2` + `youtube_group_channels` (canonical) with fallback to legacy `youtube_groups` / `youtube_channel_metadata`
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
- **Tailscale**: Connected this server to Tailscale network for worker access
- **Worker configs fixed**: Updated `master_url` on pi1/pi2/pi3 workers from wrong endpoints to `51.91.11.36:8000`
- **Docker revived**: Successfully restarted Docker daemon and recovered 3 container workers on local machine
  - `velox-worker-host_57_129_132_133` ✅
  - `velox-worker-host_149_56_131_97` ✅
  - `velox-worker-host_51_222_204_158` ✅
- **Remote workers**: pi1 worker `host_57_129_132_133` (100.120.146.5) connected via Tailscale

### ✅ Testing
- **Job template**: Created `docs/api/job-template-endpoint.md` with reusable job payload template
- **E2E test**: Successfully submitted and completed a content generation job to Worker 77 (via tunnel)
  - Generated script, 5 AI scenes, voiceovers for 6 languages (EN, PT, PL, FR, DE, RU)
  - Uploaded assets to Google Drive
  - Created Drive folder and Google Doc

### 💡 Docs
- **API docs**: Created `docs/api/job-template-endpoint.md` with full payload reference for worker content generation API
