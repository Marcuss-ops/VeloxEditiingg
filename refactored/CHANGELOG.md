# Changelog

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
