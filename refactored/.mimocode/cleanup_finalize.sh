#!/bin/bash
set +e
cd /home/pierone/Pyt/VeloxLEgit/refactored

echo '=== orphan remote-engine-bridge check ==='
if [ -e DataServer/remote-engine-bridge ] && [ ! -d DataServer/remote-engine-bridge ]; then
  rm -f DataServer/remote-engine-bridge
  echo 'removed broken symlink/file'
fi
if [ -L DataServer/remote-engine-bridge ]; then
  rm -f DataServer/remote-engine-bridge
  echo 'removed dangling symlink'
fi
echo

echo '=== STEP 1: go build ./... ==='
cd DataServer
go build ./... 2>&1 | tail -50
echo "build_exit=$?"
echo
echo '=== STEP 2: go vet ./... ==='
go vet ./... 2>&1 | tail -30
echo "vet_exit=$?"
echo
echo '=== STEP 3a: youtube package tests ==='
go test -count=1 -short ./internal/integrations/youtube/... 2>&1 | tail -30
echo "test_yt_exit=$?"
echo
echo '=== STEP 3b: handlers youtube tests ==='
go test -count=1 -short ./internal/handlers/server/youtube/... 2>&1 | tail -30
echo "test_handlers_exit=$?"
echo
echo '=== STEP 3c: config + modules + cmd tests ==='
go test -count=1 -short ./internal/config/... ./internal/modules/... ./cmd/... 2>&1 | tail -30
echo "test_others_exit=$?"
echo
cd ..
echo '=== STEP 4: residual symbol scan ==='
grep -rn 'RemoteFallback\|NewRemoteFallback\|ConsolidateOAuthTokens\|BackfillOAuthTokensFromJSON\|runMigrate' DataServer --include='*.go' 2>/dev/null | grep -v 'oauth_refresh_test.go' || echo '(none, clean)'
echo
echo '=== STEP 5: stale comment scan ==='
grep -rn 'JSON-fallback\|fallback JSON\|velox-server migrate' DataServer --include='*.go' 2>/dev/null || echo '(none, clean)'
echo
echo '=== STEP 6: git status snapshot ==='
git status --short
echo "total_changes=$(git status --short | wc -l)"
echo
echo '=== STEP 7: COMMIT ==='
git add -A
git commit -F - <<'MSG'
feat(cleanup): remove remote-engine-bridge, youtube migrate CLI, and JSON fallback

HIGH-scope cleanup of the Velox server backplane:

Cleanup (7 files removed):
- DataServer/cmd/remote-engine-bridge/main.go (orphan dev-only HTTP bridge
  for /api/script/generate-with-images; production pipeline already talks
  to the canonical engine, the bridge was never wired into any caller).
- DataServer/cmd/server/migrate.go + the youtube-oauth-json subcommand
  dispatcher (idempotent one-shot CLI; legacy JSON layouts no longer
  exist on any canonical install).
- DataServer/internal/integrations/youtube/consolidate.go (+ test)
- DataServer/internal/integrations/youtube/backfill.go (+ test)
- DataServer/internal/integrations/youtube/remote_fallback.go

API and config changes (9 files modified):
- youtube.APIClient.NewAPIClient drops the fallbackURL parameter and the
  fallback field. Quota issues now surface to the operator instead of
  being silently masked by a third-party scraper.
- manager NewYouTubeManager drops the fallbackURL parameter.
- internal/config: removed YouTubeConfig.RemoteFallback, top-level
  Config.RemoteFallbackURL and the VELOX_REMOTE_FALLBACK_URL env var.
- internal/integrations/youtube/service.go NewService + boot hydrator
  comments: SQLite-only rehydration, no JSON fallback.
- internal/integrations/youtube/service.go RevokeToken doc-comment
  rewritten from a 4-step to a 3-step sequence now that the JSON
  file-delete step is gone.
- cmd/server/main.go: removed the migrate dispatcher and the alias
  branch; legacy 'velox-server migrate <sub>' invocations now exit
  with the usage text and status 2 (cron scripts that relied on the
  silent fallback to boot the master must be updated).
- cmd/server/bootstrap.go: stale reference to cmd/server/migrate.go
  replaced with a historical note; boot path is SQLite-only.
- internal/modules/youtube/module.go: removed BackfillOAuthTokensFromJSON
  helper comment and the fallback URL wiring.

Notes:
- expiryTimeLayout constant remains in channels.go (other call sites use it).
- fakeYTStore helper remains in oauth_refresh_test.go (shared fixture for
  the OAuth refresh path; orphan check performed before deletion).
MSG
echo "commit_exit=$?"
echo
echo '=== STEP 8: log + push ==='
git log -1 --oneline
git push origin main 2>&1 | tail -10
echo "push_exit=$?"
echo
echo DONE
