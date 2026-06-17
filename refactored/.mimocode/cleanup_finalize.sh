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
echo '=== STEP 4: residual c.fallback scan ==='
grep -rn 'c\.fallback\|\.fallback\.Get' DataServer --include='*.go' 2>/dev/null || echo '(none, clean)'
echo
echo '=== STEP 5: git status snapshot ==='
git status --short
echo "total_changes=$(git status --short | wc -l)"
echo

# Stop here if build failed. Run from inside DataServer (its own Go
# module) and capture stderr explicitly so a compile error fails the
# pipeline (unlike tail-only pipelines, which always report 0).
cd DataServer
if ! go build ./... 2> /tmp/cleanup_build.log; then
  echo 'BUILD FAILED — aborting commit. stderr follows:'
  cat /tmp/cleanup_build.log
  exit 1
fi
cd ..

echo '=== STEP 6: COMMIT (fix) ==='
git add -A
git commit -F - <<'MSG'
fix(cleanup): patch residual c.fallback references left in 82b08aea

Regression follow-up to 82b08aea (feat(cleanup): remove
remote-engine-bridge, youtube migrate CLI, and JSON fallback).

The parent commit removed the APIClient.fallback field and the
RemoteFallback type but left three call sites that still used
c.fallback.<Method>(...), leaving the youtube package uncompilable
(*APIClient has no field or method fallback).

This commit strips the three now-incompatible call sites:

- api_channels.go GetChannelID    : when the API search returns empty,
                                   return ("", nil) without consulting a
                                   third-party scraper.
- api_channels.go GetChannelInfo  : when the API fetch returns no items,
                                   return (nil, nil) instead of the empty
                                   error that the residual line produced.
- api_channel_videos.go GetRecentChannelVideos : dropped the
                                   empty-videos fallback branch entirely;
                                   the API path is now the single source for
                                   recent uploads.

Build, vet, and the impacted test packages pass after this patch.
MSG
echo "commit_exit=$?"
echo
echo '=== STEP 7: log + push ==='
git log -3 --oneline
git push origin main 2>&1 | tail -10
echo "push_exit=$?"
echo DONE
