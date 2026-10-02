#!/usr/bin/env bash
# dev-master.sh — fast local loop for Velox master development.
#
# Why this exists: a full CI release (race matrix + image build + sign)
# takes 15-30+ minutes from push to a deployable image. Most intake /
# validation / projection changes can be verified in seconds locally:
#   1. go build + go test on touched packages (seconds)
#   2. this script: run the REAL master binary on scratch ports/DB (1-2 min)
#   3. local docker image (first build slow, then cached)
#   4. CI release image (production only)
# See docs/DEV-LOOP.md for the tier guide with measured times.
#
# Usage:
#   scripts/ops/dev-master.sh build                 # compile ./velox-server-dev (repo root/DataServer)
#   scripts/ops/dev-master.sh run                   # build + serve dev master on :18000 (scratch DB)
#   scripts/ops/dev-master.sh dry-run <payload.json> [endpoint] [master]
#                                                   # POST payload to ?dry_run=true, print summary+warnings
#   scripts/ops/dev-master.sh build-image [tag]     # docker build local image (default tag velox-server:local)
#   scripts/ops/dev-master.sh push-image [tag]      # build + push ghcr.io/<owner>/velox-server:<tag>
#
# The dev master NEVER touches production: fresh scratch DB under
# $DEV_DIR, HTTP :18000, gRPC :19000, fixed dev admin token printed at boot.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEV_DIR="${VELOX_DEV_DIR:-/tmp/velox-dev-master}"
# NOTE: :18000/:18200 on this host are SSH tunnels to the OVH production
# master (see the -L flags on the pierone@51.91.11.36 ssh process) — the
# dev master defaults to :18001/:19001 to stay clear of them AND of the
# local production container (:8000/:9000).
DEV_HTTP_PORT="${VELOX_DEV_HTTP_PORT:-18001}"
DEV_GRPC_PORT="${VELOX_DEV_GRPC_PORT:-19001}"
DEV_ADMIN_TOKEN="velox-dev-local-token"
BIN="$REPO_ROOT/DataServer/velox-server-dev"

cmd_build() {
  echo "==> go build ./cmd/server -> $BIN"
  (cd "$REPO_ROOT/DataServer" && go build -o "$BIN" ./cmd/server)
  echo "built: $(du -h "$BIN" | cut -f1)"
}

cmd_run() {
  cmd_build
  mkdir -p "$DEV_DIR/data"
  # The boot data-layer audit requires the DB file to exist; an empty
  # file is fine (migrations initialize the schema). Production data is
  # never copied here.
  touch "$DEV_DIR/data/velox.db"
  echo "==> dev master: http=:${DEV_HTTP_PORT} grpc=:${DEV_GRPC_PORT} db=$DEV_DIR/data/velox.db"
  echo "    admin token: $DEV_ADMIN_TOKEN (dev only, never production)"
  echo "    stop with Ctrl-C; production on :8000 is untouched"
  # Self-referential control-plane endpoints: the dev master only
  # serves validation/dry-run locally, so loopback dummies satisfy the
  # required-URL boot gate without reaching anything external.
  # Dev capability posture (AGENTS.md §6): smoke runs on explicit dev
  # doubles (VELOX_SMOKE_MODE=development + VELOX_ENVIRONMENT=development);
  # production Validate() rejects this combination.
  VELOX_ENVIRONMENT="development" \
  VELOX_SMOKE_MODE="development" \
  VELOX_GRPC_ALLOW_INSECURE_DEV="true" \
  VELOX_CONTROL_PLANE_REST_PUBLIC_URL="https://127.0.0.1:$DEV_HTTP_PORT" \
  VELOX_CONTROL_PLANE_REST_INTERNAL_URL="https://127.0.0.1:$DEV_HTTP_PORT" \
  VELOX_CONTROL_PLANE_GRPC_URL="127.0.0.1:$DEV_GRPC_PORT" \
  VELOX_DB_PATH="$DEV_DIR/data/velox.db" \
  VELOX_DATA_DIR="$DEV_DIR/data" \
  VELOX_ADMIN_TOKEN="$DEV_ADMIN_TOKEN" \
  VELOX_ALLOWED_WORKERS="dev-worker-01" \
  VELOX_MASTER_PORT="$DEV_HTTP_PORT" \
  VELOX_HTTP_PORT="$DEV_HTTP_PORT" \
  VELOX_GRPC_PORT="$DEV_GRPC_PORT" \
    exec "$BIN" serve
}

cmd_dryrun() {
  local payload="${1:?usage: dev-master.sh dry-run <payload.json> [endpoint] [master-url]}"
  local endpoint="${2:-/api/v1/creator/jobs}"
  local master="${3:-http://127.0.0.1:$DEV_HTTP_PORT}"
  local token="${VELOX_ADMIN_TOKEN:-$DEV_ADMIN_TOKEN}"
  echo "==> POST $master$endpoint?dry_run=true"
  curl -sS -m 60 -X POST "$master$endpoint?dry_run=true" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    --data @"$payload" | python3 -c "
import json,sys
d = json.load(sys.stdin)
print('ok:', d.get('ok'), 'dry_run:', d.get('dry_run'))
s = d.get('summary', {})
for k in ['scenes','declared_duration_s','clip_scenes','stock_scenes','voiceover_scenes',
          'overlays','runtime_assets','runtime_audio_present','final_audio_duration_s',
          'audio_coverage','copy_only']:
    if k in s: print(f'{k}: {s[k]}')
print('kind_clip_without_clip:', s.get('kind_clip_without_clip'))
for w in s.get('warnings', []) + d.get('warnings', []):
    print('WARNING', w.get('code'), '-', str(w.get('detail'))[:220])
"
}

cmd_build_image() {
  local tag="${1:-velox-server:local}"
  echo "==> docker buildx $tag (first build is slow: CGO + cold cache; later builds reuse layers)"
  (cd "$REPO_ROOT" && docker buildx build \
    -f DataServer/Dockerfile \
    -t "$tag" \
    --build-arg "VERSION=local-$(git rev-parse --short HEAD)" \
    --build-arg "BUILD_COMMIT=$(git rev-parse HEAD)" \
    .)
  echo "image ready: $tag"
}

cmd_push_image() {
  local tag="${1:?usage: dev-master.sh push-image <tag>  (e.g. bench-local-abc1234)}"
  local owner
  owner="$(gh api user -q .login 2>/dev/null | tr '[:upper:]' '[:lower:]')"
  local ref="ghcr.io/${owner}/velox-server:${tag}"
  (cd "$REPO_ROOT" && docker buildx build \
    -f DataServer/Dockerfile \
    -t "$ref" \
    --push \
    --build-arg "VERSION=${tag}" \
    --build-arg "BUILD_COMMIT=$(git rev-parse HEAD)" \
    .)
  echo "pushed: $ref"
  echo "digest: $(gh api \"/users/${owner}/packages/container/velox-server/versions\" -q '.[0].name')"
}

case "${1:-}" in
  build)       cmd_build ;;
  run)         cmd_run ;;
  dry-run)     shift; cmd_dryrun "$@" ;;
  build-image) shift; cmd_build_image "$@" ;;
  push-image)  shift; cmd_push_image "$@" ;;
  *) echo "usage: $0 {build|run|dry-run|build-image|push-image}" >&2; exit 2 ;;
esac
