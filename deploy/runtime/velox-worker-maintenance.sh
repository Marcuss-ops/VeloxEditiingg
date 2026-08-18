#!/usr/bin/env bash
set -euo pipefail

# Conservative, idempotent host maintenance for a canonical Velox worker.
# This only removes reproducible data. It never removes worker state, active
# asset caches, certificates, configuration, or the running image.

readonly ENV_FILE="/etc/velox-worker/worker.env"
readonly MAINTENANCE_DIR="/var/lib/velox-worker/maintenance"
readonly ROLLBACK_FILE="/etc/velox-worker/rollback-image"
readonly KEEP_DAYS="${VELOX_MAINTENANCE_KEEP_DAYS:-14}"
readonly CCACHE_MAX_SIZE="${VELOX_CCACHE_MAX_SIZE:-5G}"

log() {
  logger -t velox-worker-maintenance -- "$*" 2>/dev/null || true
  printf '[maintenance] %s\n' "$*"
}

env_value() {
  local key="$1"
  sed -n "s/^${key}=//p" "$ENV_FILE" 2>/dev/null | tail -n 1
}

image_id_for() {
  docker image inspect "$1" --format '{{.Id}}' 2>/dev/null || true
}

mkdir -p "$MAINTENANCE_DIR"

active_id="$(docker inspect --format '{{.Image}}' velox-worker 2>/dev/null || true)"
[[ -n "$active_id" ]] || { log "skip: velox-worker container is not present"; exit 0; }

rollback_ref="$(env_value VELOX_ROLLBACK_IMAGE)"
if [[ -z "$rollback_ref" && -r "$ROLLBACK_FILE" ]]; then
  rollback_ref="$(sed -n '1p' "$ROLLBACK_FILE")"
fi
rollback_id="$(image_id_for "$rollback_ref")"

# Older hosts predate VELOX_ROLLBACK_IMAGE. Select the newest tagged worker
# image other than the active one and persist that choice as the rollback pin.
if [[ -z "$rollback_id" || "$rollback_id" == "$active_id" ]]; then
  rollback_ref="$(docker image ls --no-trunc \
    --format '{{.Repository}}:{{.Tag}} {{.ID}} {{.CreatedAt}}' \
    | awk '$1 ~ /^(velox-worker|velox-worker-agent|ghcr.io\/marcuss-ops\/velox-worker):/ && $1 !~ /:<none>$/ && $2 != "'"$active_id"'"' \
    | sort -k3,4r \
    | awk 'NR == 1 { print $1 }')"
  rollback_id="$(image_id_for "$rollback_ref")"
fi

if [[ -n "$rollback_ref" && -n "$rollback_id" && "$rollback_id" != "$active_id" ]]; then
  install -o root -g root -m 0600 /dev/null "$ROLLBACK_FILE"
  printf '%s\n' "$rollback_ref" >"$ROLLBACK_FILE"
  log "protected active image=$active_id and rollback=$rollback_ref ($rollback_id)"
else
  rollback_ref=""
  rollback_id=""
  log "protected active image=$active_id; no separate rollback image found"
fi

protected_image() {
  [[ "$1" == "$active_id" || ( -n "$rollback_id" && "$1" == "$rollback_id" ) ]]
}

container_uses_running_image() {
  [[ -n "$(docker ps --filter "ancestor=$1" -q 2>/dev/null)" ]]
}

# Remove only exited containers older than KEEP_DAYS. Running containers are
# never touched; recent failed containers remain available for diagnosis.
cutoff_epoch="$(date -d "${KEEP_DAYS} days ago" +%s)"
while read -r container_id; do
  [[ -n "$container_id" ]] || continue
  created="$(docker inspect --format '{{.Created}}' "$container_id" 2>/dev/null || true)"
  created_epoch="$(date -d "$created" +%s 2>/dev/null || echo 0)"
  if (( created_epoch > 0 && created_epoch < cutoff_epoch )); then
    docker rm "$container_id" >/dev/null 2>&1 || true
    log "removed exited container older than ${KEEP_DAYS}d: $container_id"
  fi
done < <(docker ps -a --no-trunc --filter status=exited -q 2>/dev/null)

# Build cache is disposable on a worker host. The unfiltered form is
# intentional: older Docker installations ignored the `until` filter while
# reporting large reclaimable BuildKit caches.
docker builder prune --all --force >/dev/null 2>&1 || true
docker image prune --force >/dev/null 2>&1 || true

# Keep exactly the active image and one rollback candidate among worker
# images. Do not consider unrelated images (for example velox-server).
while read -r image_id repository tag; do
  [[ -n "$image_id" ]] || continue
  protected_image "$image_id" && continue
  container_uses_running_image "$image_id" && continue
  docker image rm -f "$image_id" >/dev/null 2>&1 || log "could not remove image $image_id ($repository:$tag); left intact"
done < <(
  docker image ls --no-trunc --format '{{.ID}} {{.Repository}} {{.Tag}}' \
    | awk '$2 ~ /^(velox-worker|velox-worker-agent|ghcr.io\/marcuss-ops\/velox-worker)$/ { print $1, $2, $3 }' \
    | sort -u
)

# Keep the developer cache useful but bounded. This is not the worker asset
# cache; it is the user's compiler cache under /home/pierone.
if id pierone >/dev/null 2>&1 && command -v ccache >/dev/null 2>&1; then
  runuser -u pierone -- ccache --max-size="$CCACHE_MAX_SIZE" >/dev/null 2>&1 || true
  runuser -u pierone -- ccache --cleanup >/dev/null 2>&1 || true
fi

# Chronon build staging is explicitly temporary. Only remove old staging
# trees, and only when no local build process is running.
if ! pgrep -af '(chronon|cmake|ninja|(^|[[:space:]])make([[:space:]]|$))' 2>/dev/null | grep -v velox-worker-maintenance >/dev/null; then
  while IFS= read -r -d '' staging_dir; do
    find "$staging_dir" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    log "cleared stale Chronon staging: $staging_dir"
  done < <(find /home/pierone/Pyt -xdev -type d -path '*/.tmp/chronon-builds' -mtime +14 -print0 2>/dev/null)
else
  log "left Chronon staging intact because a build process is active"
fi

apt-get clean >/dev/null 2>&1 || true
journalctl --vacuum-time=14d >/dev/null 2>&1 || true
df -h / | tail -n 1
