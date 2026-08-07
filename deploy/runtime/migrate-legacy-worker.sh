#!/usr/bin/env bash
# Velox Worker — legacy runtime migration.
#
# This script is deliberately separate from the steady-state converger. It
# translates the old per-inventory runtime once, records what it found, and
# then makes every legacy worker unit unable to start again. A completion
# marker makes the boundary explicit: after migration, normal deploys skip
# legacy discovery and cleanup rather than re-normalizing the host forever.
# It never deletes the persistent worker data tree; the old unit files are
# copied to the migration evidence directory before they are retired.

# The marker is intentionally stored in the canonical state tree. It is both
# the durable migration contract and the audit trail for operators.
#
# Canonical steady-state convergence must not call this script as a cleanup
# mechanism after the marker exists.
#
# Canonical contract after a successful run:
#   unit:      /etc/systemd/system/velox-worker.service
#   project:   velox-worker
#   container: velox-worker
#   env:       /etc/velox-worker/worker.env
#   state:     /var/lib/velox-worker
#
# Usage (root):
#   migrate-legacy-worker.sh
#   WORKER_ID=worker-01 LEGACY_RUNTIME_DIR=/var/lib/velox/workers/worker-01 \
#     migrate-legacy-worker.sh

set -euo pipefail

CANONICAL_ROOT="${CANONICAL_ROOT:-/opt/velox-worker}"
CANONICAL_ETC_ROOT="${CANONICAL_ETC_ROOT:-/etc/velox-worker}"
CANONICAL_ENV="${CANONICAL_ENV:-$CANONICAL_ETC_ROOT/worker.env}"
if [[ -z "${VELOX_STATE_DIR:-}" && -r "$CANONICAL_ENV" ]]; then
  VELOX_STATE_DIR="$(awk -F= '$1 == "VELOX_STATE_DIR" {print substr($0, index($0, "=")+1); exit}' "$CANONICAL_ENV" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")"
fi
CANONICAL_STATE="${VELOX_STATE_DIR:-}"
LEGACY_ETC_ROOT="${LEGACY_ETC_ROOT:-/etc}"
SYSTEMD_SYSTEM_DIR="${SYSTEMD_SYSTEM_DIR:-/etc/systemd/system}"
SYSTEMD_LIB_DIR="${SYSTEMD_LIB_DIR:-/lib/systemd/system}"
SYSTEMD_USR_LIB_DIR="${SYSTEMD_USR_LIB_DIR:-/usr/lib/systemd/system}"

log() { printf '[migrate] %s\n' "$*"; }
warn() { printf '[migrate][WARN] %s\n' "$*" >&2; }
fail() { printf '[migrate][FAIL] %s\n' "$*" >&2; exit 1; }

: "${CANONICAL_STATE:?VELOX_STATE_DIR is required}"
[[ "$CANONICAL_STATE" == /* ]] || fail "VELOX_STATE_DIR must be an absolute path"
CANONICAL_STATE_REAL="$(realpath -m -- "$CANONICAL_STATE")"
[[ "$CANONICAL_STATE_REAL" == "$CANONICAL_STATE" ]] \
  || fail "VELOX_STATE_DIR must be normalized and must not contain traversal"
MIGRATION_DIR="$CANONICAL_STATE/migration"
WORKER_ID_ARG="${WORKER_ID:-}"
LEGACY_RUNTIME_DIR_ARG="${LEGACY_RUNTIME_DIR:-}"

[[ $EUID -eq 0 ]] || fail 'must run as root'

ensure_dir_preserving() {
  local path="$1" mode="$2"
  if [[ -e "$path" ]]; then
    [[ -d "$path" ]] || fail "$path exists but is not a directory"
    return 0
  fi
  mkdir -p "$path"
  chmod "$mode" "$path"
}

# Do not reset mode or ownership on an existing state tree. Migration may
# create missing paths, but state/work ownership belongs to the operator.
ensure_dir_preserving "$CANONICAL_ROOT" 0750
ensure_dir_preserving "$CANONICAL_STATE" 0750
ensure_dir_preserving "$MIGRATION_DIR" 0750

if [[ -f "$MIGRATION_DIR/completed" ]]; then
  [[ -f "$CANONICAL_ENV" ]] || fail "migration marker exists but canonical env is missing: $CANONICAL_ENV"
  log "legacy migration already completed; skipping legacy discovery and cleanup"
  exit 0
fi

# Resolve the stable logical identity from the canonical env first, then from
# the old env conventions. Hostnames and inventory aliases are only migration
# hints; they are not rewritten into the logical identity once it is known.
worker_id="$WORKER_ID_ARG"
legacy_env=""
if [[ -z "$worker_id" && -r "$CANONICAL_ENV" ]]; then
  worker_id="$(awk -F= '$1 == "VELOX_WORKER_ID" {print substr($0, index($0, "=")+1); exit}' "$CANONICAL_ENV" | tr -d '\r' || true)"
fi
if [[ -z "$worker_id" ]]; then
  candidates=("$LEGACY_ETC_ROOT/velox-worker.env" "$LEGACY_ETC_ROOT"/velox-worker-*.env)
  for candidate in "${candidates[@]}"; do
    [[ -f "$candidate" ]] || continue
    candidate_id="$(awk -F= '$1 == "VELOX_WORKER_ID" || $1 == "WORKER_ID" {print substr($0, index($0, "=")+1); exit}' "$candidate" | tr -d '\r' || true)"
    if [[ -n "$candidate_id" ]]; then
      worker_id="$candidate_id"
      legacy_env="$candidate"
      break
    fi
  done
fi

# If an old env has no explicit ID, derive only the migration source name.
# The resulting canonical env still requires an explicit VELOX_WORKER_ID.
if [[ -z "$legacy_env" ]]; then
  for candidate in "$LEGACY_ETC_ROOT/velox-worker.env" "$LEGACY_ETC_ROOT"/velox-worker-*.env; do
    [[ -f "$candidate" ]] || continue
    [[ "$candidate" == "$CANONICAL_ENV" ]] && continue
    legacy_env="$candidate"
    break
  done
fi

if [[ ! -f "$CANONICAL_ENV" && -n "$legacy_env" ]]; then
  log "Migrating environment $legacy_env → $CANONICAL_ENV"
  install -D -o root -g root -m 0600 "$legacy_env" "$CANONICAL_ENV"
fi

# A canonical env file may already exist from a partial provisioning run but
# still lack the worker credential. Preserve the legacy credential exactly
# once, before the normalizer drops all legacy env discovery. Never overwrite
# a non-empty canonical credential.
if ! grep -Eq '^VELOX_WORKER_SECRET=[^[:space:]]' "$CANONICAL_ENV" 2>/dev/null; then
  for candidate in "$LEGACY_ETC_ROOT/velox-worker.env" "$LEGACY_ETC_ROOT"/velox-worker-*.env; do
    [[ -f "$candidate" && "$candidate" != "$CANONICAL_ENV" ]] || continue
    legacy_secret="$(grep -m1 -E '^VELOX_WORKER_SECRET=[^[:space:]]' "$candidate" || true)"
    [[ -n "$legacy_secret" ]] || continue
    printf '%s\n' "$legacy_secret" >> "$CANONICAL_ENV"
    break
  done
fi

if [[ -z "$worker_id" && -r "$CANONICAL_ENV" ]]; then
  worker_id="$(awk -F= '$1 == "VELOX_WORKER_ID" {print substr($0, index($0, "=")+1); exit}' "$CANONICAL_ENV" | tr -d '\r' || true)"
fi
if [[ -z "$worker_id" ]]; then
  if [[ -n "$legacy_env" ]]; then
    fail "legacy env $legacy_env has no VELOX_WORKER_ID/WORKER_ID; refusing an identity-ambiguous migration"
  fi
  warn "no canonical or legacy worker environment found; nothing to migrate"
  exit 0
fi

# Make the canonical env self-contained and point runtime writes at the one
# persistent tree. Existing operator values are preserved verbatim. A fresh
# Ansible-managed host may provide WORKER_ID before any env file exists, so
# materialise an empty canonical file before the rewrite instead of letting
# awk fail on a missing path.
if [[ ! -f "$CANONICAL_ENV" ]]; then
  install -D -o root -g root -m 0600 /dev/null "$CANONICAL_ENV"
fi
tmp_env="$(mktemp "$CANONICAL_ENV.XXXXXX")"
awk -v id="$worker_id" -v state="$CANONICAL_STATE" '
  BEGIN { seen_id=0; seen_state=0; seen_work=0 }
  /^VELOX_WORKER_ID=/ { print "VELOX_WORKER_ID=" id; seen_id=1; next }
  /^WORKER_ID=/ { next }
  /^VELOX_STATE_DIR=/ { print "VELOX_STATE_DIR=" state; seen_state=1; next }
  /^VELOX_WORK_DIR=/ { print "VELOX_WORK_DIR=" state "/work"; seen_work=1; next }
  { print }
  END {
    if (!seen_id) print "VELOX_WORKER_ID=" id
    if (!seen_state) print "VELOX_STATE_DIR=" state
    if (!seen_work) print "VELOX_WORK_DIR=" state "/work"
  }
' "$CANONICAL_ENV" > "$tmp_env"
install -o root -g root -m 0600 "$tmp_env" "$CANONICAL_ENV"
rm -f "$tmp_env"

# Choose the old writable tree explicitly when supplied; otherwise inspect
# the known per-inventory and pre-canonical locations. Never copy the
# canonical tree into itself.
legacy_runtime="$LEGACY_RUNTIME_DIR_ARG"
if [[ -z "$legacy_runtime" ]]; then
  for candidate in \
    "/var/lib/velox/workers/$worker_id" \
    "/var/lib/velox/workers/${legacy_env##*/}" \
    "/var/lib/velox/worker"; do
    [[ -d "$candidate" && "$candidate" != "$CANONICAL_STATE" ]] || continue
    legacy_runtime="$candidate"
    break
  done
fi
copy_missing_tree() {
  local source="$1" target="$2" entry relative destination
  while IFS= read -r -d '' entry; do
    relative="${entry#"$source"/}"
    destination="$target/$relative"
    if [[ -e "$destination" || -L "$destination" ]]; then
      continue
    fi
    if [[ -L "$entry" ]]; then
      fail "refusing to migrate symlink from legacy state: $entry"
    elif [[ -d "$entry" ]]; then
      mkdir -p "$destination"
      chown 10001:10001 "$destination"
      chmod 0750 "$destination"
    elif [[ -f "$entry" ]]; then
      mkdir -p "$(dirname "$destination")"
      # Do not import legacy owner/mode into the canonical state tree. The
      # contents and timestamps are useful evidence, but newly created files
      # receive the worker's canonical metadata; existing destinations were
      # skipped above and retain their operator-owned metadata.
      cp --preserve=timestamps "$entry" "$destination"
      chown 10001:10001 "$destination"
      chmod 0640 "$destination"
    else
      fail "unsupported legacy state entry: $entry"
    fi
  done < <(find "$source" -mindepth 1 -print0)
}

if [[ -n "$legacy_runtime" && -d "$legacy_runtime" && "$legacy_runtime" != "$CANONICAL_STATE" ]]; then
  log "Migrating missing persistent state entries $legacy_runtime → $CANONICAL_STATE"
  copy_missing_tree "$legacy_runtime" "$CANONICAL_STATE"
fi

# Migrate credentials/certs when old deployments kept them outside the
# canonical /etc/velox-worker tree. Existing canonical files always win.
mkdir -p "$CANONICAL_ETC_ROOT/certs" "$CANONICAL_ETC_ROOT/secrets"
for source_dir in \
  "$LEGACY_ETC_ROOT/velox/certs" \
  "$CANONICAL_ETC_ROOT/certs-legacy" \
  "$LEGACY_ETC_ROOT/velox-worker-${worker_id}/certs"; do
  [[ -d "$source_dir" ]] || continue
  cp -an "$source_dir"/. "$CANONICAL_ETC_ROOT/certs/" 2>/dev/null || true
done
for source_dir in \
  "$LEGACY_ETC_ROOT/velox/secrets" \
  "$LEGACY_ETC_ROOT/velox-worker-${worker_id}/secrets"; do
  [[ -d "$source_dir" ]] || continue
  cp -an "$source_dir"/. "$CANONICAL_ETC_ROOT/secrets/" 2>/dev/null || true
done
# Keep existing credential/cert directory modes intact; only the mkdir above
# establishes missing paths. prepare-host applies file-level secret policy.

# Retire old systemd units. The generic name is reserved for the new
# canonical unit, so preserve its old file as evidence and remove it rather
# than masking the path that the canonical installer must occupy.
backup_units="$MIGRATION_DIR/units"
mkdir -p "$backup_units"
for unit_path in "$SYSTEMD_SYSTEM_DIR"/velox-worker*.service "$SYSTEMD_LIB_DIR"/velox-worker*.service "$SYSTEMD_USR_LIB_DIR"/velox-worker*.service; do
  [[ -e "$unit_path" || -L "$unit_path" ]] || continue
  unit="${unit_path##*/}"
  [[ "$unit" == 'velox-worker-watchdog.service' ]] && continue
  if [[ "$unit" == 'velox-worker.service' ]] && grep -q 'canonical Compose runtime' "$unit_path" 2>/dev/null; then
    continue
  fi
  log "Retiring legacy systemd unit $unit"
  systemctl stop "$unit" >/dev/null 2>&1 || true
  systemctl disable "$unit" >/dev/null 2>&1 || true
  if [[ -f "$unit_path" ]]; then
    cp -p "$unit_path" "$backup_units/$unit.$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null || true
  fi
  rm -f "$unit_path"
  if [[ "$unit" != 'velox-worker.service' ]]; then
    systemctl mask --force "$unit" >/dev/null 2>&1 || true
  fi
done

# Remove only containers that are not owned by the canonical Compose project.
# This avoids disrupting an already-converged host on an idempotent rerun.
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  while read -r container; do
    [[ -n "$container" ]] || continue
    [[ "$container" == velox-worker ]] && {
      project="$(docker inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' "$container" 2>/dev/null || true)"
      [[ "$project" == 'velox-worker' ]] && continue
    }
    case "$container" in
      velox-worker|velox-worker-*)
        log "Removing legacy container $container"
        docker rm -f "$container" >/dev/null 2>&1 || true
        ;;
    esac
  done < <(docker ps -a --format '{{.Names}}' 2>/dev/null)
fi

cat > "$MIGRATION_DIR/manifest.json" <<EOF
{
  "schema": 1,
  "worker_id": "$worker_id",
  "legacy_env": "${legacy_env:-}",
  "legacy_runtime": "${legacy_runtime:-}",
  "canonical_unit": "velox-worker.service",
  "canonical_project": "velox-worker",
  "canonical_container": "velox-worker",
  "canonical_env": "$CANONICAL_ENV",
  "canonical_state": "$CANONICAL_STATE",
  "completed_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
chmod 0640 "$MIGRATION_DIR/manifest.json"
touch "$MIGRATION_DIR/completed"

systemctl daemon-reload >/dev/null 2>&1 || true
log "Migration complete for worker $worker_id"
