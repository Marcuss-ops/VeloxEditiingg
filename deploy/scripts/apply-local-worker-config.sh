#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# apply-local-worker-config.sh — operator helper for the co-located worker.
# ─────────────────────────────────────────────────────────────────────────────
# Renders deploy/runtime/worker_config.example.json to
# $VELOX_STATE_DIR/worker_config.json (uid 10001:10001 mode 0640), validates
# it, and writes a deployment-fingerprint so a subsequent `docker compose up -d`
# will NOT restart the worker when nothing material has changed.
#
# Bundle metadata precedence (per cmd/velox-worker-agent/main.go:162-176):
#
#   bundle_version: VELOX_BUNDLE_VERSION env > JSON > ldflags Version > VERSION.txt
#   bundle_hash:    VELOX_BUNDLE_HASH env > BUNDLE_HASH.txt (work_dir or /opt/velox) > empty
#   image_digest:   TELEMETRY ONLY — stamped by this script from `docker image
#                   inspect RepoDigests[0]`. Surfaced in master logs, never
#                   consumed by the worker binary for handshake or compat.
#
# By default the script leaves bundle_version + bundle_hash EMPTY in the
# rendered JSON so the worker binary fills them in from the env / work_dir
# files. The ONLY way to bake a literal bundle_hash into JSON is
# `--bundle-hash-source=manual --bundle-hash VALUE` together with
# `VELOX_FORCE_MANUAL_HASH=1` in the environment. This prevents typing
# arbitrary hashes that would later drift from the in-image build metadata.
#
# mTLS double-consent contract (transport_factory.go + grpcserver/handler.go):
#
#   worker plaintext = allow_insecure_grpc_dev:true  AND  VELOX_ALLOW_INSECURE_GRPC_DEV=true
#   master plaintext = VELOX_GRPC_ALLOW_INSECURE_DEV=true  (different env var name!)
#   Both sides MUST opt in independently; partial opt-in is rejected.
#
# Environment safety contract:
#   --environment=dev  →  --allow-insecure-grpc permitted freely (local dev only).
#   --environment=prod →  --allow-insecure-grpc requires --force-insecure-production
#                         AND `I_UNDERSTAND_INSECURE=1` in the env. Loud banner.
#
# Idempotency:
#   deployment-fingerprint = SHA256(JSON + compose.yml + image_digest).
#   Same fingerprint + same JSON hash on disk → no-op (no install, no
#   spurious restart-loop iteration on the worker container).
#
# Exit codes:
#   0   applied OR no-op
#   2   template missing
#   3   validation failure (json / compose / worker --validate-config)
#   4   insecure-refused (--environment vs flag mismatch, missing confirmation env)
#   5   manual bundle_hash refused (must use VELOX_FORCE_MANUAL_HASH=1)
#   6   atomic install / fingerprint write failure
#   64  usage / invalid arguments

set -euo pipefail

# ─── Constants ──────────────────────────────────────────────────────────────
SCRIPT_DIR="$(dirname "$(readlink -f -- "${BASH_SOURCE[0]}")")"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
readonly REPO_ROOT
readonly SRC_DEFAULT="${REPO_ROOT}/runtime/worker_config.example.json"
readonly COMPOSE_FILE_DEFAULT="${REPO_ROOT}/runtime/compose.yml"
DST=""
FINGERPRINT_FILE=""
readonly IMAGE_UID="10001"
readonly IMAGE_GID="10001"
OPENSSL="${OPENSSL:-openssl}"
BACKUP_DIR=""

# ─── Defaults ───────────────────────────────────────────────────────────────
WORKER_ID=""
WORKER_NAME=""
CONTROL_GRPC_URL=""
MASTER_URL=""
BUNDLE_VERSION=""
BUNDLE_VERSION_SOURCE="auto"      # auto | manual | skip
BUNDLE_HASH=""
BUNDLE_HASH_SOURCE="auto"         # auto | manual | env | skip
HEALTH_PORT="8081"
WORK_DIR=""
WORK_DIR_EXPLICIT=false
DST_EXPLICIT=false
PROTOCOL_VERSION="2026-06-worker-v1"
IMAGE="${VELOX_WORKER_IMAGE:-}"
ENVIRONMENT="dev"                 # dev | prod
ALLOW_INSECURE_GRPC=false
FORCE_INSECURE_PROD=false
SRC="${SRC_DEFAULT}"
COMPOSE_FILE="${COMPOSE_FILE_DEFAULT}"
ENV_FILE="/etc/velox-worker/worker.env"
SKIP_VALIDATE_CONFIG=false
SKIP_COMPOSE_CHECK=false
KEEP_TMP=false

# ─── Logging ─────────────────────────────────────────────────────────────────
log()  { printf '[apply] %s\n' "$*" >&2; }
warn() { printf '[apply][WARN] %s\n' "$*" >&2; }
die()  { printf '[apply][FAIL] %s\n' "$*" >&2; exit "${2:-1}"; }

RENDERER="$SCRIPT_DIR/render-worker-config.py"
FINGERPRINT_TOOL="$SCRIPT_DIR/worker-config-fingerprint.py"
[[ -r "$RENDERER" ]] || die "renderer component missing or unreadable: $RENDERER" 3
[[ -r "$FINGERPRINT_TOOL" ]] || die "fingerprint component missing or unreadable: $FINGERPRINT_TOOL" 3

# The canonical velox-worker.service.d directory is allowed. Only
# per-worker legacy directories (velox-worker-<id>.service.d) are forbidden.
assert_no_legacy_dropins() {
  local found=0 path entry
  shopt -s nullglob
  for path in \
    /etc/systemd/system/velox-worker-*.service.d \
    /run/systemd/system/velox-worker-*.service.d \
    /lib/systemd/system/velox-worker-*.service.d \
    /usr/lib/systemd/system/velox-worker-*.service.d; do
    [[ -d "$path" ]] || continue
    for entry in "$path"/*.conf; do
      [[ -e "$entry" || -L "$entry" ]] || continue
      warn "legacy systemd drop-in detected: $entry"
      found=1
    done
  done
  shopt -u nullglob
  (( found == 0 )) || die "velox-worker systemd drop-ins are forbidden; remove or migrate them before applying config" 1
}

# ─── Usage ───────────────────────────────────────────────────────────────────
usage() {
  cat <<USAGE
Usage: sudo $0 [options]

REQUIRED (no defaults):
  --worker-id              ID               Worker identity; must appear in
                                            the VELOX_ALLOWED_WORKERS list of
                                            /etc/velox-server.env (materialized
                                            by resolve-master-env.sh).
  --control-grpc-url       URL              REQUIRED. gRPC dial target. Accepts
                                            host:port (preferred, no scheme)
                                            OR http(s)://host:port (scheme is
                                            stripped + logged, since
                                            transport_factory & grpc.Dial expect
                                            host:port without scheme — otherwise
                                            grpc.Dial fails with "too many
                                            colons in address").
                                            transport_factory rejects empty.

OPTIONAL flags:
  --worker-name            NAME             Defaults from --worker-id.
  --master-url             URL              Defaults from --control-grpc-url.

  --bundle-version         STRING           Manual override. Default: leave
                                            empty so runtime fills from
                                            VELOX_BUNDLE_VERSION / ldflags
                                            / VERSION.txt.
  --bundle-version-source  auto|manual|skip Default auto (sanity check only).

  --bundle-hash            STRING           Manual override. NEVER for
                                            registration — use
                                            VELOX_BUNDLE_HASH env or
                                            BUNDLE_HASH.txt in work_dir.
  --bundle-hash-source     auto|manual|env|skip
                                            Default auto (auto=runtime fill
                                            via env / file; manual=literal
                                            write requires VELOX_FORCE_MANUAL_HASH=1).

  --health-port            PORT             Default 8081.
  --work-dir               PATH             Optional override below required
                                            VELOX_STATE_DIR; default is
                                            VELOX_STATE_DIR/work.
  --protocol-version       STRING           Default 2026-06-worker-v1.
  --image                  GHCR_DIGEST      Required immutable ghcr.io/...@sha256:<64hex>.

  --environment            dev|prod         Default dev. prod requires
                                            --force-insecure-production
                                            AND I_UNDERSTAND_INSECURE=1
                                            to pair with --allow-insecure-grpc.

  --allow-insecure-grpc                    Sets allow_insecure_grpc_dev:true
                                            in JSON AND warns that the
                                            worker's env file MUST contain
                                            VELOX_ALLOW_INSECURE_GRPC_DEV=true.
                                            Master uses DIFFERENT env name
                                            — see script header.

  --force-insecure-production              Confirms insecure gRPC in prod.
                                            Prints loud banners.

  --dst                    PATH             Optional path below
                                            VELOX_STATE_DIR; default is
                                            VELOX_STATE_DIR/worker_config.json.
  --src                    PATH             Default \${REPO_ROOT}/runtime/worker_config.example.json.
  --compose-file           PATH             Default \${REPO_ROOT}/runtime/compose.yml.
  --env-file               PATH             Default /etc/velox-worker/worker.env.

  --skip-validate-config                   Skip the docker run ... --validate-config
                                            semantic check (only JSON parse
                                            + compose check then).
  --skip-compose-check                     Skip docker compose config --quiet.
                                            Prints visible WARNING if Docker
                                            is missing — never silent PASS.
  --keep-tmp                               Do not delete staging TMP file.

USAGE
  exit 64
}

# ─── Arg parsing ─────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker-id)              WORKER_ID="$2";                shift 2 ;;
    --worker-name)            WORKER_NAME="$2";              shift 2 ;;
    --control-grpc-url)       CONTROL_GRPC_URL="$2";         shift 2 ;;
    --master-url)             MASTER_URL="$2";               shift 2 ;;
    --bundle-version)         BUNDLE_VERSION="$2";           shift 2 ;;
    --bundle-version-source)  BUNDLE_VERSION_SOURCE="$2";    shift 2 ;;
    --bundle-hash)            BUNDLE_HASH="$2";              shift 2 ;;
    --bundle-hash-source)     BUNDLE_HASH_SOURCE="$2";       shift 2 ;;
    --health-port)            HEALTH_PORT="$2";              shift 2 ;;
    --work-dir)               WORK_DIR="$2"; WORK_DIR_EXPLICIT=true; shift 2 ;;
    --protocol-version)       PROTOCOL_VERSION="$2";         shift 2 ;;
    --image)                  IMAGE="$2";                    shift 2 ;;
    --environment)            ENVIRONMENT="$2";              shift 2 ;;
    --dst)                    DST="$2"; DST_EXPLICIT=true;   shift 2 ;;
    --src)                    SRC="$2";                      shift 2 ;;
    --compose-file)           COMPOSE_FILE="$2";             shift 2 ;;
    --env-file)               ENV_FILE="$2";                 shift 2 ;;
    --allow-insecure-grpc)    ALLOW_INSECURE_GRPC=true;      shift ;;
    --force-insecure-production) FORCE_INSECURE_PROD=true;   shift ;;
    --skip-validate-config)   SKIP_VALIDATE_CONFIG=true;     shift ;;
    --skip-compose-check)     SKIP_COMPOSE_CHECK=true;       shift ;;
    --keep-tmp)               KEEP_TMP=true;                 shift ;;
    -h|--help)                usage ;;
    *)                        die "unknown argument: $1" 64 ;;
  esac
done

assert_no_legacy_dropins

# ─── Arg validation ──────────────────────────────────────────────────────────
[[ -n "$WORKER_ID" ]]            || die "--worker-id is required" 64
[[ -n "$CONTROL_GRPC_URL" ]]     || die "--control-grpc-url is required (transport_factory.go rejects empty)" 64

# VELOX_STATE_DIR is deliberately not defaulted here. It is the canonical
# mutable-state boundary and must be explicit in the process environment or
# the canonical worker.env; silently falling back would recreate split-brain
# state between apply, Compose, and the worker binary.
if [[ -z "${VELOX_STATE_DIR:-}" && -f "$ENV_FILE" ]]; then
  VELOX_STATE_DIR="$(awk -F= '$1 == "VELOX_STATE_DIR" {print substr($0, index($0, "=")+1); exit}' "$ENV_FILE" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")"
fi
: "${VELOX_STATE_DIR:?VELOX_STATE_DIR is required in the environment or $ENV_FILE}"
[[ "$VELOX_STATE_DIR" == /* ]] \
  || die "VELOX_STATE_DIR must be an absolute path (got: $VELOX_STATE_DIR)" 64
[[ "$VELOX_STATE_DIR" != *"/../"* && "$VELOX_STATE_DIR" != ../* && "$VELOX_STATE_DIR" != */.. ]] \
  || die "VELOX_STATE_DIR must not contain parent-directory traversal (got: $VELOX_STATE_DIR)" 64
[[ "$VELOX_STATE_DIR" != *$'\n'* && "$VELOX_STATE_DIR" != *$'\r'* ]] \
  || die "VELOX_STATE_DIR contains a newline" 64

[[ "$WORK_DIR_EXPLICIT" == true ]] || WORK_DIR="$VELOX_STATE_DIR/work"
[[ "$DST_EXPLICIT" == true ]] || DST="$VELOX_STATE_DIR/worker_config.json"
FINGERPRINT_FILE="$VELOX_STATE_DIR/deployment-fingerprint"
BACKUP_DIR="$VELOX_STATE_DIR/.backups"
[[ "$WORK_DIR" == /* ]] || die "--work-dir must be an absolute path (got: $WORK_DIR)" 64
STATE_DIR_REAL="$(realpath -m -- "$VELOX_STATE_DIR")"
[[ "$STATE_DIR_REAL" == "$VELOX_STATE_DIR" ]] \
  || die "VELOX_STATE_DIR must not be a symlink or contain traversal (got: $VELOX_STATE_DIR)" 64
WORK_DIR_REAL="$(realpath -m -- "$WORK_DIR")"
[[ "$WORK_DIR_REAL" == "$WORK_DIR" ]] \
  || die "--work-dir must not be a symlink or contain traversal (got: $WORK_DIR)" 64
[[ "$WORK_DIR_REAL" == "$STATE_DIR_REAL"/* ]] \
  || die "--work-dir must resolve below VELOX_STATE_DIR=$VELOX_STATE_DIR (got: $WORK_DIR)" 64
if [[ "$DST_EXPLICIT" == true ]]; then
  DST_REAL="$(realpath -m -- "$DST")"
  [[ "$DST_REAL" == "$STATE_DIR_REAL"/* ]] \
    || die "--dst must resolve below VELOX_STATE_DIR=$VELOX_STATE_DIR (got: $DST)" 64
fi
[[ "$WORKER_ID" != CHANGE_ME_* ]]    || die "--worker-id still set to placeholder CHANGE_ME_*. Pass a real worker_id." 64
[[ "$CONTROL_GRPC_URL" != CHANGE_ME_* ]] || die "--control-grpc-url still set to placeholder CHANGE_ME_*. Pass a real URL." 64
[[ "$ENVIRONMENT" =~ ^(dev|prod)$ ]] || die "--environment must be 'dev' or 'prod' (got: $ENVIRONMENT)" 64
[[ "$IMAGE" =~ ^ghcr\.io/[a-z0-9._-]+/[a-z0-9._/-]+@sha256:[a-f0-9]{64}$ ]] \
  || die "--image/VELOX_WORKER_IMAGE must be an immutable ghcr.io/...@sha256:<64hex> reference" 64
[[ "$BUNDLE_VERSION_SOURCE" =~ ^(auto|manual|skip)$ ]]    || die "--bundle-version-source must be auto|manual|skip" 64
[[ "$BUNDLE_HASH_SOURCE" =~ ^(auto|manual|env|skip)$ ]]   || die "--bundle-hash-source must be auto|manual|env|skip" 64
[[ "$HEALTH_PORT" =~ ^[0-9]+$ ]]  || die "--health-port must be a positive integer (got: $HEALTH_PORT)" 64

if [[ "$ALLOW_INSECURE_GRPC" == "true" && "$ENVIRONMENT" == "prod" ]]; then
  if [[ "$FORCE_INSECURE_PROD" != "true" ]]; then
    die "--allow-insecure-grpc with --environment=prod REQUIRES --force-insecure-production" 4
  fi
  if [[ "${I_UNDERSTAND_INSECURE:-}" != "1" ]]; then
    die "--force-insecure-production REQUIRES I_UNDERSTAND_INSECURE=1 in the environment" 4
  fi
  warn "════════════════════════════════════════════════════════════════════"
  warn "PRODUCTION INSECURE gRPC ENABLED — plaintext worker↔master traffic."
  warn "I_UNDERSTAND_INSECURE=1 was set; certs will be ignored if present."
  warn "════════════════════════════════════════════════════════════════════"
fi

if [[ "$BUNDLE_HASH_SOURCE" == "manual" ]]; then
  [[ -n "$BUNDLE_HASH" ]] || die "--bundle-hash-source=manual requires --bundle-hash VALUE" 64
  if [[ "${VELOX_FORCE_MANUAL_HASH:-}" != "1" ]]; then
    die "manual bundle_hash requires VELOX_FORCE_MANUAL_HASH=1 in env (else the worker will report a bundle mismatch)" 5
  fi
  warn "manual bundle_hash=$BUNDLE_HASH written into JSON — this WILL drift if the image is rebuilt."
fi

[[ -f "$SRC" ]]        || die "template $SRC not found (re-pull deploy/ tree or pass --src)" 2
[[ -f "$COMPOSE_FILE" ]] || warn "compose file $COMPOSE_FILE not found; compose check will be skipped and fingerprint will exclude the compose hash"

# Resolve defaults
[[ -n "$WORKER_NAME" ]]  || WORKER_NAME="$WORKER_ID"

# Normalize --control-grpc-url: transport_factory / grpc.Dial expect host:port
# (no scheme). If operator supplied http(s)://, strip and log loudly so the
# rewrite is visible (otherwise the worker silently fails gRPC dial with
# "too many colons in address" because grpc.Dial sees http://host:port as
# having two colons).
if [[ "$CONTROL_GRPC_URL" =~ ^https?:// ]]; then
  _stripped="${CONTROL_GRPC_URL#http://}"
  _stripped="${_stripped#https://}"
  _stripped="${_stripped%/}"
  log "stripped http(s):// from --control-grpc-url: $CONTROL_GRPC_URL → $_stripped"
  CONTROL_GRPC_URL="$_stripped"
  unset _stripped
fi
# Sanity post-normalization: must be host:port (no scheme, no path).
[[ "$CONTROL_GRPC_URL" =~ ^([A-Za-z0-9._-]+|\[[0-9a-fA-F:%.]+\]):[0-9]+$ ]] \
  || die "--control-grpc-url after normalization must be host:port or [IPv6]:port (got: $CONTROL_GRPC_URL)" 64

[[ -n "$MASTER_URL" ]]   || MASTER_URL="$CONTROL_GRPC_URL"

# ─── Image inspection (telemetry + sanity check) ─────────────────────────────
# image_digest is consumed only by the fingerprint + (optionally) the worker
# JSON as a telemetry-only field. We NEVER auto-write it as bundle_hash.
IMAGE_DIGEST=""
if command -v docker >/dev/null 2>&1 && docker image inspect "$IMAGE" >/dev/null 2>&1; then
  IMAGE_DIGEST="$(docker image inspect "$IMAGE" --format '{{index .RepoDigests 0}}' 2>/dev/null || true)"
  [[ -z "$IMAGE_DIGEST" ]] && IMAGE_DIGEST="$(docker image inspect "$IMAGE" --format '{{.Id}}' 2>/dev/null || true)"
  log "image_digest (telemetry): $IMAGE_DIGEST"
else
  warn "docker missing OR image $IMAGE not pulled; image_digest will be empty (fingerprint covers just JSON+compose)."
fi

# Bundle-version SANITY CHECK: if --bundle-version was explicit AND the image
# has a RepoTag, compare. Per reviewer prescription ("Non riscriverei
# automaticamente la configurazione") — WARN only, NEVER rewrite the operator's
# value. The runtime mismatch will surface in master logs so the operator is
# educated about it, rather than silently closed.
if [[ "$BUNDLE_VERSION_SOURCE" == "auto" && -n "$IMAGE_DIGEST" && "$BUNDLE_VERSION" != "dev" ]]; then
  tag="$(docker image inspect "$IMAGE" --format '{{index .RepoTags 1}}' 2>/dev/null || echo "")"
  if [[ -n "$tag" && -n "$BUNDLE_VERSION" && "$tag" != *"$BUNDLE_VERSION"* ]]; then
    warn "metadata mismatch: --bundle-version=$BUNDLE_VERSION but image tag=$tag — keeping operator-supplied value in JSON so the runtime mismatch surfaces in master logs for diagnosis. Per reviewer prescription: never auto-rewrite config fields."
  fi
fi

# ─── Bundle-hash auto-resolution ─────────────────────────────────────────────
case "$BUNDLE_HASH_SOURCE" in
  skip)
    BUNDLE_HASH=""
    ;;
  env)
    [[ -n "${VELOX_BUNDLE_HASH:-}" ]] && BUNDLE_HASH="${VELOX_BUNDLE_HASH}"
    [[ -n "$BUNDLE_HASH" ]] || warn "BUNDLE_HASH_SOURCE=env but VELOX_BUNDLE_HASH is unset — leaving empty."
    ;;
  auto)
    # Run the docker-image-sanity side, but never write into JSON.
    BUNDLE_HASH=""
    [[ -n "${VELOX_BUNDLE_HASH:-}" ]] && log "(ignored) runtime will use VELOX_BUNDLE_HASH env instead of the JSON field"
    if command -v docker >/dev/null 2>&1 && docker image inspect "$IMAGE" >/dev/null 2>&1; then
      img_digest_strip="${IMAGE_DIGEST#*@}"
      log "(sanity) image_digest shasum is $img_digest_strip — worker binary will not have access to it via JSON; ensure BUNDLE_HASH.txt exists in $WORK_DIR or set VELOX_BUNDLE_HASH in $ENV_FILE"
    else
      warn "BUNDLE_HASH_SOURCE=auto but image not inspectable — please ensure BUNDLE_HASH.txt is in $WORK_DIR on the worker host."
    fi
    ;;
  manual)
    # already validated above
    ;;
  *)
    die "internal: BUNDLE_HASH_SOURCE=$BUNDLE_HASH_SOURCE reached switch fallthrough" 64
    ;;
esac

# ─── Prepare directories ─────────────────────────────────────────────────────
# Persistent state tree — provision defensively so apply can succeed on a
# freshly-provisioned host, but NEVER change owner/mode on an existing path.
# This preserves operator ACLs and ownership across config-only convergence.
ensure_dir_preserving() {
  local path="$1" owner="$2" group="$3" mode="$4"
  if [[ -e "$path" ]]; then
    [[ -d "$path" ]] || die "$path exists but is not a directory"
    return 0
  fi
  mkdir -p "$path"
  chown "$owner:$group" "$path"
  chmod "$mode" "$path"
}

ensure_dir_preserving "$VELOX_STATE_DIR" "$IMAGE_UID" "$IMAGE_GID" 0750
ensure_dir_preserving "$(dirname "$DST")" "$IMAGE_UID" "$IMAGE_GID" 0750
ensure_dir_preserving "$BACKUP_DIR" root root 0750
# WorkDir siblings — only missing paths receive canonical ownership/mode.
for sub in "$WORK_DIR" "$WORK_DIR/state" "$WORK_DIR/cache" "$WORK_DIR/output"; do
  ensure_dir_preserving "$sub" "$IMAGE_UID" "$IMAGE_GID" 0750
done
# Existing state directory permissions are intentionally left untouched.
# Container traversal is part of the host's explicit state-directory contract,
# not something config rendering may silently broaden.

# ─── Stage TMP JSON ──────────────────────────────────────────────────────────
TMP="$(mktemp /tmp/apply-local-worker-config.XXXXXX.json)"
if [[ "$KEEP_TMP" != "true" ]]; then
  trap 'rm -f "$TMP"' EXIT
fi

python3 "$RENDERER" \
  "$SRC" "$TMP" \
  "$WORKER_ID" "$WORKER_NAME" \
  "$CONTROL_GRPC_URL" "$MASTER_URL" \
  "$WORK_DIR" "$HEALTH_PORT" \
  "$PROTOCOL_VERSION" \
  "$BUNDLE_VERSION" "$BUNDLE_HASH" \
  "$IMAGE_DIGEST" \
  "$ALLOW_INSECURE_GRPC"

# Preliminary structural check (the embedded Python should always produce
# valid JSON, but defend against edge cases).
python3 -c "import json,sys; json.load(open(sys.argv[1])); print('[apply] JSON parses OK', file=sys.stderr)" "$TMP"

# ─── Backup existing DST ─────────────────────────────────────────────────────
if [[ -f "$DST" ]]; then
  ts="$(date -u +%Y%m%dT%H%M%S)"
  if ! cp -a "$DST" "$BACKUP_DIR/worker_config.${ts}.json" 2>/dev/null; then
    warn "could not backup $DST to $BACKUP_DIR (continuing)"
  fi
  # Keep at most 10 backups; prune older.
  # shellcheck disable=SC2012
  ls -1tr "$BACKUP_DIR"/worker_config.*.json 2>/dev/null | head -n -10 | xargs -r rm -f --
fi

# ─── Compute deployment fingerprint ─────────────────────────────────────────
NEW_FINGERPRINT="$(python3 "$FINGERPRINT_TOOL" \
  "$TMP" "${COMPOSE_FILE:-}" "${IMAGE_DIGEST:-}")"
log "deployment_fingerprint: $NEW_FINGERPRINT (composed of: TMP + compose ${COMPOSE_FILE:-<none>} + image_digest)"

# ─── Idempotency check ───────────────────────────────────────────────────────
if [[ -f "$DST" && -f "$FINGERPRINT_FILE" ]]; then
  OLD_FINGERPRINT="$(cat "$FINGERPRINT_FILE" 2>/dev/null || true)"
  OLD_DST_HASH="$(sha256sum "$DST" | awk '{print $1}')"
  NEW_DST_HASH="$(sha256sum "$TMP" | awk '{print $1}')"
  if [[ "$OLD_FINGERPRINT" == "$NEW_FINGERPRINT" && "$OLD_DST_HASH" == "$NEW_DST_HASH" ]]; then
    log "no-op: deployment_fingerprint and JSON hash unchanged on disk."
    log "  worker_id=$WORKER_ID master=$CONTROL_GRPC_URL"
    log "  next: systemctl restart velox-worker.service (only if image or env var changed)"
    exit 0
  fi
  log "delta detected: fingerprint OR JSON changed — proceeding with install."
fi

# ─── Atomic install ──────────────────────────────────────────────────────────
# New files receive canonical metadata. Replacing an existing state file must
# preserve its owner/mode just like the surrounding state directory.
DST_UID="$IMAGE_UID"
DST_GID="$IMAGE_GID"
DST_MODE=0640
if [[ -f "$DST" ]]; then
  DST_UID="$(stat -c '%u' "$DST")"
  DST_GID="$(stat -c '%g' "$DST")"
  DST_MODE="$(stat -c '%a' "$DST")"
fi
chown "$IMAGE_UID:$IMAGE_GID" "$TMP" 2>/dev/null || true
chmod 0640 "$TMP"
if ! mv -f "$TMP" "$DST"; then
  die "atomic install of $DST failed" 6
fi
chown "$DST_UID:$DST_GID" "$DST"
chmod "$DST_MODE" "$DST"

FINGERPRINT_UID="$IMAGE_UID"
FINGERPRINT_GID="$IMAGE_GID"
FINGERPRINT_MODE=0644
if [[ -f "$FINGERPRINT_FILE" ]]; then
  FINGERPRINT_UID="$(stat -c '%u' "$FINGERPRINT_FILE")"
  FINGERPRINT_GID="$(stat -c '%g' "$FINGERPRINT_FILE")"
  FINGERPRINT_MODE="$(stat -c '%a' "$FINGERPRINT_FILE")"
fi
printf '%s\n' "$NEW_FINGERPRINT" > "$FINGERPRINT_FILE"
chown "$FINGERPRINT_UID:$FINGERPRINT_GID" "$FINGERPRINT_FILE"
chmod "$FINGERPRINT_MODE" "$FINGERPRINT_FILE"

# ─── Compose schema check ────────────────────────────────────────────────────
if [[ "$SKIP_COMPOSE_CHECK" == "false" ]]; then
  if [[ ! -f "$COMPOSE_FILE" ]]; then
    warn "compose file $COMPOSE_FILE absent; compose schema check SKIPPED (NOT a pass)."
  elif command -v docker >/dev/null 2>&1; then
    log "compose schema validation: docker compose --env-file $ENV_FILE -f $COMPOSE_FILE config --quiet"
    if ! docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" config --quiet >/dev/null 2>&1; then
      die "compose schema validation failed (run 'docker compose config' for details)" 3
    fi
    log "compose schema validation: PASS"
  else
    warn "Docker not present on this host. compose schema check SKIPPED — this is NOT a pass."
  fi
else
  warn "--skip-compose-check set; compose schema validation NOT performed."
fi

# ─── Optional semantic validation via worker binary ──────────────────────────
# cmd/velox-worker-agent/main.go does not yet expose a --validate-config flag;
# when invoked with an unknown flag, Go's flag package exits with rc=2. We
# treat rc=2 as "flag not implemented yet — skip without failing", and rc=1
# as a real validation failure.
if [[ "$SKIP_VALIDATE_CONFIG" == "false" ]]; then
  if [[ ! -f "$DST" ]]; then
    warn "DST $DST not on disk; semantic validation skipped."
  elif ! command -v docker >/dev/null 2>&1; then
    warn "docker missing; semantic validation SKIPPED — this is NOT a pass."
  elif ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    warn "image $IMAGE not pulled on this host; semantic validation SKIPPED."
  else
    log "worker semantic validation: docker run --rm -v $DST:/config/wc.json:ro $IMAGE --config /config/wc.json --validate-config"
    VAL_STDERR="$(mktemp)"
    set +e
    docker run --rm \
      -v "$DST:/config/worker_config.json:ro" \
      "$IMAGE" \
      --config /config/worker_config.json \
      --validate-config >/dev/null 2>"$VAL_STDERR"
    rc=$?
    set -e
    val_first_line="$(head -1 "$VAL_STDERR" 2>/dev/null | tr -d '\n' || true)"
    rm -f "$VAL_STDERR"
    case "$rc" in
      0) log "worker semantic validation: PASS" ;;
      1) warn "worker --validate-config FAILED rc=1 — first line of stderr: ${val_first_line:-<empty>}"; die "required field missing or TLS triple partial in JSON" 3 ;;
      2) warn "worker binary does not yet support --validate-config (rc=2 = Go's 'flag not defined'). Add to cmd/velox-worker-agent/main.go."
         [[ -n "$val_first_line" ]] && warn "stderr first line: $val_first_line" ;;
      *) die "worker --validate-config returned unexpected rc=$rc" 3 ;;
    esac
  fi
else
  warn "--skip-validate-config set; semantic validation NOT performed."
fi

# ─── Summary ─────────────────────────────────────────────────────────────────
log "applied: $DST"
log "  worker_id         : $WORKER_ID"
log "  worker_name       : $WORKER_NAME"
log "  control_grpc_url  : $CONTROL_GRPC_URL"
log "  master_url        : $MASTER_URL"
log "  work_dir          : $WORK_DIR"
log "  health_port       : $HEALTH_PORT"
log "  environment       : $ENVIRONMENT"
log "  allow_insecure_grpc_dev (JSON): $ALLOW_INSECURE_GRPC"
log "  bundle_version (JSON): ${BUNDLE_VERSION:-<empty: runtime fills from VELOX_BUNDLE_VERSION / ldflags / VERSION.txt>}"
log "  bundle_hash    (JSON): ${BUNDLE_HASH:-<empty: runtime fills from VELOX_BUNDLE_HASH / BUNDLE_HASH.txt>}"
log "  image_digest (stamp): ${IMAGE_DIGEST:-<not stamped: docker missing or image not pulled>}"
log "  owner             : $(stat -c '%U:%G' "$DST")"
log "  fingerprint       : $NEW_FINGERPRINT (saved at $FINGERPRINT_FILE)"

# Insecure double-consent reminder (always printed when the flag is set).
if [[ "$ALLOW_INSECURE_GRPC" == "true" ]]; then
  cat <<NEXT

[mTLS DOUBLE-CONSENT REMINDER]
  worker side: ensure $ENV_FILE has VELOX_ALLOW_INSECURE_GRPC_DEV=true
  master side: ensure /etc/velox-server.env has VELOX_GRPC_ALLOW_INSECURE_DEV=true
                (NB: the variable names are different — don't conflate them)
NEXT
fi

cat <<NEXT

[NEXT STEPS]
  1. If you need the worker to REPORT a real bundle_hash:
        echo "$IMAGE_DIGEST" | sed 's|.*@sha256:||' > $WORK_DIR/BUNDLE_HASH.txt
     or set VELOX_BUNDLE_HASH in $ENV_FILE.
  2. Restart worker:
        systemctl restart velox-worker.service
  3. Tail master log for the registration marker:
        journalctl -u velox-server --since '30s ago' 2>/dev/null | grep -E '$WORKER_ID|hello_ack|session'
        (or:   sudo journalctl -u velox-server -f   to follow live)
NEXT

# ─── Cert-metadata recording (RW-PROD-001 §3 A8) ────────────────────────────
# After the JSON is installed we walk the dst and extract the worker`s
# tls_cert_file, then compute SHA-256 + serial via openssl and stash
# the pair in $WORK_DIR/worker_cert.meta so operators can answer
# "which cert is currently deployed on this host?" without re-reading
# the cert. Failure modes are NOT fatal — the meta is purely an
# audit-trail convenience; the worker`s actual auth flow uses the
# live cert/key on disk.
record_cert_metadata() {
  local dst="$1" work_dir="$2"
  [[ -f "$dst" ]] || { warn "A8: $dst absent, skipping worker_cert.meta stamp"; return 0; }
  command -v "$OPENSSL" >/dev/null 2>&1 || { warn "A8: openssl missing, skipping worker_cert.meta stamp"; return 0; }
  local cert_path
  cert_path="$(python3 -c "
import json, sys
try:
    with open(sys.argv[1]) as f:
        c = json.load(f)
    print(c.get('tls_cert_file', '') or '')
except Exception:
    print('', end='')
" "$dst")"
  [[ -z "$cert_path" ]] && { warn "A8: dst $dst has no tls_cert_file, skipping"; return 0; }
  [[ ! -f "$cert_path" ]] && { warn "A8: tls_cert_file $cert_path not on disk, skipping"; return 0; }
  local fp serial
  fp="$("$OPENSSL" x509 -in "$cert_path" -noout -fingerprint -sha256 2>/dev/null | cut -d'=' -f2 | tr -d ' ' || true)"
  serial="$("$OPENSSL" x509 -in "$cert_path" -noout -serial 2>/dev/null | cut -d'=' -f2 | tr -d ' ' || true)"
  [[ -z "$fp" || -z "$serial" ]] && { warn "A8: could not extract fingerprint/serial from $cert_path (corrupt?)"; return 0; }
  local meta="$work_dir/worker_cert.meta"
  ensure_dir_preserving "$(dirname "$meta")" "$IMAGE_UID" "$IMAGE_GID" 0750
  local now
  now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  local tmp
  tmp="$(mktemp "$(dirname "$meta")/.worker_cert.meta.XXXXXX")"
  printf 'LAST_CERT_HASH=%s\nLAST_CERT_SERIAL=%s\nLAST_KNOWN_AT=%s\nCERT_PATH=%s\n' \
    "$fp" "$serial" "$now" "$cert_path" > "$tmp"
  mv -f "$tmp" "$meta"
  chmod 0640 "$meta" 2>/dev/null || true
  log "A8: recorded cert metadata to $meta (fingerprint=$fp serial=$serial)"
}
record_cert_metadata "$DST" "$WORK_DIR"
