#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# apply-local-worker-config.sh — operator helper for the co-located worker.
# ─────────────────────────────────────────────────────────────────────────────
# Renders deploy/runtime/worker_config.example.json to
# /var/lib/velox-worker/worker_config.json (uid 1000:1000 mode 0640), validates
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
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SRC_DEFAULT="${REPO_ROOT}/runtime/worker_config.example.json"
readonly COMPOSE_FILE_DEFAULT="${REPO_ROOT}/runtime/compose.yml"
readonly DST="/var/lib/velox-worker/worker_config.json"
readonly FINGERPRINT_FILE="/var/lib/velox-worker/deployment-fingerprint"
readonly BACKUP_DIR="/var/lib/velox-worker/.backups"

# ─── Defaults (mutable) ─────────────────────────────────────────────────────
WORKER_ID=""
WORKER_NAME=""
CONTROL_GRPC_URL=""
MASTER_URL=""
BUNDLE_VERSION=""
BUNDLE_VERSION_SOURCE="auto"      # auto | manual | skip
BUNDLE_HASH=""
BUNDLE_HASH_SOURCE="auto"         # auto | manual | env | skip
HEALTH_PORT="8081"
WORK_DIR="/var/lib/velox-worker/work"
PROTOCOL_VERSION="2026-06-worker-v1"
IMAGE="${VELOX_WORKER_IMAGE:-velox-worker:latest}"
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

# ─── Usage ───────────────────────────────────────────────────────────────────
usage() {
  cat <<USAGE
Usage: sudo $0 [options]

REQUIRED (no defaults):
  --worker-id              ID               Worker identity; must appear in
                                            deploy/group_vars/all.yml
                                            velox_allowed_workers.
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
  --work-dir               PATH             Default /var/lib/velox-worker/work.
  --protocol-version       STRING           Default 2026-06-worker-v1.
  --image                  NAME_OR_DIGEST   Default velox-worker:latest.

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

  --dst                    PATH             Default $DST.
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
    --work-dir)               WORK_DIR="$2";                 shift 2 ;;
    --protocol-version)       PROTOCOL_VERSION="$2";         shift 2 ;;
    --image)                  IMAGE="$2";                    shift 2 ;;
    --environment)            ENVIRONMENT="$2";              shift 2 ;;
    --dst)                    DST="$2";                      shift 2 ;;
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

# ─── Arg validation ──────────────────────────────────────────────────────────
[[ -n "$WORKER_ID" ]]            || die "--worker-id is required" 64
[[ -n "$CONTROL_GRPC_URL" ]]     || die "--control-grpc-url is required (transport_factory.go rejects empty)" 64
[[ "$WORKER_ID" != CHANGE_ME_* ]]    || die "--worker-id still set to placeholder CHANGE_ME_*. Pass a real worker_id." 64
[[ "$CONTROL_GRPC_URL" != CHANGE_ME_* ]] || die "--control-grpc-url still set to placeholder CHANGE_ME_*. Pass a real URL." 64
[[ "$ENVIRONMENT" =~ ^(dev|prod)$ ]] || die "--environment must be 'dev' or 'prod' (got: $ENVIRONMENT)" 64
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
# Persistent state tree — provisioned defensively so apply can succeed on
# freshly-provisioned hosts before prepare-host.sh has run.
install -d -o root -g root -m 0755 "$(dirname "$DST")"   # /var/lib/velox-worker
install -d -o root -g root -m 0750 "$BACKUP_DIR"
# WorkDir siblings — try chowning to uid 1000 (velox in container). Fall
# back to root:root if no user with that uid exists on the host yet.
for sub in "$WORK_DIR" "$WORK_DIR/state" "$WORK_DIR/cache" "$WORK_DIR/output"; do
  if ! install -d -o 1000 -g 1000 -m 0750 "$sub" 2>/dev/null; then
    install -d -o root -g root -m 0750 "$sub"
    warn "$sub not chownable to uid 1000 on this host (no such user yet) — container writes may fail until prepare-host.sh provisions velox user."
  fi
done
# Traversal for the docker group so operators without root can `docker exec`.
if getent group docker >/dev/null; then
  chmod o+x "$(dirname "$DST")" 2>/dev/null || true
fi

# ─── Stage TMP JSON ──────────────────────────────────────────────────────────
TMP="$(mktemp /tmp/apply-local-worker-config.XXXXXX.json)"
if [[ "$KEEP_TMP" != "true" ]]; then
  trap 'rm -f "$TMP"' EXIT
fi

python3 - "$SRC" "$TMP" \
  "$WORKER_ID" "$WORKER_NAME" \
  "$CONTROL_GRPC_URL" "$MASTER_URL" \
  "$WORK_DIR" "$HEALTH_PORT" \
  "$PROTOCOL_VERSION" \
  "$BUNDLE_VERSION" "$BUNDLE_HASH" \
  "$IMAGE_DIGEST" \
  "$ALLOW_INSECURE_GRPC" \
  <<'PY'
import json, sys
src, dst = sys.argv[1], sys.argv[2]
worker_id, worker_name = sys.argv[3], sys.argv[4]
control_grpc_url, master_url = sys.argv[5], sys.argv[6]
work_dir, health_port = sys.argv[7], int(sys.argv[8])
protocol_version = sys.argv[9]
bundle_version, bundle_hash = sys.argv[10], sys.argv[11]
image_digest = sys.argv[12]
allow_insecure = sys.argv[13].lower() == "true"

with open(src) as f:
    cfg = json.load(f)

# Strip operator-side documentation keys (prefix _) so runtime JSON is clean.
cfg = {k: v for k, v in cfg.items() if not k.startswith("_")}

# Operator-supplied fields — ALWAYS overwrite from flags.
cfg["worker_id"]           = worker_id
cfg["worker_name"]         = worker_name
cfg["control_grpc_url"]    = control_grpc_url
cfg["master_url"]          = master_url
cfg["work_dir"]            = work_dir
cfg["health_port"]         = health_port
cfg["protocol_version"]    = protocol_version
cfg.setdefault("log_level", "info")

# Optional overrides — only write when explicitly non-empty / non-false.
if bundle_version:
    cfg["bundle_version"] = bundle_version
elif "bundle_version" in cfg and cfg["bundle_version"] == "":
    pass  # keep empty; runtime fills from env/ldflags/VERSION.txt
if bundle_hash:
    cfg["bundle_hash"] = bundle_hash
elif "bundle_hash" in cfg and cfg["bundle_hash"] == "":
    pass  # keep empty; runtime fills from VELOX_BUNDLE_HASH / BUNDLE_HASH.txt

if image_digest:
    cfg["image_digest"] = image_digest

if allow_insecure:
    cfg["allow_insecure_grpc_dev"] = True
else:
    cfg["allow_insecure_grpc_dev"] = False

# Schema sanity defaults. NOTE: HTTP-polling-era keys
# (command_poll_interval_secs, use_v2_endpoints) were dropped in PR3 final;
# the worker is gRPC-push only.
cfg.setdefault("max_active_jobs", 1)
cfg.setdefault("prometheus_port", 0)

with open(dst, "w") as f:
    json.dump(cfg, f, indent=2, sort_keys=False)
PY

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
  ls -1tr "$BACKUP_DIR"/worker_config.*.json 2>/dev/null | head -n -10 | xargs -r rm -f --
fi

# ─── Compute deployment fingerprint ─────────────────────────────────────────
COMPOSE_HASH=""
if [[ -f "$COMPOSE_FILE" ]]; then
  COMPOSE_HASH="$(sha256sum "$COMPOSE_FILE" | awk '{print $1}')"
fi
NEW_FINGERPRINT="$(python3 -c '
import hashlib, sys
data = open(sys.argv[1], "rb").read()
# compose.yml: optional; missing on host already produced a warning above.
if len(sys.argv) > 2 and sys.argv[2]:
    try:
        data += open(sys.argv[2], "rb").read()
    except FileNotFoundError:
        pass
# image_digest: optional string (may be empty if docker/image absent).
if len(sys.argv) > 3 and sys.argv[3]:
    data += sys.argv[3].encode()
print(hashlib.sha256(data).hexdigest())
' "$TMP" "${COMPOSE_FILE:-}" "${IMAGE_DIGEST:-}")"
log "deployment_fingerprint: $NEW_FINGERPRINT (composed of: TMP + compose ${COMPOSE_FILE:-<none>} + image_digest)"

# ─── Idempotency check ───────────────────────────────────────────────────────
if [[ -f "$DST" && -f "$FINGERPRINT_FILE" ]]; then
  OLD_FINGERPRINT="$(cat "$FINGERPRINT_FILE" 2>/dev/null || true)"
  OLD_DST_HASH="$(sha256sum "$DST" | awk '{print $1}')"
  NEW_DST_HASH="$(sha256sum "$TMP" | awk '{print $1}')"
  if [[ "$OLD_FINGERPRINT" == "$NEW_FINGERPRINT" && "$OLD_DST_HASH" == "$NEW_DST_HASH" ]]; then
    log "no-op: deployment_fingerprint and JSON hash unchanged on disk."
    log "  worker_id=$WORKER_ID master=$CONTROL_GRPC_URL"
    log "  next: docker compose -p velox-worker-$WORKER_ID -f $COMPOSE_FILE up -d --force-recreate (only if image or env var changed)"
    exit 0
  fi
  log "delta detected: fingerprint OR JSON changed — proceeding with install."
fi

# ─── Atomic install ──────────────────────────────────────────────────────────
chown 1000:1000 "$TMP" 2>/dev/null || true
chmod 0640 "$TMP"
if ! mv -f "$TMP" "$DST"; then
  die "atomic install of $DST failed" 6
fi
chown 1000:1000 "$DST"
chmod 0640 "$DST"
printf '%s\n' "$NEW_FINGERPRINT" > "$FINGERPRINT_FILE"
chmod 0644 "$FINGERPRINT_FILE"

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
        docker compose -p velox-worker-$WORKER_ID -f $COMPOSE_FILE up -d --force-recreate
  3. Tail master log for the registration marker:
        journalctl -u velox-server --since '30s ago' 2>/dev/null | grep -E '$WORKER_ID|hello_ack|session'
        (or:   sudo journalctl -u velox-server -f   to follow live)
NEXT
