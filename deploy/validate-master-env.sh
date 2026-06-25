#!/usr/bin/env bash
# /opt/velox/current/deploy/validate-master-env.sh
#
# Canonical runtime validator for /etc/velox-server.env. Single source of
# truth for the production deploy-time invariants on the master env file.
# Closes audit-verdict block #3 ("deploy installer can declare success with
# invalid config"). Invoked by:
#
#   * deploy/install-server.sh          (between Step 6 and Step 7)
#   * deploy/playbooks/deploy-master-config.yml   (mandatory pre-checks)
#   * deploy/playbooks/rollback.yml               (post-rollback)
#
# Strategy: ACCUMULATE errors. Every check_* call increments ERR_COUNT
# instead of exiting immediately. Operators see ALL problems in one pass.
#
# Severity:
#   HARD FAIL (exit 1) — any of: CHANGE_ME_* placeholder, mandatory field
#                        empty, TLS triple partial, allowed-workers empty or
#                        with wildcard/duplicates, malformed URL.
#   WARNING (exit 0)   — admin token short (< 32 chars), http:// MASTER_URL,
#                        empty VELOX_GRPC_PORT (REST-only mode), TLS files
#                        unreadable, etc.
#
# Exit codes:
#   0   env file passed every check (warnings allowed)
#   1   at least one HARD-FAIL field; offending lines printed
#   2   env file unreadable / missing
#   64  usage error (no env file path supplied AND /etc/velox-server.env absent
#      is treated as level-2, NOT usage; this code reserved for future)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source shared helpers. Three search paths cover both: install-server.sh
# invocations (deploy/) and apply-local-worker-config.sh invocations
# (deploy/scripts/), and Ansible's /opt/velox/current/deploy/ in production.
for lib in \
    "${SCRIPT_DIR}/scripts/lib-validations.sh" \
    "${SCRIPT_DIR}/lib-validations.sh" \
    "${SCRIPT_DIR%/deploy/}/deploy/scripts/lib-validations.sh"; do
    if [[ -r "$lib" ]]; then
        # shellcheck source=lib-validations.sh
        source "$lib"
        break
    fi
done
if ! declare -F is_https_url >/dev/null 2>&1; then
    printf '[validate-master-env][FAIL] cannot find lib-validations.sh (looked under %s/{scripts,lib-validations.sh})\n' "$SCRIPT_DIR" >&2
    exit 64
fi

# Default to /etc/velox-server.env; overridable for tests.
ENV_FILE="${1:-/etc/velox-server.env}"

log()  { printf '[validate-master-env] %s\n' "$*" >&2; }
warn() { printf '[validate-master-env][WARN] %s\n' "$*" >&2; }
err()  { printf '[validate-master-env][FAIL] %s\n' "$*" >&2; }
ok()   { printf '[validate-master-env][OK] %s\n' "$*" >&2; }

if [[ ! -r "$ENV_FILE" ]]; then
    err "env file not readable: $ENV_FILE"
    err "fix: copy from $(dirname "$SCRIPT_DIR")/deploy/velox-server.env.example, edit values, then re-run."
    exit 2
fi

ERR_COUNT=0
WARN_COUNT=0

# Helper: print value of KEY= from the env file, skipping comment lines and
# blank lines. Optional surrounding " or ' quotes are stripped (ansible-vault
# rendering uses double-quoted values; some operators quote manually).
get_env_value() {
    local file="$1"
    local key="$2"
    awk -v k="$key" '
        /^[[:space:]]*#/                  { next }
        /^[[:space:]]*$/                  { next }
        $0 ~ "^[[:space:]]*" k "=" {
            sub("^[[:space:]]*" k "=", "")
            sub("^\"", "");   sub("\"$", "")
            sub("^\x27", ""); sub("\x27$", "")
            print
            exit
        }
    ' "$file"
}

is_pinned_image_ref() {
    local ref="${1:-}"
    [[ "$ref" =~ ^ghcr\.io/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+@sha256:[a-f0-9]{64}$ ]]
}

# ─── CHANGE_ME sweep on ACTIVE lines only ───────────────────────────────────
# Match uncommented lines (anything that doesn't start with `#` after
# whitespace). A comment mentioning CHANGE_ME is acceptable; an active line
# containing CHANGE_ME_ is fatal — operators MUST replace every placeholder.
if CHANGE_ME_HITS="$(grep -nE '^[[:space:]]*[^#].*CHANGE_ME_' "$ENV_FILE" 2>/dev/null)"; then
    err "CHANGE_ME_* placeholder still present in active lines (operator MUST replace before deploy):"
    while IFS= read -r line; do
        err "    $line"
    done <<<"$CHANGE_ME_HITS"
    err "fix: edit $ENV_FILE and replace every CHANGE_ME_* literal with a real value"
    ERR_COUNT=$((ERR_COUNT + 1))
fi

# ─── VELOX_ADMIN_TOKEN ──────────────────────────────────────────────────────
ADMIN_TOKEN="$(get_env_value "$ENV_FILE" VELOX_ADMIN_TOKEN)"
if [[ -z "$ADMIN_TOKEN" ]]; then
    err "VELOX_ADMIN_TOKEN is empty or missing (mandatory for REST API auth)"
    ERR_COUNT=$((ERR_COUNT + 1))
elif [[ "$ADMIN_TOKEN" =~ CHANGE_ME_ ]]; then
    err "VELOX_ADMIN_TOKEN still set to a CHANGE_ME_* placeholder"
    ERR_COUNT=$((ERR_COUNT + 1))
elif [[ ${#ADMIN_TOKEN} -lt 32 ]]; then
    warn "VELOX_ADMIN_TOKEN length=${#ADMIN_TOKEN} chars; recommend ≥ 32 chars. Server side enforces a minimum, but a short token is a foot-gun."
    WARN_COUNT=$((WARN_COUNT + 1))
fi

# ─── VELOX_ALLOWED_WORKERS ──────────────────────────────────────────────────
# Mirror ValidateProductionWorkers in DataServer/internal/config/workers_validator.go:
#   * non-empty (production rejects empty allowlists)
#   * no '*' wildcard
#   * no blank tokens (parse-and-trim, then reject empty)
#   * unique IDs
#   * canonical shape (1..63 chars, [A-Za-z0-9._-])
WORKERS_RAW="$(get_env_value "$ENV_FILE" VELOX_ALLOWED_WORKERS)"
if [[ -z "$WORKERS_RAW" ]]; then
    err "VELOX_ALLOWED_WORKERS is empty (rejects all workers — fleet never starts)"
    ERR_COUNT=$((ERR_COUNT + 1))
else
    declare -A SEEN_WORKERS=()
    WORKER_BAD=()
    WORKER_DUPES=()
    IFS=',' read -ra TOKENS <<<"$WORKERS_RAW"
    for raw in "${TOKENS[@]}"; do
        # Trim whitespace; reject empties.
        token="$(echo "$raw" | xargs 2>/dev/null || echo "$raw")"
        if [[ -z "$token" ]]; then
            WORKER_BAD+=("(empty token between commas)")
            continue
        fi
        if [[ "$token" == "*" ]]; then
            WORKER_BAD+=("'*' wildcard")
            continue
        fi
        if ! worker_id_shape "$token"; then
            WORKER_BAD+=("'$token' (canonical shape: ^[A-Za-z0-9._-]{1,63}\$)")
            continue
        fi
        if [[ -n "${SEEN_WORKERS[$token]:-}" ]]; then
            WORKER_DUPES+=("'$token'")
            continue
        fi
        SEEN_WORKERS[$token]=1
    done
    if (( ${#WORKER_BAD[@]} > 0 )); then
        err "VELOX_ALLOWED_WORKERS invalid entries: ${WORKER_BAD[*]}"
        ERR_COUNT=$((ERR_COUNT + 1))
    fi
    if (( ${#WORKER_DUPES[@]} > 0 )); then
        err "VELOX_ALLOWED_WORKERS duplicate IDs: ${WORKER_DUPES[*]}"
        ERR_COUNT=$((ERR_COUNT + 1))
    fi
fi

# ─── MASTER_PUBLIC_URL ──────────────────────────────────────────────────────
MASTER_URL="$(get_env_value "$ENV_FILE" MASTER_PUBLIC_URL)"
if [[ -z "$MASTER_URL" ]]; then
    err "MASTER_PUBLIC_URL is empty (workers cannot dial gRPC back to the master)"
    ERR_COUNT=$((ERR_COUNT + 1))
elif [[ "$MASTER_URL" =~ CHANGE_ME_ ]]; then
    err "MASTER_PUBLIC_URL still set to a CHANGE_ME_* placeholder"
    ERR_COUNT=$((ERR_COUNT + 1))
else
    case "$(is_https_url "$MASTER_URL"; echo $?)" in
        0) : ;;  # https — ok
        1) warn "MASTER_PUBLIC_URL uses http:// — only valid behind VPN/front-door TLS. Production must be https://."
            WARN_COUNT=$((WARN_COUNT + 1)) ;;
        *) err "MASTER_PUBLIC_URL malformed: '$MASTER_URL' (expected https://host[:port][/path])"
            ERR_COUNT=$((ERR_COUNT + 1)) ;;
    esac
fi

# ─── VELOX_SERVER_IMAGE ─────────────────────────────────────────────────────
SERVER_IMAGE="$(get_env_value "$ENV_FILE" VELOX_SERVER_IMAGE)"
if [[ -z "$SERVER_IMAGE" ]]; then
    err "VELOX_SERVER_IMAGE is empty (master deploy now requires a pinned container image)"
    ERR_COUNT=$((ERR_COUNT + 1))
elif [[ "$SERVER_IMAGE" =~ CHANGE_ME_ ]]; then
    err "VELOX_SERVER_IMAGE still set to a CHANGE_ME_* placeholder"
    ERR_COUNT=$((ERR_COUNT + 1))
elif ! is_pinned_image_ref "$SERVER_IMAGE"; then
    err "VELOX_SERVER_IMAGE must be a pinned GHCR digest, got '$SERVER_IMAGE'"
    ERR_COUNT=$((ERR_COUNT + 1))
fi

# ─── TLS triple + insecure-dev opt-in ──────────────────────────────────────
TLS_CERT="$(get_env_value "$ENV_FILE" VELOX_GRPC_TLS_CERT_FILE)"
TLS_KEY="$(get_env_value "$ENV_FILE" VELOX_GRPC_TLS_KEY_FILE)"
TLS_CA="$(get_env_value "$ENV_FILE" VELOX_GRPC_TLS_CA_FILE)"
INSECURE_DEV="$(get_env_value "$ENV_FILE" VELOX_GRPC_ALLOW_INSECURE_DEV)"

cert_set=$([ -n "$TLS_CERT" ] && echo Y || echo N)
key_set=$([ -n "$TLS_KEY" ] && echo Y || echo N)
ca_set=$([ -n "$TLS_CA" ] && echo Y || echo N)

if [[ -n "$TLS_CERT" || -n "$TLS_KEY" || -n "$TLS_CA" ]]; then
    if [[ -z "$TLS_CERT" || -z "$TLS_KEY" || -z "$TLS_CA" ]]; then
        err "TLS triple incomplete (cert=$cert_set, key=$key_set, ca=$ca_set). All three VELOX_GRPC_TLS_* paths are required if any is set — partial triples would crash at runtime."
        ERR_COUNT=$((ERR_COUNT + 1))
    fi
else
    # No TLS at all — production requires either TLS or explicit dev opt-in.
    if [[ "${INSECURE_DEV,,}" != "true" ]]; then
        err "no TLS configured AND VELOX_GRPC_ALLOW_INSECURE_DEV!=true. Production requires the cert+key+CA triple OR an explicit dev-only flag."
        ERR_COUNT=$((ERR_COUNT + 1))
    else
        warn "VELOX_GRPC_ALLOW_INSECURE_DEV=true with no TLS — plaintext gRPC. Production MUST NOT use this; CI/dev only."
        WARN_COUNT=$((WARN_COUNT + 1))
    fi
fi

# ─── VELOX_DB_PATH ─────────────────────────────────────────────────────────
DB_PATH="$(get_env_value "$ENV_FILE" VELOX_DB_PATH)"
if [[ -z "$DB_PATH" ]]; then
    err "VELOX_DB_PATH is empty (server has no persistent Jobs/Tasks/Workers state)"
    ERR_COUNT=$((ERR_COUNT + 1))
fi

# ─── VELOX_GRPC_PORT ───────────────────────────────────────────────────────
GRPC_PORT="$(get_env_value "$ENV_FILE" VELOX_GRPC_PORT)"
if [[ -z "$GRPC_PORT" ]]; then
    warn "VELOX_GRPC_PORT empty — server will run in REST-only mode (no gRPC workers can register; legacy HTTP pull endpoints deprecated per docs/roadmap/14-polling-removal.md)"
    WARN_COUNT=$((WARN_COUNT + 1))
elif ! is_port "$GRPC_PORT"; then
    err "VELOX_GRPC_PORT='$GRPC_PORT' is not a valid 1..65535 port"
    ERR_COUNT=$((ERR_COUNT + 1))
fi

# ─── Summary ───────────────────────────────────────────────────────────────
if (( ERR_COUNT > 0 )); then
    err "FAIL: $ERR_COUNT error(s)${WARN_COUNT:+, $WARN_COUNT warning(s)} on $ENV_FILE"
    err "fix the errors above, then re-run: $0 $ENV_FILE"
    exit 1
fi
if (( WARN_COUNT > 0 )); then
    ok "PASS with $WARN_COUNT warning(s) — review the WARN lines above before declaring production-ready"
    exit 0
fi
ok "PASS: env file is production-ready"
exit 0
