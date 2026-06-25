#!/usr/bin/env bash
# deploy/runtime/checklist-verify.sh
# ─────────────────────────────────────────────────────────────────────────────
# Automates sections 5–15 of the Velox worker VPS deployment checklist on a
# fresh (or freshly-provisioned) worker host. The remaining sections are out
# of scope of an automated runner and remain operator checks:
#
#   1–4  Pre-emptive (github.com Packages UI, workflow status, PAT choice).
#        These are operator actions BEFORE this script can run — there is
#        nothing on the VPS to verify until docker login succeeds and a
#        digest is published.
#
#   16–18 Post-deployment audit ops (manual canary observation, restart
#              resilience, rollback). Out of scope for the automated
#              runner — operator-side checks.
#
#   13         Log scrub. `docker logs --since ${VELOX_LOG_SINCE:-10m}` on
#              the running worker container (declared healthy by §11) and
#              FAIL on ANY of the 8 forbidden tokens (plaintext,
#              allow_insecure, fallback, python emergency, empty executor
#              registry, certificate error, permission denied,
#              unauthenticated). Match is case-insensitive so log-level
#              noise like "Permission Denied" still trips the gate. Empty
#              log in window = FAIL (healthy worker emits heartbeats).
#
#   14         Master-side CONNECTED assertion. This script DOES run it
#              when a master HTTP URL is supplied — --master flag,
#              VELOX_MASTER_API_BASE in worker.env, or derived from
#              VELOX_GRPC_MASTER_URL. If no URL can be resolved, section
#              14 FAILS with a clear remediation hint rather than silently
#              skipping, so the operator must explicitly opt out.
#
#   15         E2E Canary SUCCEEDED assertion. This script invokes
#              deploy/runtime/submit-canary.sh and bridges its exit
#              code (0=PASS, 1=FAIL, 255=SKIP) to the record() pattern.
#              Section 15 SKIPs only when opt-in prerequisites
#              (VELOX_ADMIN_TOKEN, VELOX_DB_PATH, a running worker
#              container) are missing — a missing render-required tool
#              (ffmpeg, ffprobe, sqlite3) is FAIL, NOT SKIP, because
#              §15's whole purpose is to exercise the render pipeline.
#              See section_15_canary() body for the precise contract.
#
# Usage:
#   sudo deploy/runtime/checklist-verify.sh [options]
#
# Exit codes:
#   0  every section PASS (or SKIP-with-cause)
#   1  at least one section FAIL
#   2  pre-condition failure (missing tool, not root, bad args, …)
#
# Why this script runs every section even when an earlier one fails:
#   the alternate "fail fast" mode hides downstream failure modes that would
#   otherwise surface after the operator fixes the first bug. Diagnostically,
#   a full sweep is more valuable than a single leading signal. Operators can
#   still short-circuit deploy with `--skip-deploy` when they only want to
#   audit pre-deploy file/config integrity (sections 5–9).

set -euo pipefail

readonly VERSION="1.0.0"
readonly SCRIPT_NAME="$(basename "$0")"

# ── Defaults & argument parsing ─────────────────────────────────────────────
IMAGE=""
WORKER_ID=""
MASTER=""
MASTER_API="/api/v1/workers"
SKIP_DEPLOY=0
JSON_OUT=""
VERBOSE=0

usage() {
    cat <<USAGE
Usage: $SCRIPT_NAME [options]

Options:
  --image <ref>        Worker image reference (must contain @sha256:).
                       Default: read from /etc/velox-worker/worker.env.
  --worker-id <id>     Expected worker id (matches compose's container_name).
                       Default: read from /etc/velox-worker/worker.env.
  --master <base_url>  Master HTTP base URL used by section 14 to query
                       /api/v1/workers. Default: derive from
                       VELOX_MASTER_API_BASE, else from VELOX_GRPC_MASTER_URL
                       (host extracted; port = VELOX_HTTP_PORT or 8000).
  --master-api <path>  Master API path queried by section 14
                       (default: /api/v1/workers).
  --skip-deploy        Do NOT run prepare-host.sh (read-only mode — sections
                       10–12 will be SKIP, useful while editing worker.env).
  --json <path>        Write a machine-readable summary to <path> in addition
                       to the human-readable stdout table.
  --verbose            Print sub-step diagnostics under each section header.
  -h, --help           Show this help and exit.

Exit codes:
  0   every section PASS or SKIP-with-cause
  1   at least one section FAIL
  2   pre-condition failure (missing tool, not root, …)

USAGE
}

while [[ $# -gt 0 ]]; do case "$1" in
    --image)        IMAGE="$2"; shift 2 ;;
    --worker-id)    WORKER_ID="$2"; shift 2 ;;
    --master)       MASTER="$2"; shift 2 ;;
    --master-api)   MASTER_API="$2"; shift 2 ;;
    --skip-deploy)  SKIP_DEPLOY=1; shift ;;
    --json)         JSON_OUT="$2"; shift 2 ;;
    --verbose)      VERBOSE=1; shift ;;
    -h|--help)      usage; exit 0 ;;
    *) printf 'unknown argument: %s\n\n' "$1" >&2; usage >&2; exit 2 ;;
esac; done

# ── Optional color (TTY only) ────────────────────────────────────────────────
if [[ -t 1 ]]; then
    RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'
    BLUE=$'\033[0;34m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
    RED=''; GREEN=''; YELLOW=''; BLUE=''; BOLD=''; NC=''
fi

log()  { printf '%s[check]%s %s\n' "$BLUE"  "$NC" "$*"; }
ok()   { printf '%s[  OK]%s %s\n' "$GREEN" "$NC" "$*"; }
warn() { printf '%s[WARN]%s %s\n' "$YELLOW" "$NC" "$*" >&2; }
fail() { printf '%s[FAIL]%s %s\n' "$RED"    "$NC" "$*" >&2; exit 2; }
vrb()  { [[ "$VERBOSE" -eq 1 ]] && printf '       %s\n' "$*" || true; }

# ── Pre-conditions ───────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    fail "This script must run as root (use sudo)."
fi
for tool in docker openssl curl jq; do
    command -v "$tool" >/dev/null 2>&1 \
        || fail "$tool CLI not found on PATH — install before re-running."
done
docker compose version >/dev/null 2>&1 \
    || fail "docker compose plugin missing — install docker-compose-plugin."
docker info >/dev/null 2>&1 \
    || fail "Cannot reach the docker daemon — check the caller's permissions."

readonly ENV_FILE="/etc/velox-worker/worker.env"
[[ -r "$ENV_FILE" ]] \
    || fail "env file not found at $ENV_FILE; copy deploy/runtime/worker.env.example and edit."

# shellcheck disable=SC1090
source "$ENV_FILE"

: "${IMAGE:=${VELOX_WORKER_IMAGE:-}}"
: "${WORKER_ID:=${VELOX_WORKER_ID:-}}"
HEALTH_PORT="${VELOX_HEALTH_PORT:-8081}"

# Master HTTP base URL for section 14. Precedence (highest first):
#   1. --master CLI flag (set earlier in arg parsing).
#   2. VELOX_MASTER_API_BASE in worker.env (operator-supplied).
#   3. Derive from VELOX_GRPC_MASTER_URL — strip the gRPC port,
#      wrap the host in http://, port = VELOX_HTTP_PORT (or 8000).
# If none of the three paths produces a URL, section 14 will FAIL
# with a remediation hint; we do NOT silently skip — operators must
# explicitly opt out of the master-side check.
if [[ -z "$MASTER" && -n "${VELOX_MASTER_API_BASE:-}" ]]; then
    MASTER="$VELOX_MASTER_API_BASE"
elif [[ -z "$MASTER" && -n "${VELOX_GRPC_MASTER_URL:-}" ]]; then
    mhost="${VELOX_GRPC_MASTER_URL%%:*}"
    if [[ -n "$mhost" ]]; then
        MASTER="http://${mhost}:${VELOX_HTTP_PORT:-8000}"
    fi
fi

[[ -n "$IMAGE"     ]] || fail "VELOX_WORKER_IMAGE not set (--image or worker.env)."
[[ -n "$WORKER_ID" ]] || fail "VELOX_WORKER_ID not set (--worker-id or worker.env)."

readonly IMAGE
readonly WORKER_ID
readonly MASTER
readonly MASTER_API
readonly HEALTH_PORT

log "Velox worker VPS checklist verifier v$VERSION"
log "image      : $IMAGE"
log "worker_id  : $WORKER_ID"
log "master     : ${MASTER:-<not configured — section 14 will FAIL with remediation>}"
log "skip_deploy: $SKIP_DEPLOY"
log "json_out   : ${JSON_OUT:-<none>}"

# ── Result aggregation ───────────────────────────────────────────────────────
# 4 parallel arrays keyed by position. Tiny list, no need for hash maps.
declare -a SECTION_IDS=()
declare -a SECTION_TITLES=()
declare -a SECTION_STATUS=()    # PASS | FAIL | SKIP
declare -a SECTION_DETAILS=()

record() {
    SECTION_IDS+=("$1")
    SECTION_TITLES+=("$2")
    SECTION_STATUS+=("$3")
    SECTION_DETAILS+=("${4:-}")
}

section_header() {
    printf '\n%s── Section %s: %s ──%s\n' "$BOLD" "$1" "$2" "$NC"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 5 — Pull via digest
# ═════════════════════════════════════════════════════════════════════════════
section_5_pull() {
    section_header 5 "Pull via digest"

    if [[ "$IMAGE" != *@sha256:* ]]; then
        record 5 "Pull via digest" FAIL \
            "image ref does not use @sha256: digest syntax (got: $IMAGE)"
        return 0
    fi

    vrb "docker pull $IMAGE"
    local pull_out
    if ! pull_out="$(docker pull "$IMAGE" 2>&1)"; then
        record 5 "Pull via digest" FAIL "docker pull exited non-zero (output below)"
        printf '%s\n' "$pull_out" | sed 's/^/       /' >&2
        return 0
    fi
    vrb "$pull_out"

    if printf '%s' "$pull_out" | grep -Eiq \
        'unauthorized|access denied|authentication required|manifest unknown'; then
        record 5 "Pull via digest" FAIL "pull output mentions auth/manifest error"
        return 0
    fi
    if printf '%s' "$pull_out" | grep -Eq \
        'Status: Downloaded newer image|Status: Image is up to date|Status: Downloaded'; then
        record 5 "Pull via digest" PASS "pull OK"
        return 0
    fi

    record 5 "Pull via digest" FAIL "pull returned unexpected status line"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 6 — Local digest + architecture
# ═════════════════════════════════════════════════════════════════════════════
section_6_digest() {
    section_header 6 "Local digest + architecture"

    local digests_json
    if ! digests_json="$(docker image inspect "$IMAGE" \
            --format '{{json .RepoDigests}}' 2>/dev/null)"; then
        record 6 "Local digest + architecture" FAIL \
            "image not present locally (inspect failed — did section 5 succeed?)"
        return 0
    fi

    if ! printf '%s' "$digests_json" | grep -q -- "$IMAGE"; then
        record 6 "Local digest + architecture" FAIL \
            "RepoDigests does not contain $IMAGE (got: $digests_json)"
        return 0
    fi

    local os_arch
    os_arch="$(docker image inspect "$IMAGE" \
        --format 'OS={{.Os}} ARCH={{.Architecture}}' 2>/dev/null || true)"
    vrb "$os_arch"

    if [[ "$os_arch" == "OS=linux ARCH=amd64" ]]; then
        record 6 "Local digest + architecture" PASS "$os_arch"
    else
        record 6 "Local digest + architecture" FAIL \
            "unexpected OS/ARCH (want OS=linux ARCH=amd64, got $os_arch)"
    fi
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 7 — worker.env integrity
# ═════════════════════════════════════════════════════════════════════════════
section_7_worker_env() {
    section_header 7 "worker.env protected"

    local perms
    if ! perms="$(stat -c '%U:%G %a' "$ENV_FILE" 2>/dev/null)"; then
        record 7 "worker.env protected" FAIL "stat failed on $ENV_FILE"
        return 0
    fi
    if [[ "$perms" != "root:root 600" ]]; then
        record 7 "worker.env protected" FAIL \
            "want perms root:root 600 (got: $perms)"
        return 0
    fi

    local required=(
        VELOX_WORKER_ID
        VELOX_WORKER_NAME
        VELOX_WORKER_IMAGE
        VELOX_GRPC_MASTER_URL
        VELOX_WORKER_CREDENTIAL_FILE
        VELOX_GRPC_TLS_CERT_FILE
        VELOX_GRPC_TLS_KEY_FILE
        VELOX_GRPC_TLS_CA_FILE
        VELOX_WORK_DIR
        VELOX_MAX_ACTIVE_JOBS
        VELOX_HEALTH_PORT
    )
    local missing=()
    local v
    for v in "${required[@]}"; do
        # Fixed list of identifiers — safe to expand unquoted into the regex
        # here (no shell metacharacters in any var name).
        grep -qE "^${v}=" "$ENV_FILE" || missing+=("$v")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        record 7 "worker.env protected" FAIL \
            "missing vars: ${missing[*]}"
        return 0
    fi

    local img_decl
    img_decl="$(grep -E '^VELOX_WORKER_IMAGE=' "$ENV_FILE" | head -1 || true)"
    if ! printf '%s' "$img_decl" | grep -q '@sha256:'; then
        record 7 "worker.env protected" FAIL \
            "VELOX_WORKER_IMAGE must use @sha256: digest (got: $img_decl)"
        return 0
    fi

    # Cover all GitHub token families in one alternation:
    #   ghp_  classic PAT    github_pat_  fine-grained PAT
    #   ghs_  GitHub App sec gho_          OAuth
    #   ghu_  user-to-server
    if grep -Eq 'gh[psou]_|github_pat_' "$ENV_FILE"; then
        record 7 "worker.env protected" FAIL \
            "PAT pattern (ghp_ or github_pat_) found in worker.env — git-history hazard"
        return 0
    fi
    if grep -Eiq 'allow_insecure' "$ENV_FILE"; then
        record 7 "worker.env protected" FAIL \
            "allow_insecure token found in worker.env — must live in worker_config.json only"
        return 0
    fi

    record 7 "worker.env protected" PASS "perms=${perms}; all required vars; digest-pinned"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 8 — TLS certs + credential
# ═════════════════════════════════════════════════════════════════════════════
section_8_certs() {
    section_header 8 "TLS certs + credential"

    local certs_dir="/etc/velox-worker/certs"
    local secrets_dir="/etc/velox-worker/secrets"

    local required=(
        "$certs_dir/worker.crt"
        "$certs_dir/worker.key"
        "$certs_dir/ca.crt"
        "$secrets_dir/worker_credential"
    )
    local missing=()
    local f
    for f in "${required[@]}"; do
        [[ -e "$f" ]] || missing+=("$f")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        record 8 "TLS certs + credential" FAIL "missing files: ${missing[*]}"
        return 0
    fi

    local bad_perms=()
    local mode
    for f in "$certs_dir/worker.key" "$secrets_dir/worker_credential"; do
        mode="$(stat -c '%a' "$f")"
        [[ "$mode" == "600" ]] || bad_perms+=("$f=mode$mode (want 600)")
    done
    for f in "$certs_dir/worker.crt" "$certs_dir/ca.crt"; do
        mode="$(stat -c '%a' "$f")"
        # Both 600 (tightened) and 644 (default) are acceptable for read-only certs.
        [[ "$mode" == "600" || "$mode" == "644" ]] \
            || bad_perms+=("$f=mode$mode (want 600 or 644)")
    done
    if [[ ${#bad_perms[@]} -gt 0 ]]; then
        record 8 "TLS certs + credential" FAIL "bad perms: ${bad_perms[*]}"
        return 0
    fi

    local verify_out
    if ! verify_out="$(openssl verify -CAfile "$certs_dir/ca.crt" \
            "$certs_dir/worker.crt" 2>&1)"; then
        record 8 "TLS certs + credential" FAIL \
            "openssl verify exited non-zero: $verify_out"
        return 0
    fi
    vrb "$verify_out"
    if [[ "$verify_out" != "worker.crt: OK" ]]; then
        record 8 "TLS certs + credential" FAIL "verify output: $verify_out"
        return 0
    fi

    local not_after=""
    # `if ! … ; then … return 0 ; fi` explicitly disables errexit AND pipefail
    # propagation for this command substitution. Without it, an openssl or
    # sed failure terminates the verifier before `record` can emit FAIL.
    if ! not_after="$(openssl x509 -in "$certs_dir/worker.crt" \
            -noout -enddate 2>/dev/null | sed 's/^notAfter=//')"; then
        record 8 "TLS certs + credential" FAIL \
            "openssl x509 -enddate failed (readable cert? permissions?)"
        return 0
    fi
    vrb "notAfter=$not_after"
    local not_after_epoch now_epoch
    if ! not_after_epoch="$(date -d "$not_after" +%s 2>/dev/null)"; then
        record 8 "TLS certs + credential" FAIL \
            "could not parse notAfter ($not_after) — check locale"
        return 0
    fi
    now_epoch="$(date +%s)"
    if (( not_after_epoch <= now_epoch )); then
        record 8 "TLS certs + credential" FAIL \
            "worker.crt is EXPIRED (notAfter=$not_after)"
        return 0
    fi

    # RW-PROD-001 A9: cert CN MUST equal WORKER_ID (identity binding).
    # Without this assertion a stolen worker.key could impersonate any
    # worker_id, defeating the registry's CN-based filter.
    local cert_cn=""
    if ! cert_cn="$(openssl x509 -in "$certs_dir/worker.crt" \
            -noout -subject 2>/dev/null \
            | sed -n 's/.*CN *= *\([^,/]*\).*/\1/p' | tr -d ' ')"; then
        record 8 "TLS certs + credential" FAIL \
            "openssl x509 -subject failed"
        return 0
    fi
    if [[ -z "$cert_cn" ]]; then
        record 8 "TLS certs + credential" FAIL \
            "could not extract CN from worker.crt subject"
        return 0
    fi
    if [[ "$cert_cn" != "$WORKER_ID" ]]; then
        record 8 "TLS certs + credential" FAIL \
            "cert CN=${cert_cn} does NOT match WORKER_ID=${WORKER_ID} (RW-PROD-001 A9 binding)"
        return 0
    fi

    vrb "cert_cn=${cert_cn} matches WORKER_ID"
    record 8 "TLS certs + credential" PASS \
        "chain OK; notAfter=${not_after}; CN=${cert_cn} matches WORKER_ID"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 9 — Compose config
# ═════════════════════════════════════════════════════════════════════════════
section_9_compose() {
    section_header 9 "Compose config"

    local compose_dir="/opt/velox-worker"
    local compose_yml="$compose_dir/compose.yml"
    if [[ ! -r "$compose_yml" ]]; then
        record 9 "Compose config" FAIL \
            "$compose_yml not readable — has prepare-host.sh run at least once?"
        return 0
    fi

    # compose.yml uses ${VELOX_*}-style substitutions; source env_file in a
    # subshell so the exports do not leak back into the parent verifier env.
    local cfg_json
    if ! cfg_json="$(
        set -a
        # shellcheck disable=SC1090
        source "$ENV_FILE"
        set +a
        docker compose -p "velox-verify-${WORKER_ID}" \
            -f "$compose_yml" config --format json 2>/dev/null
    )"; then
        record 9 "Compose config" FAIL \
            "docker compose config exited non-zero (env vars / yaml may be invalid)"
        return 0
    fi

    local svc="velox-worker"
    local svc_json
    svc_json="$(printf '%s' "$cfg_json" | jq -r --arg s "$svc" '.services[$s]')"
    if [[ -z "$svc_json" || "$svc_json" == "null" ]]; then
        record 9 "Compose config" FAIL "service '$svc' not present in rendered config"
        return 0
    fi

    local failures=()
    local img
    img="$(printf '%s' "$svc_json" | jq -r '.image // ""')"
    [[ "$img" == *@sha256:* ]] || failures+=("image uses digest=false (got: $img)")

    local nname
    nname="$(printf '%s' "$svc_json" | jq -r '.container_name // ""')"
    [[ "$nname" == "velox-worker-$WORKER_ID" ]] \
        || failures+=("container_name='$nname' (want velox-worker-$WORKER_ID)")

    [[ "$(printf '%s' "$svc_json" | jq -r '.read_only // false')" == "true" ]] \
        || failures+=("read_only != true")

    if ! printf '%s' "$svc_json" | jq -r '.cap_drop // [] | .[]' 2>/dev/null | grep -qx 'ALL'; then
        failures+=("cap_drop missing ALL")
    fi
    if ! printf '%s' "$svc_json" | jq -r '.security_opt // [] | .[]' 2>/dev/null | grep -qx 'no-new-privileges:true'; then
        failures+=("security_opt missing no-new-privileges:true")
    fi

    local mem_limit
    mem_limit="$(printf '%s' "$svc_json" | jq -r '.mem_limit // ""')"
    [[ -n "$mem_limit" ]] || failures+=("mem_limit unset")

    local vols ro_mounts
    vols="$(printf '%s' "$svc_json" | jq -r '.volumes // [] | .[]' 2>/dev/null)"
    # Compose renders volume strings as "src:dst[:mode]" with `:ro` (when
    # set) exactly at the end of the string. The `,` alternation in `:ro(,|$)`
    # is dead — composite compose output never contains a `,)`.
    ro_mounts="$(printf '%s\n' "$vols" | grep -E ':ro$' || true)"
    if ! printf '%s' "$ro_mounts" | grep -q '/etc/velox-worker/certs'; then
        failures+=("certs mount not present with :ro")
    fi
    if ! printf '%s' "$ro_mounts" | grep -q '/etc/velox-worker/secrets'; then
        failures+=("secrets mount not present with :ro")
    fi

    # Compose renders healthcheck.test as a JSON array (e.g.
    # ["CMD","/bin/sh","-c","curl … /health/ready || exit 1"]). Stream each
    # element so the substring check is robust to the array form.
    if ! printf '%s' "$svc_json" \
            | jq -r '.healthcheck.test // [] | .[] | tostring' \
            2>/dev/null | grep -qF '/health/ready'; then
        failures+=("healthcheck does not probe /health/ready")
    fi

    if [[ ${#failures[@]} -gt 0 ]]; then
        record 9 "Compose config" FAIL "violations: ${failures[*]}"
        return 0
    fi

    record 9 "Compose config" PASS \
        "image=${img}; ro mounts verified; caps dropped; readiness on /health/ready"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 10 — Prepare host (deployment)
# ═════════════════════════════════════════════════════════════════════════════
prepare_host_search_paths=(
    /opt/velox-worker/prepare-host.sh
    /usr/local/bin/velox-worker-prepare-host.sh
    /root/VeloxLEgit/deploy/runtime/prepare-host.sh
)

section_10_prepare() {
    section_header 10 "prepare-host.sh"

    if [[ "$SKIP_DEPLOY" -eq 1 ]]; then
        record 10 "prepare-host.sh" SKIP "--skip-deploy specified"
        return 0
    fi

    local script=""
    local p
    for p in "${prepare_host_search_paths[@]}"; do
        if [[ -x "$p" ]]; then
            script="$p"
            break
        fi
    done
    if [[ -z "$script" ]]; then
        record 10 "prepare-host.sh" FAIL \
            "prepare-host.sh not found in any of: ${prepare_host_search_paths[*]}"
        return 0
    fi

    vrb "running $script"
    local out
    if ! out="$("$script" 2>&1)"; then
        record 10 "prepare-host.sh" FAIL \
            "prepare-host.sh exited non-zero (last 12 lines below)"
        printf '%s\n' "$out" | tail -12 | sed 's/^/       /' >&2
        return 0
    fi
    vrb "$(printf '%s' "$out" | tail -8)"

    if printf '%s' "$out" | grep -Eq 'Worker .* is up'; then
        record 10 "prepare-host.sh" PASS \
            "prepare-host.sh reported successful bring-up"
        return 0
    fi

    record 10 "prepare-host.sh" FAIL \
        "ran to completion but final 'Worker ... is up' line not found"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 11 — Container reachable + healthy
# ═════════════════════════════════════════════════════════════════════════════
section_11_container() {
    section_header 11 "Container healthy"

    local name="velox-worker-${WORKER_ID}"

    local ps_line
    if ! ps_line="$(docker ps --filter "name=${name}" \
            --format '{{.Names}}|{{.Image}}|{{.Status}}' 2>/dev/null)"; then
        record 11 "Container healthy" FAIL "docker ps failed"
        return 0
    fi
    if [[ -z "$ps_line" ]]; then
        record 11 "Container healthy" FAIL \
            "no running container matching name=${name}"
        return 0
    fi

    local rc status health
    rc="$(docker inspect --format '{{.RestartCount}}' "$name" 2>/dev/null || echo "?")"
    status="$(docker inspect --format '{{.State.Status}}' "$name" 2>/dev/null || echo "?")"
    # Health may be empty while still in start_period; surface as "starting".
    health="$(docker inspect --format '{{.State.Health.Status}}' "$name" 2>/dev/null || echo "starting")"
    vrb "RestartCount=$rc  State.Status=$status  Health.Status=$health"

    local issues=()
    [[ "$rc" == "0" ]]      || issues+=("RestartCount=$rc (want 0)")
    [[ "$status" == "running" ]] || issues+=("State.Status=$status (want running)")
    [[ "$health" == "healthy" ]] || issues+=("Health.Status=$health (want healthy)")

    if [[ ${#issues[@]} -gt 0 ]]; then
        record 11 "Container healthy" FAIL "${issues[*]}"
        return 0
    fi

    record 11 "Container healthy" PASS "RestartCount=0 running healthy"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 12 — Liveness/readiness over HTTP
# ═════════════════════════════════════════════════════════════════════════════
section_12_health() {
    section_header 12 "/health/live + /health/ready"

    local issues=()
    local ep code body
    for ep in /health/live /health/ready; do
        # Per-endpoint temp file so the second curl cannot overwrite the
        # first curl's body if both happen in the same script iteration.
        body="/tmp/checklist-health-body.$$.${ep//\//_}"
        code="$(curl -sS -o "$body" -w '%{http_code}' \
            "http://127.0.0.1:${HEALTH_PORT}${ep}" 2>/dev/null)" || code="000"
        rm -f "$body"
        vrb "${ep} -> HTTP ${code}"
        if [[ "$code" != "200" ]]; then
            issues+=("${ep}=${code} (want 200)")
        fi
    done

    if [[ ${#issues[@]} -gt 0 ]]; then
        record 12 "/health/live + /health/ready" FAIL "${issues[*]}"
        return 0
    fi

    record 12 "/health/live + /health/ready" PASS \
        "/health/live=200; /health/ready=200"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 13 — Log scrub (forbidden tokens in last ${VELOX_LOG_SINCE:-10m})
# ═════════════════════════════════════════════════════════════════════════════
# Pulls the worker's stdout+stderr for the configured look-back window and
# SCANS for any of these 8 forbidden tokens (case-insensitive substring
# match):
#
#   plaintext              - TLS / gRPC dial without TLS
#   allow_insecure         - explicit degraded-mode flag (worker_config)
#   fallback               - any fallback path engaged (legacy python)
#   python emergency       - emergency python renderer path (RW-PROD-003)
#   empty executor registry- bootstrap gate failed (RW-PROD-005)
#   certificate error      - mTLS handshake / cert validation problems
#   permission denied      - credential / filesystem / gRPC permission issue
#   unauthenticated        - mTLS or API-token auth rejection
#
# Any single match fails the audit. Multi-word phrases are matched
# verbatim (NOT regex-word-bounded) so "python emergency" cannot be
# accidentally escaped by surrounding punctuation in production code.
#
# Edge cases:
#   * container not inspectable → SKIP (§11 already flagged the symptom)
#   * docker logs itself fails  → FAIL with rc
#   * empty log in window       → FAIL (a "healthy" worker emits
#                                 heartbeats per RW-PROD-004; silence is
#                                 a silent-failure signal)
#   * matches present           → FAIL with up to 5 distinct offending lines
#                                 emitted under the FAIL summary for triage
#
# Time window is configurable via VELOX_LOG_SINCE (default 10m). Operators
# can tighten (e.g. "2m") post-incident and broaden (e.g. "1h") for
# canary investigations.
section_13_logs() {
    local since="${VELOX_LOG_SINCE:-10m}"
    section_header 13 "Log scrub (last ${since})"

    local name="velox-worker-${WORKER_ID}"

    # If §11 already flagged the container as down, the log-scrub cannot
    # run. SKIP is the polite category — §11 surfaced the root cause.
    if ! docker inspect "$name" >/dev/null 2>&1; then
        record 13 "Log scrub (last ${since})" SKIP \
            "container ${name} not inspectable (see §11)"
        return 0
    fi

    local log_tmp="/tmp/checklist-sec13-body.$$.log"
    local log_rc=0
    # `docker logs --since` accepts Go duration strings (10m, 1h, …).
    # tee to stderr is NOT what we want — capture to a side file we own.
    if ! docker logs --since "$since" "$name" >"$log_tmp" 2>&1; then
        log_rc=$?
        rm -f "$log_tmp"
        record 13 "Log scrub (last ${since})" FAIL \
            "docker logs exited rc=${log_rc}; cannot scan (is ${name} running?)"
        return 0
    fi

    # Silence in the look-back window is itself an alert: a "healthy"
    # worker periodically emits heartbeats / readiness lines per the
    # RW-PROD-004 contract. An empty log in 10m is a strong silent-failure
    # signal (OOM, stuck goroutine, stdout broken pipe).
    if [[ ! -s "$log_tmp" ]]; then
        rm -f "$log_tmp"
        record 13 "Log scrub (last ${since})" FAIL \
            "no log output in last ${since} (a healthy worker emits periodic heartbeats — silence is a failure signal)"
        return 0
    fi

    # Single regex alternation over the 8 forbidden tokens. Case
    # insensitive so log-level noise ("Permission Denied", "PLAINTEXT",
    # "Allow_Insecure") still trips the gate.
    local matches
    matches="$(grep -iE \
        'plaintext|allow_insecure|fallback|python emergency|empty executor registry|certificate error|permission denied|unauthenticated' \
        "$log_tmp" || true)"
    local line_count
    line_count=$(wc -l < "$log_tmp" 2>/dev/null | tr -d ' ' || echo 0)

    if [[ -z "$matches" ]]; then
        rm -f "$log_tmp"
        record 13 "Log scrub (last ${since})" PASS \
            "no forbidden tokens in last ${since} (${line_count} lines scanned)"
        return 0
    fi

    # Compose a compact FAIL detail: distinct count + up to 5 first hits,
    # one per line, sed-prefixed for readability. Capping stops a noisy
    # burst from blowing out the audit summary.
    local hit_count distinct_hits
    hit_count="$(printf '%s\n' \"$matches\" | awk '!seen[$0]++ {n++} END{print n+0}')"
    distinct_hits="$(printf '%s\n' \"$matches\" \
        | awk '!seen[$0]++ {print; if (++c >= 5) exit}' \
        | sed 's/^/      /')"
    record 13 "Log scrub (last ${since})" FAIL \
        "${hit_count} distinct forbidden line(s) in ${line_count}-line window; first match(es):
${distinct_hits}"
    rm -f "$log_tmp"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 14 — Master sees CONNECTED worker
# ═════════════════════════════════════════════════════════════════════════════
# Verifies that the running worker is visible on the master's operator
# API as session_active=true, status=CONNECTED, and the freshest heartbeat
# in the canonical window (< 30s; matches workers.ConnectionStaleThreshold).
# Together with sections 11–12 (liveness/readiness) this closes the
# "process is alive but worker has not registered" gap.
#
# On ANY failure (master unreachable, /api/v1/workers auth-gated,
# worker_id absent, session_active=false, status!=CONNECTED,
# heartbeat_age_seconds >= 30) the audit FAILS with a structured
# detail so the operator has one fix cycle.
section_14_master_workers() {
    section_header 14 "Master sees CONNECTED worker"

    if [[ -z "$MASTER" ]]; then
        record 14 "Master sees CONNECTED worker" FAIL \
            "master URL not configured — pass --master http://host:port, or add VELOX_MASTER_API_BASE to worker.env"
        return 0
    fi

    vrb "GET $MASTER$MASTER_API"
    local body="/tmp/checklist-sec14-body.$$.json"
    local code=000 curl_rc=0
    # 5s timeout prevents hanging on a gated/unreachable master and
    # preserves the "fast audit sweep" contract of the rest of the script.
    code="$(curl -sS --max-time 5 -o "$body" -w '%{http_code}' \
        "$MASTER$MASTER_API" 2>/dev/null)" || curl_rc=$?
    vrb "HTTP=${code}  curl_rc=${curl_rc}"

    if [[ "$code" != "200" ]]; then
        local detail
        if (( curl_rc != 0 )); then
            detail="GET failed (curl_rc=${curl_rc}, http=${code}); master unreachable"
        elif [[ "$code" == "401" || "$code" == "403" ]]; then
            detail="GET returned HTTP ${code}; /api/v1/workers is auth-gated on this master (whitelist this network or point --master at a non-gated replica)"
        elif [[ "$code" == "404" ]]; then
            detail="GET returned HTTP 404 — wrong --master-api path? (current: ${MASTER_API})"
        else
            detail="GET returned HTTP ${code}"
        fi
        [[ -f "$body" ]] && rm -f "$body"
        record 14 "Master sees CONNECTED worker" FAIL "$detail"
        return 0
    fi

    if [[ ! -s "$body" ]]; then
        rm -f "$body"
        record 14 "Master sees CONNECTED worker" FAIL \
            "GET returned HTTP 200 but empty body"
        return 0
    fi

    # Validate JSON shape so the downstream jq selectors do not explode
    # on a misrouted proxied response that returns 200 + non-JSON body.
    if ! jq -e '.workers | type == "array"' "$body" >/dev/null 2>&1; then
        local preview
        preview="$(head -c 200 "$body" 2>/dev/null || true)"
        rm -f "$body"
        record 14 "Master sees CONNECTED worker" FAIL \
            "response does not contain a 'workers' array (body preview: ${preview})"
        return 0
    fi

    # Locate the entry for our WORKER_ID.
    local got_worker_id
    got_worker_id="$(jq -r --arg w "$WORKER_ID" \
        '.workers[]? | select(.worker_id == $w) | .worker_id' "$body" 2>/dev/null | head -1)"
    if [[ -z "$got_worker_id" ]]; then
        rm -f "$body"
        record 14 "Master sees CONNECTED worker" FAIL \
            "worker_id=${WORKER_ID} NOT present in master response (registry does not list this worker)"
        return 0
    fi

    # Three sub-assertions collected into one composite FAIL so the
    # operator gets a single fix cycle. Each is independently diagnosable.
    local issues=()
    local got_session_active got_status got_hb_age
    got_session_active="$(jq -r --arg w "$WORKER_ID" \
        '.workers[]? | select(.worker_id == $w) | .session_active' "$body")"
    got_status="$(jq -r --arg w "$WORKER_ID" \
        '.workers[]? | select(.worker_id == $w) | .status' "$body")"
    # heartbeat_age_seconds MUST be present, numeric, and < 30s.
    # Intentional: NO jq default here. A missing or null field must FAIL
    # the audit (`got_hb_age="null"` falls through the numeric regex and
    # is reported as not numeric), instead of sliding past as 0.
    got_hb_age="$(jq -r --arg w "$WORKER_ID" \
        '.workers[]? | select(.worker_id == $w) | .heartbeat_age_seconds' "$body")"

    rm -f "$body"

    [[ "$got_session_active" == "true" ]] \
        || issues+=("session_active=${got_session_active} (want true)")
    [[ "$got_status" == "CONNECTED" ]] \
        || issues+=("status=${got_status} (want CONNECTED; canonical enum: CONNECTED|STALE|DISCONNECTED|DRAINING per workers.ConnectionStatus)")

    if ! [[ "$got_hb_age" =~ ^[0-9]+$ ]]; then
        issues+=("heartbeat_age_seconds is not numeric (got: ${got_hb_age})")
    elif (( got_hb_age >= 30 )); then
        issues+=("heartbeat_age_seconds=${got_hb_age} (want < 30; canonical stale threshold is workers.ConnectionStaleThreshold=30s)")
    fi

    if [[ ${#issues[@]} -gt 0 ]]; then
        record 14 "Master sees CONNECTED worker" FAIL "${issues[*]}"
        return 0
    fi

    record 14 "Master sees CONNECTED worker" PASS \
        "session_active=true; status=CONNECTED; heartbeat_age_seconds=${got_hb_age}"
}

# ═════════════════════════════════════════════════════════════════════════════
# Section 15 — E2E Canary SUCCEEDED (delegates to submit-canary.sh)
# ═════════════════════════════════════════════════════════════════════════════
# Bridges deploy/runtime/submit-canary.sh's exit-code contract to the
# record() pattern:
#
#   rc == 0    → PASS  (detail = "PASS:" sentinel line)
#   rc == 255  → SKIP  (detail = "SKIP:" sentinel — pre-flight gate)
#   rc != 0    → FAIL  (detail = "FAIL:" sentinel)
#
# submit-canary.sh DOES NOT require a live master gRPC session — §14 has
# already verified the worker is registered. What it DOES need:
#
#   VELOX_ADMIN_TOKEN       cfg.Auth.AdminToken-equivalent bearer.
#                           POST /api/v1/orchestrator/jobs is gated by
#                           AdminAuthMiddleware (DataServer/internal/
#                           handlers/server/api/api_v1.go).
#   VELOX_DB_PATH           direct READ access to velox.db so the script
#                           can poll jobs.status + artifacts.sha256.
#                           Multi-host topologies (worker on one VPS,
#                           master on another) must expose a read-replica
#                           OR run the verifier on the master host. The
#                           co-located default (single compose on one
#                           VPS) Just Works.
#   docker running          the script uses `docker exec` to generate
#                           fixtures inside the worker container (its
#                           /tmp is a per-container tmpfs, NOT shared
#                           with the host). §10/§11 have already proved
#                           docker is operational by this point.
#   VELOX_WORKER_ID         optional; auto-detected from docker ps filter
#                           label=com.docker.compose.project=velox-worker
#                           when unset.
#
# Section 15 SKIPs-with-cause when any prerequisite is missing — NEVER
# FAILs just because the operator hasn't wired canary config up. SKIP
# is the polite category: "we did not run, here's the missing piece".
section_15_canary() {
    section_header 15 "E2E Canary SUCCEEDED"

    # Locate submit-canary.sh in canonical deploy locations. Mirror the
    # search strategy used by section_10 for prepare-host.sh.
    local script=""
    local p
    for p in \
        /opt/velox-worker/submit-canary.sh \
        /usr/local/bin/velox-submit-canary.sh \
        "${VELOXWORK_CHECKLIST_PARENT:-}/submit-canary.sh" \
        "$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)/submit-canary.sh"; do
        if [[ -r "$p" ]]; then
            script="$p"
            break
        fi
    done
    if [[ -z "$script" ]]; then
        record 15 "E2E Canary SUCCEEDED" SKIP \
            "submit-canary.sh not found in any of: /opt/velox-worker, /usr/local/bin, repo deploy/runtime, beside the verifier"
        return 0
    fi

    # Capture stdout+stderr to a temp file so we can parse the structured
    # sentinel line without re-running the canary. PIPESTATUS preserves
    # the canary's exit code through the pipeline.
    local canary_log="/tmp/checklist-sec15-body.$$.log"
    local rc=0
    VELOX_MASTER_URL="${MASTER}" \
        bash "$script" >"$canary_log" 2>&1 || rc=$?
    vrb "$(tail -n 50 "$canary_log" 2>/dev/null || true)"

    local detail=""
    case "$rc" in
        0)   detail="$(grep -E '^PASS:' "$canary_log" | head -1 | sed 's/^PASS: //')"  ;;
        255) detail="$(grep -E '^SKIP:' "$canary_log" | head -1 | sed 's/^SKIP: //')"  ;;
        *)   detail="$(grep -E '^FAIL:' "$canary_log" | head -1 | sed 's/^FAIL: //')"  ;;
    esac
    # Cap detail length so a verbose canary log doesn't blow out the
    # summary row. 200 chars is plenty for a single-line cause.
    if [[ ${#detail} -gt 200 ]]; then
        detail="${detail:0:197}..."
    fi

    rm -f "$canary_log"

    case "$rc" in
        0)   record 15 "E2E Canary SUCCEEDED" PASS  "${detail:-canary rendered and verified}" ;;
        255) record 15 "E2E Canary SUCCEEDED" SKIP  "${detail:-pre-flight gate (rc=255)}"     ;;
        *)   record 15 "E2E Canary SUCCEEDED" FAIL  "${detail:-non-zero exit (rc=${rc})}"      ;;
    esac
}

# ── Run in checklist order ──────────────────────────────────────────────────
section_5_pull
section_6_digest
section_7_worker_env
section_8_certs
section_9_compose
section_10_prepare
section_11_container
section_12_health
section_13_logs
section_14_master_workers
section_15_canary

# ── Summary table ───────────────────────────────────────────────────────────
printf '\n%s── Summary ──%s\n' "$BOLD" "$NC"

pass_count=0
fail_count=0
skip_count=0
i=0
for status in "${SECTION_STATUS[@]}"; do
    sid="${SECTION_IDS[$i]}"
    title="${SECTION_TITLES[$i]}"
    detail="${SECTION_DETAILS[$i]}"
    case "$status" in
        PASS)
            printf '  %s[PASS]%s Section %s: %s\n' "$GREEN" "$NC" "$sid" "$title"
            pass_count=$((pass_count + 1)) ;;
        FAIL)
            printf '  %s[FAIL]%s Section %s: %s — %s\n' "$RED" "$NC" "$sid" "$title" "$detail"
            fail_count=$((fail_count + 1)) ;;
        SKIP)
            printf '  %s[SKIP]%s Section %s: %s — %s\n' "$YELLOW" "$NC" "$sid" "$title" "$detail"
            skip_count=$((skip_count + 1)) ;;
        *)
            printf '  %s[????]%s Section %s: %s (unknown status: %s)\n' \
                "$RED" "$NC" "$sid" "$title" "$status"
            fail_count=$((fail_count + 1)) ;;
    esac
    i=$((i + 1))
done

total=$((pass_count + fail_count + skip_count))
printf '\n  PASS=%d  FAIL=%d  SKIP=%d  TOTAL=%d\n' \
    "$pass_count" "$fail_count" "$skip_count" "$total"

# ── Optional JSON summary ──────────────────────────────────────────────────
# Emits the per-section records through jq → NDJSON file → `jq -s --slurpfile`.
# jq universally accepts NDJSON (newline-separated JSON values); comma-
# separated streams are NOT reliably accepted across jq versions — v1's
# comma-join emitter failed with `parse error: Expected value before ','`
# against the verifier's jq. NDJSON is portable.
if [[ -n "$JSON_OUT" ]]; then
    ndjson="$(mktemp /tmp/velox-checklist-sections.XXXXXX.ndjson)"
    trap 'rm -f "${ndjson:-}"' EXIT

    i=0
    for status in "${SECTION_STATUS[@]}"; do
        sid="${SECTION_IDS[$i]}"
        title="${SECTION_TITLES[$i]}"
        detail="${SECTION_DETAILS[$i]}"
        # `-c` so each jq invocation emits exactly one JSON object on one
        # line; appending N times yields N newline-separated records → NDJSON.
        jq -nc \
            --arg id     "$sid" \
            --arg title  "$title" \
            --arg st     "$status" \
            --arg detail "$detail" \
            '{id: $id, title: $title, status: $st, detail: $detail}' \
            >> "$ndjson"
        i=$((i + 1))
    done

    # --slurpfile reads the NDJSON file as a stream of JSON values and
    # exposes the resulting array as `$sections`. Reference the whole
    # array (`sections: $sections`) — NOT `$sections[0]`, which would
    # index only the first section and drop the rest.
    jq -s \
        --slurpfile sections "$ndjson" \
        --arg version   "$VERSION" \
        --arg image     "$IMAGE" \
        --arg worker_id "$WORKER_ID" \
        --argjson skip  "$SKIP_DEPLOY" \
        --argjson pass  "$pass_count" \
        --argjson fail  "$fail_count" \
        --argjson skipc "$skip_count" \
        --argjson total "$total" \
        '{
            version:     $version,
            image:       $image,
            worker_id:   $worker_id,
            skip_deploy: $skip,
            sections:    $sections,
            summary:     {pass: $pass, fail: $fail, skip: $skipc, total: $total}
        }' > "$JSON_OUT"

    rm -f "$ndjson"
    trap - EXIT
    log "JSON summary written to $JSON_OUT"
fi

# ── Exit code ──────────────────────────────────────────────────────────────
if [[ "$fail_count" -gt 0 ]]; then
    exit 1
fi
exit 0
