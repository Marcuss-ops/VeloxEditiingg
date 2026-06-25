#!/usr/bin/env bash
# deploy/runtime/checklist-verify.sh
# ─────────────────────────────────────────────────────────────────────────────
# Automates sections 5–12 of the Velox worker VPS deployment checklist on a
# fresh (or freshly-provisioned) worker host. The remaining sections are out
# of scope of an automated runner and remain operator checks:
#
#   1–4  Pre-emptive (github.com Packages UI, workflow status, PAT choice).
#        These are operator actions BEFORE this script can run — there is
#        nothing on the VPS to verify until docker login succeeds and a
#        digest is published.
#   13–18 Post-deployment (logs, master polling, canary submit, restart
#        resilience). These require a running master and a load generator
#        — they belong to the broader e2e suite, not to a single-host
#        post-deploy verifier.
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

[[ -n "$IMAGE"     ]] || fail "VELOX_WORKER_IMAGE not set (--image or worker.env)."
[[ -n "$WORKER_ID" ]] || fail "VELOX_WORKER_ID not set (--worker-id or worker.env)."

readonly IMAGE
readonly WORKER_ID
readonly HEALTH_PORT

log "Velox worker VPS checklist verifier v$VERSION"
log "image      : $IMAGE"
log "worker_id  : $WORKER_ID"
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

# ── Run in checklist order ──────────────────────────────────────────────────
section_5_pull
section_6_digest
section_7_worker_env
section_8_certs
section_9_compose
section_10_prepare
section_11_container
section_12_health

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
