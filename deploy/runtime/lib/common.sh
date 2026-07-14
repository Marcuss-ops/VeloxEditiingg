# deploy/runtime/lib/common.sh
# ─────────────────────────────────────────────────────────────────────────────
# Shared library for checklist-verify.sh and its per-section siblings. Sourced
# by the orchestrator (checklist-verify.sh) after argument parsing. Every
# section function (`section_5_pull`, `section_10_prepare`, etc.) depends on
# the symbols defined here:
#
#   Logging           : log / ok / warn / fail / vrb (TTY-aware coloring)
#   Result aggregation: SECTION_IDS / SECTION_TITLES / SECTION_STATUS /
#                       SECTION_DETAILS arrays + record() helper
#   Section framing   : section_header() prints the bold "── Section N: ... ──"
#   Pre-conditions    : run_preconditions() validates root, tools, env file,
#                       resolves IMAGE / WORKER_ID / MASTER, declares them
#                       readonly, and emits the initial log banner
#   Summary           : print_summary() + emit_json_summary() +
#                       finalize_exit() — the three terminal calls the
#                       orchestrator makes after running every section
#
# Sourcing contract: this file is safe to source multiple times (idempotent
# because all symbols are defined, not re-declared). The orchestrator sources
# it once, then sources each per-section file in order.
# ─────────────────────────────────────────────────────────────────────────────

# Version is referenced by the orchestrator's log banner and the JSON summary.
# Centralized here so all section files report a single canonical version.
readonly VERSION="1.0.0"

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
vrb()  { [[ "${VERBOSE:-0}" -eq 1 ]] && printf '       %s\n' "$*" || true; }

# ── Result aggregation ───────────────────────────────────────────────────────
# 4 parallel arrays keyed by position. Tiny list, no need for hash maps.
declare -a SECTION_IDS=()
declare -a SECTION_TITLES=()
declare -a SECTION_STATUS=()    # PASS | FAIL | SKIP
declare -a SECTION_DETAILS=()

# Totals populated by print_summary() and consumed by emit_json_summary() +
# finalize_exit(). Initialized here so the contract is explicit even if
# print_summary() is somehow bypassed (e.g. a future pre-condition fail
# before the summary phase runs).
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
TOTAL=0

record() {
    SECTION_IDS+=("$1")
    SECTION_TITLES+=("$2")
    SECTION_STATUS+=("$3")
    SECTION_DETAILS+=("${4:-}")
}

section_header() {
    printf '\n%s── Section %s: %s ──%s\n' "$BOLD" "$1" "$2" "$NC"
}

# ── Pre-conditions ───────────────────────────────────────────────────────────
# Called by the orchestrator AFTER argument parsing and BEFORE sourcing the
# section files. Resolves IMAGE / WORKER_ID / MASTER from CLI flags and
# /etc/velox-worker/worker.env, validates the host environment, declares
# everything readonly, and emits the initial log banner.
#
# Exits 2 on any pre-condition failure (via the fail() helper above).
run_preconditions() {
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
}

# ── Summary table ───────────────────────────────────────────────────────────
print_summary() {
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

    # Stash totals for finalize_exit / emit_json_summary via globals.
    PASS_COUNT=$pass_count
    FAIL_COUNT=$fail_count
    SKIP_COUNT=$skip_count
    TOTAL=$total
}

# ── Optional JSON summary ──────────────────────────────────────────────────
# Emits the per-section records through jq → NDJSON file → `jq -s --slurpfile`.
# jq universally accepts NDJSON (newline-separated JSON values); comma-
# separated streams are NOT reliably accepted across jq versions — v1's
# comma-join emitter failed with `parse error: Expected value before ','`
# against the verifier's jq. NDJSON is portable.
emit_json_summary() {
    local json_out="$1"
    local ndjson
    ndjson="$(mktemp /tmp/velox-checklist-sections.XXXXXX.ndjson)"
    trap 'rm -f "${ndjson:-}"' EXIT

    local i=0
    for status in "${SECTION_STATUS[@]}"; do
        local sid="${SECTION_IDS[$i]}"
        local title="${SECTION_TITLES[$i]}"
        local detail="${SECTION_DETAILS[$i]}"
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
        --argjson pass  "$PASS_COUNT" \
        --argjson fail  "$FAIL_COUNT" \
        --argjson skipc "$SKIP_COUNT" \
        --argjson total "$TOTAL" \
        '{
            version:     $version,
            image:       $image,
            worker_id:   $worker_id,
            skip_deploy: $skip,
            sections:    $sections,
            summary:     {pass: $pass, fail: $fail, skip: $skipc, total: $total}
        }' > "$json_out"

    rm -f "$ndjson"
    trap - EXIT
    log "JSON summary written to $json_out"
}

# ── Exit code ──────────────────────────────────────────────────────────────
finalize_exit() {
    if [[ "${FAIL_COUNT:-0}" -gt 0 ]]; then
        exit 1
    fi
    exit 0
}
