#!/usr/bin/env bash
# deploy/runtime/checklist-verify.sh
# ─────────────────────────────────────────────────────────────────────────────
# Thin orchestrator for the Velox worker VPS deployment checklist. After the
# per-category refactor, this file is intentionally minimal:
#
#   1. Parse CLI args (sets IMAGE / WORKER_ID / MASTER / MASTER_API /
#      SKIP_DEPLOY / JSON_OUT / VERBOSE globals).
#   2. Source lib/common.sh (logging, result aggregation, pre-conditions,
#      summary, JSON output, exit code).
#   3. Source each per-section file (sections_5_to_9.sh, deploy.sh,
#      security.sh, master_check.sh, canary.sh).
#   4. Run run_preconditions to validate the host environment and resolve
#      IMAGE / WORKER_ID / MASTER from /etc/velox-worker/worker.env.
#   5. Invoke every section function in checklist order.
#   6. Print the summary table, emit the optional JSON summary, and exit.
#
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
#              deploy/runtime/canary.sh (which wraps submit-canary.sh) and
#              bridges its exit code (0=PASS, 1=FAIL, 255=SKIP) to the
#              record() pattern. Section 15 SKIPs only when opt-in
#              prerequisites (VELOX_ADMIN_TOKEN, VELOX_DB_PATH, a running
#              worker container) are missing — a missing render-required
#              tool (ffmpeg, ffprobe, sqlite3) is FAIL, NOT SKIP, because
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

readonly SCRIPT_NAME="$(basename "$0")"
# Sibling-files directory: every sourced file lives next to the orchestrator
# so the deployment package can be relocated as a unit (e.g. copied to
# /opt/velox-worker without breaking relative includes).
readonly CHECKLIST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)"

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

# ── Source shared library + per-section files ───────────────────────────────
# lib/common.sh provides log/ok/warn/fail/vrb, SECTION_* arrays, record,
# section_header, run_preconditions, print_summary, emit_json_summary,
# finalize_exit. Each per-section file defines one or more section_*
# functions that depend on the symbols above.
#
# Sourcing order matters: common.sh MUST come first (it defines the
# symbols every section uses); the per-section files can be sourced in
# any order because they only DEFINE functions (they don't call each
# other). We source them in checklist order for readability.
source "$CHECKLIST_DIR/lib/common.sh"
source "$CHECKLIST_DIR/sections_5_to_9.sh"
source "$CHECKLIST_DIR/deploy.sh"
source "$CHECKLIST_DIR/security.sh"
source "$CHECKLIST_DIR/master_check.sh"
source "$CHECKLIST_DIR/canary.sh"

# ── Pre-conditions: validate host env, resolve IMAGE/WORKER_ID/MASTER ───────
run_preconditions

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
print_summary

# ── Optional JSON summary ──────────────────────────────────────────────────
if [[ -n "$JSON_OUT" ]]; then
    emit_json_summary "$JSON_OUT"
fi

# ── Exit code ──────────────────────────────────────────────────────────────
finalize_exit
