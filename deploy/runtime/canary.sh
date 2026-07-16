# deploy/runtime/canary.sh
# ─────────────────────────────────────────────────────────────────────────────
# Section 15 (E2E Canary SUCCEEDED) split out of checklist-verify.sh as part
# of the per-category refactor (sections 5-9 / deploy / security /
# master_check / canary + lib/common.sh). This section delegates to the
# local canary (submit-canary-local.sh), which assumes a co-located worker
# and uses docker exec + file:// fixtures + direct SQLite access. For a
# remote topology use submit-canary-remote.sh instead; it is intentionally
# NOT wired into the checklist because it requires real remote assets and
# no shared filesystem.
#
# Sourced by the orchestrator AFTER master_check.sh (the last section).
# Depends on lib/common.sh for section_header, record, vrb and on the
# orchestrator's MASTER + WORKER_ID globals.
# ─────────────────────────────────────────────────────────────────────────────

# ═════════════════════════════════════════════════════════════════════════════
# Section 15 — E2E Canary SUCCEEDED (delegates to submit-canary-local.sh)
# ═════════════════════════════════════════════════════════════════════════════
# Bridges deploy/runtime/submit-canary-local.sh's exit-code contract to the
# record() pattern:
#
#   rc == 0    → PASS  (detail = "PASS:" sentinel line)
#   rc == 255  → SKIP  (detail = "SKIP:" sentinel — pre-flight gate)
#   rc != 0    → FAIL  (detail = "FAIL:" sentinel)
#
# submit-canary-local.sh DOES NOT require a live master gRPC session — §14 has
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

    # Locate submit-canary-local.sh in canonical deploy locations. Mirror the
    # search strategy used by section_10 for prepare-host.sh.
    #
    # BASH_SOURCE[1] is this file (canary.sh) when sourced by the
    # orchestrator; BASH_SOURCE[0] is the orchestrator. When the file
    # is run directly (e.g. `bash canary.sh` for debugging), BASH_SOURCE[1]
    # is unset and we fall back to BASH_SOURCE[0]. This avoids the
    # fragility of resolving against the orchestrator's directory if
    # canary.sh is ever relocated to a subdirectory.
    local script=""
    local p
    local _canary_dir
    _canary_dir="$(cd "$(dirname "${BASH_SOURCE[1]:-${BASH_SOURCE[0]}}")" 2>/dev/null && pwd)"
    for p in \
        /opt/velox-worker/submit-canary-local.sh \
        /usr/local/bin/velox-submit-canary-local.sh \
        "${VELOXWORK_CHECKLIST_PARENT:-}/submit-canary-local.sh" \
        "${_canary_dir}/submit-canary-local.sh"; do
        if [[ -r "$p" ]]; then
            script="$p"
            break
        fi
    done
    if [[ -z "$script" ]]; then
        record 15 "E2E Canary SUCCEEDED" SKIP \
            "submit-canary-local.sh not found in any of: /opt/velox-worker, /usr/local/bin, repo deploy/runtime, beside the verifier"
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
