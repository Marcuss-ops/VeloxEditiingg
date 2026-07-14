# deploy/runtime/deploy.sh
# ─────────────────────────────────────────────────────────────────────────────
# Section 10 (prepare-host.sh) split out of checklist-verify.sh as part of
# the per-category refactor (sections 5-9 / deploy / security / master_check /
# canary + lib/common.sh). The deploy step is the only one that triggers
# host mutation (docker compose up), so it lives in its own file to make
# the side-effect surface explicit.
#
# Sourced by the orchestrator AFTER sections_5_to_9.sh and BEFORE
# security.sh. Depends on lib/common.sh for section_header, record, vrb,
# and on the orchestrator's SKIP_DEPLOY global (read-only mode).
# ─────────────────────────────────────────────────────────────────────────────

# Canonical search paths for prepare-host.sh, ordered by install convention:
#   1. /opt/velox-worker/prepare-host.sh  — canonical install location
#   2. /usr/local/bin/...                — system-wide alt install
#   3. <repo>/deploy/runtime/...         — dev / CI runs
prepare_host_search_paths=(
    /opt/velox-worker/prepare-host.sh
    /usr/local/bin/velox-worker-prepare-host.sh
    /root/VeloxLEgit/deploy/runtime/prepare-host.sh
)

# ═════════════════════════════════════════════════════════════════════════════
# Section 10 — Prepare host (deployment)
# ═════════════════════════════════════════════════════════════════════════════
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
