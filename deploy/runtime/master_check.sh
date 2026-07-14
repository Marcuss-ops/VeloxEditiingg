# deploy/runtime/master_check.sh
# ─────────────────────────────────────────────────────────────────────────────
# Section 14 (master sees CONNECTED worker) split out of checklist-verify.sh
# as part of the per-category refactor (sections 5-9 / deploy / security /
# master_check / canary + lib/common.sh). This is the only section that
# talks to the master HTTP API, so it lives in its own file to make the
# network dependency explicit.
#
# Sourced by the orchestrator AFTER security.sh and BEFORE canary.sh.
# Depends on lib/common.sh for section_header, record, vrb and on the
# orchestrator's MASTER + MASTER_API + WORKER_ID globals (set during
# run_preconditions).
# ─────────────────────────────────────────────────────────────────────────────

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
