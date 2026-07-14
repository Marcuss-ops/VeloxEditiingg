# deploy/runtime/security.sh
# ─────────────────────────────────────────────────────────────────────────────
# Post-deploy health and security checks split out of checklist-verify.sh as
# part of the per-category refactor (sections 5-9 / deploy / security /
# master_check / canary + lib/common.sh). These three sections run AFTER
# the worker is deployed and verify runtime state: container health, HTTP
# readiness/liveness, and log-scrub for forbidden tokens.
#
# Sourced by the orchestrator AFTER deploy.sh and BEFORE master_check.sh.
# Depends on lib/common.sh for section_header, record, vrb and on the
# orchestrator's WORKER_ID + HEALTH_PORT globals.
# ─────────────────────────────────────────────────────────────────────────────

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
