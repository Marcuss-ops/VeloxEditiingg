# scripts/cert/evidence_verdict.sh
# ─────────────────────────────────────────────────────────────────────────────
# Final evidence + verdict emission split out of certify-worker-2c-2d.sh
# as part of the per-phase refactor (bootstrap_2c / static_certificate /
# dynamic_handshake / master_state / evidence_verdict + thin entrypoint).
#
# This file owns:
#   1. The _phases.json builder (bash associative-array scalar serialization).
#   2. The verdict-2c-2d.json emitter (python heredoc that captures
#      master's observed bundle_version for H11 hardening).
#   3. The H10 verdict.json promotion (combined 2C+2D verdict → verdict.json,
#      preserving narrow 2C-only as verdict.json.2c-original).
#   4. The OVERALL verdict computation (CERTIFIED / PARTIAL / FAIL) with
#      the B2 fail-closed gate for the dynamic-handshake SKIP case.
#   5. The final exit code (0/1/2/3/4 mapped to the phase that FAILed).
#
# Sourced by the entrypoint LAST. Depends on PHASE_STATUS / PHASE_DETAIL
# (set by the 4 phase files) plus the entrypoint's global variables.
# ─────────────────────────────────────────────────────────────────────────────

# --- Final verdict --------------------------------------------------------
run_evidence_verdict() {
    local VERDICT_FILE="$EV_DIR/verdict-2c-2d.json"
    local ANY_FAIL="no"
    local k
    for k in 2c_bootstrap 2d_static_cert 2d_dynamic_handshake 2d_master_state; do
        if [[ "${PHASE_STATUS[$k]}" == "FAIL" ]]; then ANY_FAIL="yes"; break; fi
    done
    local ANY_SKIP="no"
    for k in 2c_bootstrap 2d_static_cert 2d_dynamic_handshake 2d_master_state; do
        if [[ "${PHASE_STATUS[$k]}" == "SKIP" ]]; then ANY_SKIP="yes"; break; fi
    done
    # Required PASSes (fail-closed):
    #   2c_bootstrap + 2d_static_cert      — always required.
    #   2d_dynamic_handshake                — required if MASTER_URL was set
    #                                          (we attempted, must pass). If
    #                                          MASTER_URL is NOT set, dynamic
    #                                          is allowed to SKIP ONLY with
    #                                          explicit --allow-skip-dynamic
    #                                          opt-in (B2 fix).
    #   2d_master_state                     — required if MASTER_RESTSERVER
    #                                          was set; otherwise optional.
    local REQUIRED_PASS="yes"
    for k in 2c_bootstrap 2d_static_cert; do
        if [[ "${PHASE_STATUS[$k]}" != "PASS" ]]; then REQUIRED_PASS="no"; break; fi
    done
    # Dynamic-handshake requirement gate (B2)
    if [[ "${PHASE_STATUS[2d_dynamic_handshake]}" != "PASS" ]]; then
        if [[ -n "$MASTER_URL" ]]; then
            # We had a master to probe — must pass.
            REQUIRED_PASS="no"
            [[ "${PHASE_STATUS[2d_dynamic_handshake]}" == "FAIL" ]] || \
                PHASE_DETAIL[2d_dynamic_handshake]="${PHASE_DETAIL[2d_dynamic_handshake]} (auto-promoted to FAIL: MASTER_URL set but no PASS)"
        elif [[ "$ALLOW_SKIP_DYNAMIC" != "true" ]]; then
            # No master to probe AND operator didn't opt-in to skip → fail-closed.
            REQUIRED_PASS="no"
            PHASE_STATUS[2d_dynamic_handshake]="FAIL"
            PHASE_DETAIL[2d_dynamic_handshake]="MASTER_URL not set; refactor to pass --master-url or --allow-skip-dynamic (B2 fail-closed)"
        fi
    fi
    # Master-state requirement gate
    if [[ "${PHASE_STATUS[2d_master_state]}" != "PASS" && "${PHASE_STATUS[2d_master_state]}" != "SKIP" ]]; then
        # 2d_master_state already was FAIL. If MASTER_RESTSERVER was set, this
        # is a real probe failure → fail-closed.
        [[ -n "$MASTER_RESTSERVER" && "${PHASE_STATUS[2d_master_state]}" == "FAIL" ]] && REQUIRED_PASS="no"
    fi

    local OVERALL="CERTIFIED"
    if [[ "$REQUIRED_PASS" == "no" ]]; then OVERALL="FAIL"; fi
    if [[ "$ANY_FAIL" == "yes" && "$OVERALL" != "FAIL" ]]; then OVERALL="PARTIAL"; fi

    # H2 fix: the dead scaffold heredoc python3 - "...import sys" PYEOF
    # block is REMOVED in this round (no-op; ~30 ms wasted startup on
    # every certifier run). The canonical verdict emitter downstream reads
    # the side-band _phases.json produced by the bash block below.

    # Build the phases JSON via bash associative-array scalar serialization.
    {
        echo "  \"phases\": {"
        local first=true
        for k in 2c_bootstrap 2d_static_cert 2d_dynamic_handshake 2d_master_state; do
            [[ "$first" == "true" ]] && first=false || echo ","
            printf '    "%s": { "status": "%s", "detail": "%s" }' \
                "$k" "${PHASE_STATUS[$k]}" "${PHASE_DETAIL[$k]}"
        done
        echo "  }"
    } > "$EV_DIR/_phases.json"

    python3 - "$WORKER_ID" "$CERT_DATE" "$WORKER_IMAGE" "$EXPECTED_BUNDLE_HASH" \
            "$EXPECTED_BUNDLE_VERSION" "$PROTOCOL_VERSION" "$OVERALL" "$ANY_FAIL" \
            "$ANY_SKIP" "$REQUIRED_PASS" "$EV_DIR" "$EV_DIR/_phases.json" \
            "$STATE_OUT_OR_EMPTY" "$VERDICT_FILE" \
            <<'PYEOF'
import json, sys, time, os
(worker_id, cert_date, worker_image, bundle_hash, expected_bversion, proto,
 overall, any_fail, any_skip, required_pass, evid_dir, phases_path,
 state_path, verdict_path) = sys.argv[1:14]
phases = json.load(open(phases_path))

# H11 fix: capture master's observed bundle_version so verifiers have
# a non-empty record even on FAIL or partial fields.
master_observed_bundle_version = ""
try:
    if state_path and os.path.exists(state_path):
        d_master = json.load(open(state_path))
        for w in d_master.get("workers") or []:
            if w.get("worker_id") == worker_id:
                master_observed_bundle_version = w.get("bundle_version") or ""
                break
except Exception:
    master_observed_bundle_version = ""

out = {
    "schema":                          "velox.phase-2c-2d.v2",
    "worker_id":                       worker_id,
    "cert_date":                       cert_date,
    "worker_image":                    worker_image,
    "expected_bundle_hash":            bundle_hash,
    "expected_bundle_version":         expected_bversion,
    "master_observed_bundle_version":  master_observed_bundle_version,
    "protocol_version":                proto,
    "phase_status":                    phases,
    "overall_verdict":                 overall,
    "any_fail":                        any_fail == "yes",
    "any_skip":                        any_skip == "yes",
    "required_passes":                 required_pass == "yes",
    "evidence_dir":                    evid_dir,
    "generated_at_utc":                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
with open(verdict_path, "w") as f:
    json.dump(out, f, indent=2, sort_keys=True)
print(json.dumps(out, indent=2, sort_keys=True))
PYEOF
    rm -f "$EV_DIR/_phases.json"

    # H10 fix: real-bootstrap.sh writes a NARROW `verdict.json` to EV_DIR
    # (only 2C scope), and the cap. 2C+2D generic verdict emitter here writes
    # `verdict-2c-2d.json` (combined 2C+2D scope). The cap. 11 collector
    # expects ONE canonical verdict per (date, worker). We promote the
    # combined verdict → verdict.json AND preserve the narrow 2C-only
    # verdict as `verdict.json.2c-original` so historical dashboards plotting
    # "the original 2C-only verdict over time" don't lose granularity.
    if [[ -r "$EV_DIR/verdict-2c-2d.json" ]]; then
        # If real-bootstrap.sh already wrote a verdict.json (the 2C-only
        # narrow verdict), preserve it before promoting the combined verdict.
        if [[ -r "$EV_DIR/verdict.json" ]]; then
            cp "$EV_DIR/verdict.json" "$EV_DIR/verdict.json.2c-original"
            printf '→ preserved narrow 2C verdict: %s\n' "$EV_DIR/verdict.json.2c-original"
        fi
        mv "$EV_DIR/verdict-2c-2d.json" "$EV_DIR/verdict.json"
        VERDICT_FILE="$EV_DIR/verdict.json"
        printf '→ canonical (combined 2C+2D) verdict promoted to: %s\n' "$VERDICT_FILE"
    fi

    # ─── Final exit ────────────────────────────────────────────────────────
    printf '\n═══ verdict ═══\n'
    cat "$VERDICT_FILE" | sed -n '/"overall_verdict"/,/}/p' | head -5 || true
    printf '\n→ evidence dir: %s\n' "$EV_DIR"

    case "$OVERALL" in
        CERTIFIED)  printf '\n✓ Phase 2C+2D CERTIFIED — worker is fully bootable + handshakeable\n'; exit 0 ;;
        PARTIAL)    printf '\n::warn::Phase 2C+2D PARTIAL — some non-required sub-phases failed\n'; exit 0 ;;  # operator may still want to proceed if non-required skipped
        *)          printf '\n::error::Phase 2C+2D FAIL — see %s\n' "$VERDICT_FILE" >&2
                    case "${PHASE_STATUS[2c_bootstrap]}" in
                        FAIL) exit 1 ;;
                    esac
                    case "${PHASE_STATUS[2d_static_cert]}" in
                        FAIL) exit 2 ;;
                    esac
                    case "${PHASE_STATUS[2d_dynamic_handshake]}" in
                        FAIL) exit 3 ;;
                    esac
                    case "${PHASE_STATUS[2d_master_state]}" in
                        FAIL) exit 4 ;;
                    esac
                    exit 1
                    ;;
    esac
}
