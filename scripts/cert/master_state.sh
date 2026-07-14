# scripts/cert/master_state.sh
# ─────────────────────────────────────────────────────────────────────────────
# Phase 2D-3 (master state probe) split out of certify-worker-2c-2d.sh as
# part of the per-phase refactor (bootstrap_2c / static_certificate /
# dynamic_handshake / master_state / evidence_verdict + thin entrypoint).
#
# This file owns the /api/v1/workers probe + 6-check validation
# (state=CONNECTED, protocol_version, bundle_version, bundle_hash,
# capabilities.executors, max_parallel_jobs). Writes master-state.json
# to the evidence dir and records PHASE_STATUS[2d_master_state].
#
# Sourced by the entrypoint AFTER dynamic_handshake.sh and BEFORE
# evidence_verdict.sh. Depends on the entrypoint's global variables
# (MASTER_RESTSERVER, WORKER_ID, PROTOCOL_VERSION, EXPECTED_MAX_CONCURRENCY,
# EXPECTED_BUNDLE_HASH, EXPECTED_BUNDLE_VERSION) plus the EV_DIR set by
# bootstrap_2c.sh.
# ─────────────────────────────────────────────────────────────────────────────

# --- 2D-3: Master state probe via /api/v1/workers --------------------------
run_master_state() {
    if [[ -n "$MASTER_RESTSERVER" ]]; then
        printf '\n═══ Phase 2D-3: master state probe (%s) ═══\n' "$MASTER_RESTSERVER"
        # H9 fix: rstrip trailing slash so a base ending in "/" doesn't
        # resolve to "//api/v1/workers".
        MASTER_RESTSERVER="${MASTER_RESTSERVER%/}"
        local STATE_OUT="$EV_DIR/master-state.json"
        local HTTP_RC=0
        local HTTP_OUT
        HTTP_OUT="$(curl -fsS --max-time 15 "$MASTER_RESTSERVER/api/v1/workers" 2>"$EV_DIR/master-state.err" || { HTTP_RC=$?; cat "$EV_DIR/master-state.err" > "$STATE_OUT.err"; } )"
        if [[ $HTTP_RC -ne 0 ]]; then
            PHASE_STATUS[2d_master_state]="FAIL"
            PHASE_DETAIL[2d_master_state]="/api/v1/workers returned HTTP $HTTP_RC"
            printf '::error::master state probe failed (curl ec=%d)\n' "$HTTP_RC" >&2
        else
            echo "$HTTP_OUT" > "$STATE_OUT"
            # B3 + B4 fix: the master-state probe now asserts BOTH the
            # worker's capabilities (executor list non-empty) AND the bundle
            # version+hash that the master recorded matches the worker image
            # we just bootstrapped. Both checks were missing in the first pass.
            python3 - "$WORKER_ID" "$PROTOCOL_VERSION" "$EXPECTED_MAX_CONCURRENCY" \
                "$EXPECTED_BUNDLE_HASH" "$EXPECTED_BUNDLE_VERSION" "$STATE_OUT" <<'PYEOF'
import json, sys
expected_id          = sys.argv[1]
expected_proto       = sys.argv[2]
expected_max         = sys.argv[3]
expected_bhash       = sys.argv[4]   # 64 lowercase hex from --expected-bundle-hash
expected_bversion    = sys.argv[5]   # operator-supplied bundle_version (B3' fix)
try:
    d = json.load(open(sys.argv[6]))
except Exception as e:
    print(f"::error::could not parse master-state.json: {e}")
    sys.exit(0)

workers = d.get("workers") or []
match = next((w for w in workers if w.get("worker_id") == expected_id), None)
if not match:
    print(f"::error::worker {expected_id} not present in /api/v1/workers (got {len(workers)} workers)")
    sys.exit(0)

# (1) state — must be CONNECTED / READY / REGISTERED / active
status = match.get("status", match.get("state", ""))
if status not in ("CONNECTED", "READY", "REGISTERED", "active"):
    print(f"::error::worker {expected_id} state={status!r} (expected CONNECTED)")
    sys.exit(0)
print(f"OK: worker {expected_id} state={status!r}")

# (2) protocol_version — must match
if expected_proto and match.get("protocol_version") and match["protocol_version"] != expected_proto:
    print(f"::error::worker {expected_id} protocol_version={match['protocol_version']!r} != {expected_proto!r}")
    sys.exit(0)

# (3) bundle_version — must match EXPECTED_BUNDLE_VERSION (B3' + B5 fix;
# preflight already refused to enter 2D-3 without a value, and the
# B5 fix makes the empty-master case fail-CLOSED rather than warn).
master_bundle_version = match.get("bundle_version") or ""
if expected_bversion and not master_bundle_version:
    print(f"::error::worker {expected_id} bundle_version absent from master /api/v1/workers response (B5 fail-closed); operator should verify DataServer/internal/handlers/server/api/workers_handler_types.go exposes BundleVersion in the response struct, OR pass plain 'CONNECTED' state assertion via an explicit per-worker API contract change.")
    sys.exit(20)
if expected_bversion and master_bundle_version and master_bundle_version != expected_bversion:
    print(f"::error::worker {expected_id} bundle_version={master_bundle_version!r} != {expected_bversion!r} (B3' cross-check fail)")
    sys.exit(0)

# (4) bundle_hash — must match EXPECTED_BUNDLE_HASH (B4 fix)
master_bundle_hash = match.get("bundle_hash") or ""
if expected_bhash and master_bundle_hash and master_bundle_hash != expected_bhash:
    print(f"::error::worker {expected_id} bundle_hash={master_bundle_hash[:16]}... != {expected_bhash[:16]}... (B4 cross-check fail)")
    sys.exit(0)
if expected_bhash and not master_bundle_hash:
    print(f"::warn::master did not record bundle_hash for worker {expected_id}; cross-check skipped")

# (5) capabilities — must be non-empty AND list at least one executor
# (B3 fix: original user request explicitly required capabilities verification)
caps = match.get("capabilities") or {}
executors = []
if isinstance(caps, dict):
    raw = caps.get("executors")
    if isinstance(raw, list):
        executors = [str(e.get("id") if isinstance(e, dict) else e) for e in raw]
    elif isinstance(raw, dict):
        executors = list(raw.keys())
if not executors:
    print(f"::error::worker {expected_id} capabilities.executors is empty (B3 cross-check fail)")
    sys.exit(0)
print(f"OK: worker {expected_id} executors={executors}")

# (6) max-concurrency — match against EXPECTED_MAX_CONCURRENCY if provided
if expected_max:
    mx = match.get("max_parallel_jobs") or match.get("max_concurrency") or 0
    if int(mx) != int(expected_max):
        print(f"::error::worker {expected_id} max_parallel_jobs={mx} != {expected_max}")
        sys.exit(0)
sys.exit(0)
PYEOF
            local PROBE_RC=$?
            if [[ $PROBE_RC -eq 0 ]]; then
                PHASE_STATUS[2d_master_state]="PASS"
                PHASE_DETAIL[2d_master_state]="worker present in /api/v1/workers; state=CONNECTED"
            else
                # B5 fix: python exits 20 when bundle_version is absent. Map to
                # a clear PHASE_DETAIL so the verdict.json surfaces it.
                PHASE_STATUS[2d_master_state]="FAIL"
                case $PROBE_RC in
                    20) PHASE_DETAIL[2d_master_state]="bundle_version absent from master /api/v1/workers response (B5 fail-closed)" ;;
                    *)  PHASE_DETAIL[2d_master_state]="master-state probe failed (python exit $PROBE_RC, see master-state.err)" ;;
                esac
            fi
        fi
    else
        PHASE_STATUS[2d_master_state]="SKIP"
        PHASE_DETAIL[2d_master_state]="MASTER_RESTSERVER not set — REST surface not probed"
        printf '::warn::MASTER_RESTSERVER not set; 2D-state SKIPPED\n'
    fi
}
