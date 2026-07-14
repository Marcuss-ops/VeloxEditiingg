# scripts/cert/bootstrap_2c.sh
# ─────────────────────────────────────────────────────────────────────────────
# Phase 2C (real bootstrap) split out of certify-worker-2c-2d.sh as part
# of the per-phase refactor (bootstrap_2c / static_certificate /
# dynamic_handshake / master_state / evidence_verdict + thin entrypoint).
#
# This file owns:
#   1. Pre-flight sanity checks (arg validation, file readability,
#      HANDSHAKE_TIMEOUT_S floor, B3' preflight for master-state probe).
#   2. Path resolution (CERTIFIER_DIR, REAL_BOOTSTRAP, REPO_ROOT,
#      EVIDENCE_ROOT, CERT_DATE, EV_DIR, STATE_OUT_OR_EMPTY).
#   3. The 2C real-bootstrap call (delegates to real-bootstrap.sh).
#   4. H1+H8 dump.txt / bootstrap-report.json folding on FAIL.
#   5. H4 bundle_hash cross-check against EXPECTED_BUNDLE_HASH.
#
# Sourced by the entrypoint AFTER arg parsing. Depends on the entrypoint's
# global variables (WORKER_ID, WORKER_IMAGE, EXPECTED_BUNDLE_HASH, etc.)
# and the PHASE_STATUS / PHASE_DETAIL associative arrays.
# ─────────────────────────────────────────────────────────────────────────────

# --- Pre-flight sanity checks ---------------------------------------------
# These run BEFORE the 2C bootstrap so a bad input fails fast with a clear
# error rather than mid-bootstrap with a confusing real-bootstrap.sh error.
run_preflight() {
    [[ -n "$WORKER_ID"           ]] || { printf '::error::--worker-id is required\n' >&2; exit 1; }
    [[ -n "$WORKER_IMAGE"        ]] || { printf '::error::--worker-image is required\n' >&2; exit 1; }
    [[ -n "$EXPECTED_BUNDLE_HASH" ]] || { printf '::error::--expected-bundle-hash is required\n' >&2; exit 1; }
    [[ -n "$WORKER_CERT_FILE"    ]] || { printf '::error::--worker-cert-file is required\n' >&2; exit 1; }
    [[ -n "$WORKER_KEY_FILE"     ]] || { printf '::error::--worker-key-file is required\n' >&2; exit 1; }
    [[ -n "$WORKER_CA_FILE"      ]] || { printf '::error::--worker-ca-file is required\n' >&2; exit 1; }

    # Refuse non-digest pin (re-use real-bootstrap.sh invariant).
    if ! [[ "$WORKER_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]; then
        printf '::error::--worker-image must be a digest pin (got: %s) — never :latest\n' \
            "$WORKER_IMAGE" >&2; exit 1
    fi
    if [[ ! "$EXPECTED_BUNDLE_HASH" =~ ^[a-f0-9]{64}$ ]]; then
        printf '::error::--expected-bundle-hash must be 64 lowercase hex (got %d chars)\n' \
            "${#EXPECTED_BUNDLE_HASH}" >&2; exit 1
    fi

    local f
    for f in "$WORKER_CERT_FILE" "$WORKER_KEY_FILE" "$WORKER_CA_FILE"; do
        [[ -r "$f" ]] || { printf '::error::file not readable: %s\n' "$f" >&2; exit 2; }
    done

    # H3 fix: dev-hello-client's internal HelloAckTimeout is hardcoded
    # to 15s. If operator passes --handshake-timeout-s < 15, the outer
    # `timeout` wrapper would fire first, masking the actual handshake
    # failure with a misleading "timed out". Fail-fast on sub-15s.
    if (( HANDSHAKE_TIMEOUT_S < 15 )); then
        printf '::error::--handshake-timeout-s must be >= 15 (dev-hello-client internal floor); got %d\n' \
            "$HANDSHAKE_TIMEOUT_S" >&2
        exit 2
    fi

    # B3' preflight: when operator probes the master's REST surface (2D-3),
    # require EXPLICIT assertion of bundle_version. This closes the
    # user-request gap "bundle/versione corretta" — without this gate a
    # negligent operator could trigger 2D-3 just to verify CN/ConnectED,
    # missing the bundle_version half.
    if [[ -n "$MASTER_RESTSERVER" && -z "$EXPECTED_BUNDLE_VERSION" ]]; then
        printf '::error::--master-restserver is set but --expected-bundle-version is empty; pass --expected-bundle-version (the published worker image VERSION.txt, e.g. 1.2.3) to assert bundle/versione on the master-state probe.\n' >&2
        exit 2
    fi
}

# --- Path resolution -------------------------------------------------------
# CERTIFIER_DIR is set by the entrypoint (it's the entrypoint's own
# directory, NOT BASH_SOURCE[0] of this file — that would be fragile if
# bootstrap_2c.sh is ever relocated). REAL_BOOTSTRAP and REPO_ROOT are
# resolved here because they're bootstrap-specific concerns.
resolve_paths() {
    REAL_BOOTSTRAP="$CERTIFIER_DIR/real-bootstrap.sh"
    [[ -r "$REAL_BOOTSTRAP" ]] || { printf '::error::missing real-bootstrap.sh at %s\n' \
        "$REAL_BOOTSTRAP" >&2; exit 2; }

    # B1 fix (H7 hardening): do NOT hardcode /home/pierone/Pyt/VeloxLEgit.
    # Primary resolution is `git -C "$CERTIFIER_DIR" rev-parse --show-toplevel`
    # (works for symlinked + copy-installed planners), falling back to
    # `realpath "$CERTIFIER_DIR/../.."` so the certifier works on any host
    # where the repo is checked out (operator-side VPS, CI runner).
    if command -v git >/dev/null 2>&1; then
        REPO_ROOT="$(git -C "$CERTIFIER_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
    fi
    if [[ -z "$REPO_ROOT" || ! -d "$REPO_ROOT" ]]; then
        REPO_ROOT="$(realpath "$CERTIFIER_DIR/../.." 2>/dev/null || true)"
    fi
    if [[ ! -d "$REPO_ROOT/DataServer/cmd/dev-hello-client" ]]; then
        printf '::error::cannot resolve repo root (DataServer/cmd/dev-hello-client not under %s); the certifier requires the Velox repo to be checked out as a sibling of scripts/cert/\n' \
            "$REPO_ROOT" >&2
        exit 2
    fi
    printf '→ repo root resolved: %s\n' "$REPO_ROOT"

    EVIDENCE_ROOT="${EVIDENCE_ROOT:-$HOME/evidence}"
    CERT_DATE="${CERT_DATE:-$(date -u +%Y-%m-%d)}"
    EV_DIR="$EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID"
    mkdir -p "$EV_DIR"
    # Used by the verdict-emit python heredoc to surface
    # master_observed_bundle_version (H11 hardening).
    STATE_OUT_OR_EMPTY="$EV_DIR/master-state.json"
}

# --- 2C: Real Bootstrap ---------------------------------------------------
run_bootstrap_2c() {
    run_preflight
    resolve_paths

    printf '\n═══ Phase 2C: real bootstrap certifier ═══\n'
    if WORKER_IMAGE="$WORKER_IMAGE" \
       EXPECTED_BUNDLE_HASH="$EXPECTED_BUNDLE_HASH" \
       WORKER_ID="$WORKER_ID" \
       CERT_DATE="$CERT_DATE" \
       EVIDENCE_ROOT="$EVIDENCE_ROOT" \
       bash "$REAL_BOOTSTRAP"; then
        PHASE_STATUS[2c_bootstrap]="PASS"
        PHASE_DETAIL[2c_bootstrap]="real-bootstrap run PASS; verdict=OK + 4 step PASS"
        # real-bootstrap.sh saved bootstrap-report.json. Alias container-{stdout,stderr}
        # → worker.log per cap. 3 schema.
        if [[ -r "$EV_DIR/container-stdout.log" ]]; then
            {
                echo "═══ container stdout ═══"
                cat "$EV_DIR/container-stdout.log"
                echo
                echo "═══ container stderr (postgres; includes [BOOTSTRAP_REPORT] block) ═══"
                [[ -r "$EV_DIR/container-stderr.log" ]] && cat "$EV_DIR/container-stderr.log"
            } > "$EV_DIR/worker.log"
            printf '→ wrote %s/worker.log (combined stdout+stderr)\n' "$EV_DIR"
        else
            printf '::warn::no container-stdout.log; relying on bootstrap-report.json alone\n'
            : > "$EV_DIR/worker.log"  # zero-byte placeholder so verifier gate sees the canonical file
        fi
        # H4 fix: re-assert the bundle_hash from bootstrap-report.json
        # against EXPECTED_BUNDLE_HASH. pin-worker-digest.sh enforces the
        # upstream pinning; this is a per-worker cross-check that catches
        # registry-vs-on-disk divergence on the VPS specifically.
        if [[ -r "$EV_DIR/bootstrap-report.json" ]]; then
            actual_hash="$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('bundle_hash',''))" "$EV_DIR/bootstrap-report.json")"
            if [[ -n "$actual_hash" && "$actual_hash" != "$EXPECTED_BUNDLE_HASH" ]]; then
                PHASE_STATUS[2c_bootstrap]="FAIL"
                PHASE_DETAIL[2c_bootstrap]="bundle_hash cross-check FAIL: bootstrap-report.bundle_hash=$actual_hash != EXPECTED_BUNDLE_HASH=$EXPECTED_BUNDLE_HASH"
                printf '::error::2C FAIL: bundle_hash cross-check (got %s, expected %s)\n' \
                    "$actual_hash" "$EXPECTED_BUNDLE_HASH" >&2
            elif [[ -n "$actual_hash" ]]; then
                PHASE_DETAIL[2c_bootstrap]="${PHASE_DETAIL[2c_bootstrap]} + bundle_hash verified"
            fi
        fi
    else
        PHASE_STATUS[2c_bootstrap]="FAIL"
        local rc=$?
        case $rc in
            4) PHASE_DETAIL[2c_bootstrap]="real-bootstrap timed out (60s)" ;;
            5) PHASE_DETAIL[2c_bootstrap]="no [BOOTSTRAP_REPORT] block found in container stderr" ;;
            7) PHASE_DETAIL[2c_bootstrap]="bootstrap verdict != OK or step != OK (see bootstrap-report.json)" ;;
            *) PHASE_DETAIL[2c_bootstrap]="real-bootstrap exit $rc" ;;
        esac
        printf '::error::2C FAIL: %s\n' "${PHASE_DETAIL[2c_bootstrap]}" >&2
        # We continue into 2D so the operator gets the full shape; verdict.json reflects 2C overall.
    fi

    # H1 + H8 fix: in BOTH the partial-FAIL case (container ran but
    # verdict != OK — worker.log contains stdout+stderr, dump.txt is
    # written but NOT folded) AND the early-FAIL case (real-bootstrap
    # died on bad WORKER_IMAGE — worker.log is missing entirely),
    # append the canonical dump.txt + bootstrap-report.json to worker.log
    # so the cap. 11 collector sees the full failure context.
    if [[ "${PHASE_STATUS[2c_bootstrap]}" == "FAIL" ]]; then
        {
            echo
            echo "═══ real-bootstrap.sh dump.txt (debug surface; 2C FAIL) ═══"
            [[ -r "$EV_DIR/dump.txt" ]] && cat "$EV_DIR/dump.txt" || echo "(no dump.txt on disk)"
            echo
            echo "═══ bootstrap-report.json (verbatim; if present) ═══"
            [[ -r "$EV_DIR/bootstrap-report.json" ]] && cat "$EV_DIR/bootstrap-report.json" || echo "(no bootstrap-report.json on disk)"
        } >> "$EV_DIR/worker.log"
    fi
}
