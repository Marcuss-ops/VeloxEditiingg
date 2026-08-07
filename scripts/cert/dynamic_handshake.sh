# scripts/cert/dynamic_handshake.sh
# ─────────────────────────────────────────────────────────────────────────────
# Phase 2D-2 (dynamic handshake probe) split out of certify-worker-2c-2d.sh
# as part of the per-phase refactor (bootstrap_2c / static_certificate /
# dynamic_handshake / master_state / evidence_verdict + thin entrypoint).
#
# This file owns the dev-hello-client build + run + HelloAck verification
# against the master gRPC endpoint. Writes master-handshake.log and
# appends the worker stream to worker.log. Records
# PHASE_STATUS[2d_dynamic_handshake].
#
# Sourced by the entrypoint AFTER static_certificate.sh and BEFORE
# master_state.sh. Depends on the entrypoint's global variables
# (MASTER_URL, WORKER_ID, PROTOCOL_VERSION, WORKER_CERT_FILE,
# WORKER_KEY_FILE, WORKER_CA_FILE, HANDSHAKE_TIMEOUT_S) plus the
# REPO_ROOT and EV_DIR set by bootstrap_2c.sh.
# ─────────────────────────────────────────────────────────────────────────────

# --- 2D-2: Dynamic handshake probe via dev-hello-client ------------------
run_dynamic_handshake() {
    if [[ -n "$MASTER_URL" ]]; then
        printf '\n═══ Phase 2D-2: dynamic handshake probe ═══\n'
        local DEV_HELLO_BIN="$EV_DIR/dev-hello-client"
        # We compile it inline so the certifier works even if the host has no
        # previously-built binary at $GOPATH/bin. This adds ~10s but removes
        # dependency on a shared build cache.
        if ! command -v go >/dev/null 2>&1; then
            PHASE_STATUS[2d_dynamic_handshake]="FAIL"
            PHASE_DETAIL[2d_dynamic_handshake]="go toolchain not available; cannot compile dev-hello-client"
            printf '::error::go toolchain missing; cannot run 2D-2\n' >&2
        elif ! (cd "$REPO_ROOT/DataServer" && \
                  go build -o "$DEV_HELLO_BIN" ./cmd/dev-hello-client) 2>"$EV_DIR/dev-hello-build.log"; then
            PHASE_STATUS[2d_dynamic_handshake]="FAIL"
            PHASE_DETAIL[2d_dynamic_handshake]="dev-hello-client build failed (see dev-hello-build.log)"
            printf '::error::dev-hello-client build failed (see %s)\n' "$EV_DIR/dev-hello-build.log" >&2
        else
            local HANDSHAKE_LOG="$EV_DIR/master-handshake.log"
            : > "$HANDSHAKE_LOG"
            local HANDSHAKE_RC=0
            if command -v timeout >/dev/null 2>&1; then
                timeout "$HANDSHAKE_TIMEOUT_S"s "$DEV_HELLO_BIN" \
                    --master "$MASTER_URL" \
                    --worker-id "$WORKER_ID" \
                    --worker-name "certifier-$(date -u +%H%M%S)" \
                    --protocol-version "$PROTOCOL_VERSION" \
                    --tls-cert "$WORKER_CERT_FILE" \
                    --tls-key  "$WORKER_KEY_FILE" \
                    --tls-ca   "$WORKER_CA_FILE" \
                    --heartbeat-window=10s \
                    --heartbeat-interval=5s \
                    > "$EV_DIR/handshake-worker-stdout.log" \
                    2> "$EV_DIR/handshake-worker-stderr.log" || HANDSHAKE_RC=$?
            else
                "$DEV_HELLO_BIN" \
                    --master "$MASTER_URL" \
                    --worker-id "$WORKER_ID" \
                    --worker-name "certifier-$(date -u +%H%M%S)" \
                    --protocol-version "$PROTOCOL_VERSION" \
                    --tls-cert "$WORKER_CERT_FILE" \
                    --tls-key  "$WORKER_KEY_FILE" \
                    --tls-ca   "$WORKER_CA_FILE" \
                    --heartbeat-window=10s \
                    --heartbeat-interval=5s \
                    > "$EV_DIR/handshake-worker-stdout.log" \
                    2> "$EV_DIR/handshake-worker-stderr.log" || HANDSHAKE_RC=$?
            fi
            # dev-hello-client's PR 2 logic: exit 0 iff handshake
            # completed cleanly (HelloAck received + Goodbye + localCancel)
            # AND no terminal recv err after handshake phase.
            if [[ $HANDSHAKE_RC -eq 0 ]] && \
               grep -q '✓ HelloAck' "$EV_DIR/handshake-worker-stderr.log"; then
                PHASE_STATUS[2d_dynamic_handshake]="PASS"
                PHASE_DETAIL[2d_dynamic_handshake]="HelloAck received within ${HANDSHAKE_TIMEOUT_S}s; CN+cert authenticated"
            else
                PHASE_STATUS[2d_dynamic_handshake]="FAIL"
                PHASE_DETAIL[2d_dynamic_handshake]="dev-hello-client exit=$HANDSHAKE_RC (no ✓ HelloAck found)"
                printf '::error::2D-dynamic FAIL: dev-hello-client returned %d\n' "$HANDSHAKE_RC" >&2
            fi
            # Build master-handshake.log: union of handshake-worker-{stdout,stderr}
            # (which is the per-shake log from the worker side) plus the
            # operator-collected master log via logslice tool, if any.
            {
                echo "═══ handshake probe ($WORKER_ID → $MASTER_URL) ═══"
                echo "TIME     = $(date -u +%Y-%m-%dT%H:%M:%S)"
                echo "PROTO    = $PROTOCOL_VERSION"
                echo "WORKER   = $WORKER_ID"
                echo "EXIT     = $HANDSHAKE_RC"
                echo
                echo "═══ worker stdout (dev-hello-client) ═══"
                cat "$EV_DIR/handshake-worker-stdout.log" 2>/dev/null || true
                echo
                echo "═══ worker stderr (dev-hello-client) ═══"
                cat "$EV_DIR/handshake-worker-stderr.log" 2>/dev/null || true
            } > "$HANDSHAKE_LOG"
            # Append worker.log to keep a single canonical stream per cap. 3 schema
            {
                [[ -r "$EV_DIR/worker.log" ]] && cat "$EV_DIR/worker.log"
                echo
                echo
                echo "═══ 2D dynamic-handshake worker stream (dev-hello-client) ═══"
                cat "$HANDSHAKE_LOG"
            } > "$EV_DIR/worker.log"
        fi
    else
        PHASE_STATUS[2d_dynamic_handshake]="SKIP"
        PHASE_DETAIL[2d_dynamic_handshake]="MASTER_URL not set — dynamic handshake skipped (cert checks still ran)"
        printf '::warn::MASTER_URL not set; 2D-dynamic handshake SKIPPED\n'
    fi
}
