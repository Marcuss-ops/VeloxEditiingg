#!/usr/bin/env bash
# =============================================================================
# scripts/cert/certify-worker-2c-2d.sh
# =============================================================================
# Phase 2C + 2D per-worker certifier — 100% Velox certification plan (cap. 3).
#
# Thin entrypoint for the per-phase refactor:
#   bootstrap_2c.sh        — preflight + 2C real-bootstrap + bundle_hash xcheck
#   static_certificate.sh  — 2D-1 openssl-based cert chain validation
#   dynamic_handshake.sh   — 2D-2 dev-hello-client gRPC handshake probe
#   master_state.sh        — 2D-3 /api/v1/workers REST probe + B3/B4/B5 checks
#   evidence_verdict.sh    — _phases.json + verdict-2c-2d.json + exit code
#
# After the refactor this file is intentionally minimal:
#   1. Parse CLI args (sets all WORKER_* / MASTER_* / EXPECTED_* globals).
#   2. Declare PHASE_STATUS / PHASE_DETAIL associative arrays (the
#      per-phase files read+write these to communicate results).
#   3. Source the 5 per-phase files in checklist order.
#   4. Call each phase function in order.
#   5. run_evidence_verdict handles the final exit (never returns).
#
# This script orchestrates two sub-phases against a single VPS:
#   2C — Bootstrap fail-closed on the REAL worker image (not the
#        fake-FFmpeg of `make bootstrap-smoke`); asserts verdict=OK +
#        4 canonical step PASS (bundle_hash, ffmpeg, output_dir,
#        engine_self_render). Delegates to scripts/cert/real-bootstrap.sh.
#   2D — mTLS handshake verification on the master. Three layered checks:
#        (a) STATIC — worker cert CN == worker_id, validated against cluster
#            CA, not-yet-expired, EKU clientAuth present.
#        (b) DYNAMIC handshake probe — DataServer/cmd/dev-hello-client opens
#            a bidi gRPC stream with REAL mTLS certs + REAL credential_hash,
#            sends a typed Hello, asserts HelloAck within 15s.
#        (c) MASTER state probe — curl/DataServer REST surfaces (/api/v1/workers)
#            confirm CONNECTED state, protocol_version, bundle/version,
#            capabilities, max-concurrency (when the master is reachable).
#
# Output evidence (cap. 11 layout, side-loaded by the verdict-emitter):
#   $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/bootstrap-report.json     <- 2C
#   $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/worker.log                <- 2C+2D
#   $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/master-handshake.log      <- 2D
#   $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/hand-off-register.json   <- 2D
#   $EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID/verdict-2c-2d.json       <- combined
#
# Required env (or matching CLI flags):
#   WORKER_ID                       canonical worker_id (== cert CN always)
#   WORKER_IMAGE                    ghcr.io/<owner>/velox-worker@sha256:<64>
#   EXPECTED_BUNDLE_HASH            BUNDLE_HASH.txt SHA-256 (64 lowercase hex)
#   WORKER_CERT_FILE                path to worker.pem on the VPS
#   WORKER_KEY_FILE                 path to worker.key on the VPS
#   WORKER_CA_FILE                  path to cluster ca.crt on the VPS
# Optional:
#   MASTER_URL                      host:port of the master gRPC endpoint
#   MASTER_RESTSERVER               base URL of master's HTTPS REST surface
#                                   (e.g. https://velox.example.com)
#   EVIDENCE_ROOT                   (default: $HOME/evidence)
#   CERT_DATE                       (default: today UTC)
#   PROTOCOL_VERSION                (default: v3)
#   HANDSHAKE_TIMEOUT_S             (default: 20)
#   EXPECTED_MAX_CONCURRENCY        optional, asserted vs /api/v1/workers
#
# Exit: 0 on combined 2C+2D PASS; 1 on 2C bootstrap fail; 2 on 2D static
# cert fail; 3 on 2D dynamic handshake fail; 4 on 2D master-state fail.
# =============================================================================

set -uo pipefail  # NOT -e: continue across checks so all failures report

usage() {
  cat <<'USG'
usage: certify-worker-2c-2d.sh
          --worker-id ID
          --worker-image REF                  # ghcr.io/<owner>/velox-worker@sha256:<64>; refuses :latest
          --expected-bundle-hash HEX          # 64 lowercase hex
          --worker-cert-file PATH
          --worker-key-file  PATH
          --worker-ca-file   PATH
          [--master-url HOST:PORT]            # gRPC endpoint for 2D-2 dev-hello-client handshake
          [--master-restserver URL]           # HTTPS base for /api/v1/workers (sets 2D-3 in scope)
          [--expected-bundle-version VER]     # REQUIRED if --master-restserver is set (B3' preflight)
          [--expected-max-concurrency N]      # optional, asserted vs /api/v1/workers
          [--protocol-version v3]             # default: v3
          [--handshake-timeout-s 20]          # floor: 15 (dev-hello-client internal HelloAckTimeout) — H3
          [--allow-skip-dynamic]              # opt-in skip of 2D-2 dynamic handshake when no master
          [--evidence-root DIR]               # default: $HOME/evidence
          [--date YYYY-MM-DD]                 # default: today UTC
          [--help]

Sub-phases 2C (real bootstrap on the worker image, asserting verdict=OK +
4 step PASS) + 2D (mTLS handshake: cert static checks + dev-hello-client
dynamic probe + master /api/v1/workers state assertion).

Evidence path: $EVIDENCE_ROOT/<date>/<worker_id>/ — see
docs/100-percent-plan/cap-3-evidence.md for the canonical file layout.
Exit: 0 on CERTIFIED | 1 on 2C fail | 2 on 2D-1 fail | 3 on 2D-2 fail
| 4 on 2D-3 fail | 5 on preflight sanity fail.
USG
  exit "${1:-0}"
}

# Sibling-files directory: every sourced file lives next to the entrypoint
# so the certifier package can be relocated as a unit (e.g. installed to
# /usr/local/bin without breaking relative includes). Uses BASH_SOURCE[1]
# fallback to BASH_SOURCE[0] so a direct `bash certify-worker-2c-2d.sh`
# invocation also resolves correctly.
readonly CERTIFIER_DIR="$(cd "$(dirname "${BASH_SOURCE[1]:-${BASH_SOURCE[0]}}")" 2>/dev/null && pwd)"

WORKER_ID=""
WORKER_IMAGE=""
EXPECTED_BUNDLE_HASH=""
WORKER_CERT_FILE=""
WORKER_KEY_FILE=""
WORKER_CA_FILE=""
MASTER_URL="${MASTER_URL:-}"
MASTER_RESTSERVER="${MASTER_RESTSERVER:-}"
PROTOCOL_VERSION="${PROTOCOL_VERSION:-v3}"
HANDSHAKE_TIMEOUT_S="${HANDSHAKE_TIMEOUT_S:-20}"
EXPECTED_MAX_CONCURRENCY=""
# B3' fix (closure of original user request gap): bundle_version is
# half of "bundle/versione corretta" — the operator MUST assert against
# the master's record when running 2D-3 master-state probe. Empty by
# default; required when MASTER_RESTSERVER is set (preflight below).
EXPECTED_BUNDLE_VERSION=""
EVIDENCE_ROOT=""
CERT_DATE=""
# B2 fix: opt-in flag default empty; setting --allow-skip-dynamic flips
# to "true" — the verdict-emit then lets the 2D-dynamic slice pass as
# SKIP without fail-closed penalty. Default empty = fail-closed.
ALLOW_SKIP_DYNAMIC="${ALLOW_SKIP_DYNAMIC:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker-id)               WORKER_ID="$2"; shift 2 ;;
    --worker-image)            WORKER_IMAGE="$2"; shift 2 ;;
    --expected-bundle-hash)    EXPECTED_BUNDLE_HASH="$2"; shift 2 ;;
    --worker-cert-file)        WORKER_CERT_FILE="$2"; shift 2 ;;
    --worker-key-file)         WORKER_KEY_FILE="$2"; shift 2 ;;
    --worker-ca-file)          WORKER_CA_FILE="$2"; shift 2 ;;
    --master-url)              MASTER_URL="$2"; shift 2 ;;
    --master-restserver)       MASTER_RESTSERVER="$2"; shift 2 ;;
    --protocol-version)        PROTOCOL_VERSION="$2"; shift 2 ;;
    --handshake-timeout-s)     HANDSHAKE_TIMEOUT_S="$2"; shift 2 ;;
    --expected-max-concurrency) EXPECTED_MAX_CONCURRENCY="$2"; shift 2 ;;
    # B3' fix: half of the original user-request pair bundle/versione.
    # Required when --master-restserver is set.
    --expected-bundle-version) EXPECTED_BUNDLE_VERSION="$2"; shift 2 ;;
    --evidence-root)           EVIDENCE_ROOT="$2"; shift 2 ;;
    --date)                    CERT_DATE="$2"; shift 2 ;;
    # B2 fix: opt-in escape hatch for diagnostic runs on hosts that
    # don't yet have a live master.gateway.sk:50051 reachable. Without
    # this flag, CERTIFIED verdict REQUIRES a real mTLS handshake pass
    # (or a fail-closed verdict). Operators must explicitly acknowledge
    # the skip to avoid a green report without exercising the actual
    # handshake probe.
    --allow-skip-dynamic)      ALLOW_SKIP_DYNAMIC="true"; shift ;;
    --help|-h)                 usage 0 ;;
    *) printf 'unknown flag: %s\n' "$1" >&2; exit 1 ;;
  esac
done

# ─── Status accumulators (shared across all phase files) ───────────────────
# Each per-phase file reads PHASE_STATUS[$k] (e.g. "2c_bootstrap") and
# writes it; evidence_verdict.sh iterates them to build _phases.json and
# the final exit code. Declared here (not in any phase file) so the
# sourcing order doesn't matter — all phase files can read+write any key.
declare -A PHASE_STATUS=(
  [2c_bootstrap]="SKIP"
  [2d_static_cert]="SKIP"
  [2d_dynamic_handshake]="SKIP"
  [2d_master_state]="SKIP"
)
declare -A PHASE_DETAIL=(
  [2c_bootstrap]=""
  [2d_static_cert]=""
  [2d_dynamic_handshake]=""
  [2d_master_state]=""
)

# ─── Source per-phase files in checklist order ────────────────────────────
# Each file defines exactly one run_* function that reads the entrypoint's
# globals + the PHASE_* arrays. Sourcing order is the same as the run
# order below.
source "$CERTIFIER_DIR/bootstrap_2c.sh"
source "$CERTIFIER_DIR/static_certificate.sh"
source "$CERTIFIER_DIR/dynamic_handshake.sh"
source "$CERTIFIER_DIR/master_state.sh"
source "$CERTIFIER_DIR/evidence_verdict.sh"

# ─── Run in checklist order ───────────────────────────────────────────────
# run_bootstrap_2c ALSO does preflight sanity (the original preflight
# block moved into bootstrap_2c.sh as run_preflight). If preflight
# fails it exits before run_bootstrap_2c proceeds.
run_bootstrap_2c
run_static_certificate
run_dynamic_handshake
run_master_state

# run_evidence_verdict never returns — it computes OVERALL, prints the
# verdict, and calls exit with the phase-mapped code (0/1/2/3/4).
run_evidence_verdict
