#!/usr/bin/env bash
# ops/jobs/submit_benchmark_pathological.sh
#
# Submit the official benchmark-pathological payload to the master via the
# canonical M2M intake (POST /api/v1/jobs) and poll to terminal state.
#
# The payload is intentionally intake-valid (202 accepted) but every scene
# encodes a failure mode (huge asset, slow URL, corrupt clip via sha256
# mismatch, odd codec, missing audio, wrong declared duration) plus a
# missing-font layer. The EXPECTED outcome is a clean terminal FAILED —
# never a hang, never a corrupted cache.
#
# This benchmark PASSES when the system fails well:
#   exit 0  → terminal FAILED/CANCELLED or intake rejection (fail-closed)
#   exit 1  → SUCCEEDED (unexpected — the pathologies did not fail the job)
#   exit 2  → poll timeout (system hung — the real failure)
#   exit 3  → other POST rejection path
#   exit 4  → usage/env error
#
# Required env:
#   VELOX_MASTER_URL            master base URL (default http://127.0.0.1:8080)
#   VELOX_ADMIN_TOKEN           admin bearer (env var or TOKEN_FILE dotenv)
#
# Optional env:
#   VELOX_BENCHMARK_IDEM_KEY    idempotency override (cold/warm cache runs)
#   VELOX_BENCHMARK_POLL_TIMEOUT_S   poll cap in seconds (default 300)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/benchmark-common.sh
source "${SCRIPT_DIR}/lib/benchmark-common.sh"

BENCHMARK_PAYLOAD_FILE="${SCRIPT_DIR}/benchmark-pathological.generate.json"

benchmark_resolve_admin_token
benchmark_mint_m2m

# No asset substitution: the frozen placeholder URLs ARE the pathologies.
benchmark_substitute_payload() {
  cat "${BENCHMARK_PAYLOAD_FILE}"
}

rc=0
benchmark_submit_and_poll || rc=$?
case $rc in
  0) printf 'FAIL benchmark-pathological: SUCCEEDED (pathologies did not fail the job)\n' >&2; exit 1 ;;
  1) printf 'PASS benchmark-pathological: clean terminal FAILED/CANCELLED — system failed well\n'; exit 0 ;;
  2) printf 'FAIL benchmark-pathological: poll timeout — job hung\n' >&2; exit 1 ;;
  3) printf 'PASS benchmark-pathological: rejected at intake (fail-closed)\n'; exit 0 ;;
  *) printf 'FAIL benchmark-pathological: unexpected poll error rc=%s\n' "$rc" >&2; exit 1 ;;
esac
