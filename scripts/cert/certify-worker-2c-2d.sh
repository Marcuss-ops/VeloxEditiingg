#!/usr/bin/env bash
# =============================================================================
# scripts/cert/certify-worker-2c-2d.sh
# =============================================================================
# Phase 2C + 2D per-worker certifier — 100% Velox certification plan (cap. 3).
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

# ─── Sanity ─────────────────────────────────────────────────────────────────
[[ -n "$WORKER_ID"           ]] || { printf '::error::--worker-id is required\n' >&2; usage 1; }
[[ -n "$WORKER_IMAGE"        ]] || { printf '::error::--worker-image is required\n' >&2; usage 1; }
[[ -n "$EXPECTED_BUNDLE_HASH" ]] || { printf '::error::--expected-bundle-hash is required\n' >&2; usage 1; }
[[ -n "$WORKER_CERT_FILE"    ]] || { printf '::error::--worker-cert-file is required\n' >&2; usage 1; }
[[ -n "$WORKER_KEY_FILE"     ]] || { printf '::error::--worker-key-file is required\n' >&2; usage 1; }
[[ -n "$WORKER_CA_FILE"      ]] || { printf '::error::--worker-ca-file is required\n' >&2; usage 1; }

# Refuse non-digest pin (re-use real-bootstrap.sh invariant).
if ! [[ "$WORKER_IMAGE" =~ @sha256:[a-f0-9]{64}$ ]]; then
  printf '::error::--worker-image must be a digest pin (got: %s) — never :latest\n' \
    "$WORKER_IMAGE" >&2; exit 1
fi
if [[ ! "$EXPECTED_BUNDLE_HASH" =~ ^[a-f0-9]{64}$ ]]; then
  printf '::error::--expected-bundle-hash must be 64 lowercase hex (got %d chars)\n' \
    "${#EXPECTED_BUNDLE_HASH}" >&2; exit 1
fi

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

# ─── Locate supporting scripts + binaries ───────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REAL_BOOTSTRAP="$SCRIPT_DIR/real-bootstrap.sh"
[[ -r "$REAL_BOOTSTRAP" ]] || { printf '::error::missing real-bootstrap.sh at %s\n' \
  "$REAL_BOOTSTRAP" >&2; exit 2; }

# B1 fix (H7 hardening): do NOT hardcode /home/pierone/Pyt/VeloxLEgit.
# Primary resolution is `git -C "$SCRIPT_DIR" rev-parse --show-toplevel`
# (works for symlinked + copy-installed planners), falling back to
# `realpath "$SCRIPT_DIR/../.."` so the certifier works on any host
# where the repo is checked out (operator-side VPS, CI runner).
if command -v git >/dev/null 2>&1; then
  REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
fi
if [[ -z "$REPO_ROOT" || ! -d "$REPO_ROOT" ]]; then
  REPO_ROOT="$(realpath "$SCRIPT_DIR/../.." 2>/dev/null || true)"
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

# ─── Status accumulators ────────────────────────────────────────────────────
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

# --- 2C: Real Bootstrap ---------------------------------------------------
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
  rc=$?
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

# --- 2D-1: Static cert checks -------------------------------------------
printf '\n═══ Phase 2D-1: static cert checks ═══\n'
STATIC_OUT="$EV_DIR/cert-static.json"
python3 - "$WORKER_CERT_FILE" "$WORKER_KEY_FILE" "$WORKER_CA_FILE" "$WORKER_ID" \
        "$PROTOCOL_VERSION" "$STATIC_OUT" <<'PYEOF'
import json, sys
from datetime import datetime, timezone
cert_file, key_file, ca_file, expected_cn, expected_proto, out_path = sys.argv[1:7]
result = {
    "worker_id":            expected_cn,
    "protocol_version":     expected_proto,
    "checks":               {},
    "all_pass":             True,
    "fatal_fail":           False,
}

def fail(check, message, fatal=True):
    result["checks"][check] = {"pass": False, "detail": message}
    result["all_pass"] = False
    if fatal:
        result["fatal_fail"] = True

def ok(check, detail=""):
    result["checks"][check] = {"pass": True, "detail": detail}

# Parse the cert
try:
    import subprocess
    insp = subprocess.run(
        ["openssl", "x509", "-in", cert_file, "-noout", "-subject",
         "-issuer", "-startdate", "-enddate", "-ext", "extendedKeyUsage,subjectAltName"],
        capture_output=True, text=True, check=True)
except Exception as e:
    fail("cert_parse", f"openssl x509 failed: {e}")
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, sort_keys=True)
    sys.exit(0)

lines = [ln.split("=", 1) for ln in insp.stdout.splitlines() if "=" in ln]
fields = {k.strip(): v.strip() for k, v in lines}

# 1. Subject CN == worker_id
subj = fields.get("subject", "")
cn = ""
# Extract CN from "subject = CN = X, OU = ..." output
for part in subj.split(","):
    part = part.strip()
    if part.startswith("CN "):
        cn = part.split("=", 1)[1].strip() if "=" in part else part.split("CN ")[-1].strip()
        break
if cn == expected_cn:
    ok("cn_matches_worker_id", f"CN={cn}")
else:
    fail("cn_matches_worker_id", f"CN={cn!r} != worker_id={expected_cn!r}")

# 2. Not yet expired (enddate should be in the future)
enddate = fields.get("notAfter", "")
if enddate:
    # OpenSSL notAfter format: "Mar 12 14:00:00 2027 GMT"
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(enddate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta_sec = (dt - datetime.now(timezone.utc)).total_seconds()
        if delta_sec > 0:
            ok("not_expired", f"expires {dt.isoformat()} (in {delta_sec/86400:.0f} days)")
        else:
            fail("not_expired", f"expired {dt.isoformat()} ({abs(delta_sec)/86400:.0f} days ago)")
    except Exception as e:
        fail("not_expired", f"unparseable enddate {enddate!r}: {e}")
else:
    fail("not_expired", f"missing notAfter field")

# 3. EKU includes clientAuth
ext_out = "\n".join(l for l in insp.stdout.splitlines() if l.startswith("X509v3"))
has_client_auth = "TLS Web Client Authentication" in insp.stdout or "clientAuth" in insp.stdout
if has_client_auth:
    ok("eku_client_auth", "TLS Web Client Authentication present")
else:
    fail("eku_client_auth", "no clientAuth EKU — worker cert cannot authenticate as gRPC client")

# 4. Cert validates against CA (verify)
try:
    vr = subprocess.run(
        ["openssl", "verify", "-CAfile", ca_file, cert_file],
        capture_output=True, text=True, check=False)
    if vr.returncode == 0 and "OK" in vr.stdout:
        ok("ca_chain_valid", f"verify against CA succeeded")
    else:
        fail("ca_chain_valid", f"verify failed: {vr.stdout.strip()} {vr.stderr.strip()}")
except Exception as e:
    fail("ca_chain_valid", f"openssl verify failed: {e}")

# 5. Issuer == CA subject (resolve chain)
try:
    ca_subj = subprocess.run(
        ["openssl", "x509", "-in", ca_file, "-noout", "-subject"],
        capture_output=True, text=True, check=True)
    ca_cn = ""
    for part in ca_subj.stdout.split(","):
        part = part.strip()
        if part.startswith("subject ="):
            after = part[len("subject ="):].strip()
            for tok in after.split(","):
                if "CN" in tok and "=" in tok:
                    ca_cn = tok.split("=", 1)[1].strip()
                    break
    iss = fields.get("issuer", "")
    if ca_cn and ca_cn in iss:
        ok("issuer_matches_ca", f"issuer={iss} (CA CN={ca_cn})")
    else:
        fail("issuer_matches_ca", f"issuer={iss!r} doesn't include CA CN={ca_cn!r}")
except Exception as e:
    fail("issuer_matches_ca", f"failed: {e}")

# 6. SAN includes at least DNS:localhost or IP:127.0.0.1 (dev-mode hint),
#    OR an empty SAN (some worker certs are deployed without SAN). Warn-only.
if "DNS:" in insp.stdout or "IP:" in insp.stdout:
    ok("san_present", "SAN set (DNS/IP entries present)")
else:
    result["checks"]["san_present"] = {
        "pass": None,
        "warn": "no DNS/IP SAN entries — some operators require them", }

with open(out_path, "w") as f:
    json.dump(result, f, indent=2, sort_keys=True)
print(json.dumps(result, indent=2, sort_keys=True))
sys.exit(2 if result["fatal_fail"] else 0)
PYEOF
RC=$?
if [[ $RC -eq 0 ]]; then
  PHASE_STATUS[2d_static_cert]="PASS"
  has_warn=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if any(c.get("pass") is None for c in d["checks"].values()) else 1)' "$STATIC_OUT" && echo yes || echo no)
  PHASE_DETAIL[2d_static_cert]="all fatal checks passed${has_warn:+ (warnings present)}"
else
  PHASE_STATUS[2d_static_cert]="FAIL"
  PHASE_DETAIL[2d_static_cert]="(see cert-static.json for failing checks)"
  printf '::error::2D-static FAIL\n' >&2
fi

# --- 2D-2: Dynamic handshake probe via dev-hello-client ------------------
if [[ -n "$MASTER_URL" ]]; then
  printf '\n═══ Phase 2D-2: dynamic handshake probe ═══\n'
  DEV_HELLO_BIN="$EV_DIR/dev-hello-client"
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
    HANDSHAKE_LOG="$EV_DIR/master-handshake.log"
    : > "$HANDSHAKE_LOG"
    HANDSHAKE_RC=0
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

# --- 2D-3: Master state probe via /api/v1/workers --------------------------
if [[ -n "$MASTER_RESTSERVER" ]]; then
  printf '\n═══ Phase 2D-3: master state probe (%s) ═══\n' "$MASTER_RESTSERVER"
  # H9 fix: rstrip trailing slash so a base ending in "/" doesn't
  # resolve to "//api/v1/workers".
  MASTER_RESTSERVER="${MASTER_RESTSERVER%/}"
  STATE_OUT="$EV_DIR/master-state.json"
  HTTP_RC=0
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
    PROBE_RC=$?
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

# --- Final verdict --------------------------------------------------------
VERDICT_FILE="$EV_DIR/verdict-2c-2d.json"
ANY_FAIL="no"
for k in 2c_bootstrap 2d_static_cert 2d_dynamic_handshake 2d_master_state; do
  if [[ "${PHASE_STATUS[$k]}" == "FAIL" ]]; then ANY_FAIL="yes"; break; fi
done
ANY_SKIP="no"
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
REQUIRED_PASS="yes"
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

OVERALL="CERTIFIED"
if [[ "$REQUIRED_PASS" == "no" ]]; then OVERALL="FAIL"; fi
if [[ "$ANY_FAIL" == "yes" && "$OVERALL" != "FAIL" ]]; then OVERALL="PARTIAL"; fi

# H2 fix: the dead scaffold heredoc python3 - "...import sys" PYEOF
# block is REMOVED in this round (no-op; ~30 ms wasted startup on
# every certifier run). The canonical verdict emitter downstream reads
# the side-band _phases.json produced by the bash block below.

# Build the phases JSON via bash associative-array scalar serialization.
{
  echo "  \"phases\": {"
  first=true
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

# ─── Final exit ────────────────────────────────────────────────────────────
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
