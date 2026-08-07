# scripts/cert/static_certificate.sh
# ─────────────────────────────────────────────────────────────────────────────
# Phase 2D-1 (static cert checks) split out of certify-worker-2c-2d.sh
# as part of the per-phase refactor (bootstrap_2c / static_certificate /
# dynamic_handshake / master_state / evidence_verdict + thin entrypoint).
#
# This file owns the python heredoc that validates the worker's TLS cert
# against 6 checks (CN match, not expired, EKU clientAuth, CA chain,
# issuer matches CA, SAN present). Writes cert-static.json to the
# evidence dir and records PHASE_STATUS[2d_static_cert].
#
# Sourced by the entrypoint AFTER bootstrap_2c.sh and BEFORE
# dynamic_handshake.sh. Depends on the entrypoint's global variables
# (WORKER_CERT_FILE, WORKER_KEY_FILE, WORKER_CA_FILE, WORKER_ID,
# PROTOCOL_VERSION) and the PHASE_STATUS / PHASE_DETAIL arrays plus the
# EV_DIR set by bootstrap_2c.sh.
# ─────────────────────────────────────────────────────────────────────────────

# --- 2D-1: Static cert checks --------------------------------------------
run_static_certificate() {
    printf '\n═══ Phase 2D-1: static cert checks ═══\n'
    local STATIC_OUT="$EV_DIR/cert-static.json"
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
    local RC=$?
    if [[ $RC -eq 0 ]]; then
        PHASE_STATUS[2d_static_cert]="PASS"
        local has_warn
        has_warn=$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if any(c.get("pass") is None for c in d["checks"].values()) else 1)' "$STATIC_OUT" && echo yes || echo no)
        PHASE_DETAIL[2d_static_cert]="all fatal checks passed${has_warn:+ (warnings present)}"
    else
        PHASE_STATUS[2d_static_cert]="FAIL"
        PHASE_DETAIL[2d_static_cert]="(see cert-static.json for failing checks)"
        printf '::error::2D-static FAIL\n' >&2
    fi
}
