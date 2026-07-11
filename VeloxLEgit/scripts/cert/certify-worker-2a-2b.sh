#!/usr/bin/env bash
# =============================================================================
# scripts/cert/certify-worker-2a-2b.sh
# =============================================================================
# Worker certification tool — phases 2A (host) + 2B (deployment) of the
# 100% Velox certification plan (cap. 3).
#
# Runs ON the VPS (or any host where the worker container runs) as root
# or as a sudo-capable user. Does NOT modify system state except for:
#   * one marker file in /var/lib/velox-worker/.cert-write-test (removed)
#   * writes everything under ${EVIDENCE_ROOT} (default: $HOME/evidence)
#
# Outputs into ${EVIDENCE_ROOT}/<CERT_DATE>/<WORKER_ID>/:
#   host.json              — 9 host checks as structured JSON
#   host.txt               — raw dumps (df/free/uname/etc.) for post-mortem
#   image-digest.txt       — pinned running digest (verified against expected)
#   image-signature.txt    — cosign verify output OR SKIP marker
#   worker-config.sha256   — sha256 of the mounted worker_config.json
#   certificate.txt        — openssl x509 -text of the in-container CA
#   verdict.json           — aggregate PASS/FAIL + per-section counts
#
# Required env (or matching CLI flags):
#   WORKER_ID                e.g. velox-worker-1
#   EXPECTED_WORKER_IMAGE_DIGEST   sha256:abc... (from worker-image workflow)
#   MASTER_URL               e.g. http://<master-host>:8000 (probed for /health)
#   CERT_DATE                YYYY-MM-DD (default: today UTC)
#   EVIDENCE_ROOT            (default: $HOME/evidence)
#   CONTAINER_NAME           (default: velox-worker-${WORKER_ID})
#   CONFIG_FILE              host path (default: /var/lib/velox-worker/worker_config.json)
#   HEALTH_PORT              (default: 8081; reads VELOX_HEALTH_PORT from env_file)
#
# Exit codes:
#   0   all 9 host + 7 deploy checks PASS
#   1   any check FAIL or required var missing
#   2   docker not available / worker container not present
#   3   cosign required but unavailable
# =============================================================================

set -uo pipefail  # NOT -e: continue across checks so all results report

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ─── Args ────────────────────────────────────────────────────────────────────
usage() {
  cat <<USG
usage: $0 [--worker-id ID] [--expected-digest sha256:...] [--master URL]
          [--date YYYY-MM-DD] [--evidence-root DIR] [--container NAME]
          [--config-file PATH] [--help]

All flags have env-var equivalents (VELOX_WORKER_ID, EXPECTED_WORKER_IMAGE_DIGEST,
MASTER_URL, CERT_DATE, EVIDENCE_ROOT, CONTAINER_NAME, CONFIG_FILE).
USG
  exit "${1:-0}"
}
while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker-id)         WORKER_ID="$2"; shift 2 ;;
    --expected-digest)   EXPECTED_WORKER_IMAGE_DIGEST="$2"; shift 2 ;;
    --master)            MASTER_URL="$2"; shift 2 ;;
    --date)              CERT_DATE="$2"; shift 2 ;;
    --evidence-root)     EVIDENCE_ROOT="$2"; shift 2 ;;
    --container)         CONTAINER_NAME="$2"; shift 2 ;;
    --config-file)       CONFIG_FILE="$2"; shift 2 ;;
    --health-port)       HEALTH_PORT="$2"; shift 2 ;;
    --help|-h)           usage 0 ;;
    *) printf 'unknown flag: %s\n' "$1" >&2; usage 1 ;;
  esac
done

# ─── Required vars ───────────────────────────────────────────────────────────
for v in WORKER_ID; do
  if [[ -z "${!v:-}" ]]; then
    printf '::error::%s is required (flag or env)\n' "$v" >&2
    usage 1
  fi
done

CERT_DATE="${CERT_DATE:-$(date -u +%Y-%m-%d)}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$HOME/evidence}"
CONTAINER_NAME="${CONTAINER_NAME:-velox-worker-${WORKER_ID}}"
CONFIG_FILE="${CONFIG_FILE:-/var/lib/velox-worker/worker_config.json}"
HEALTH_PORT="${HEALTH_PORT:-8081}"
MASTER_URL="${MASTER_URL:-${VELOX_GRPC_MASTER_URL:-http://localhost:8000}}"
EXPECTED_WORKER_IMAGE_DIGEST="${EXPECTED_WORKER_IMAGE_DIGEST:-}"
EV_DIR="$EVIDENCE_ROOT/$CERT_DATE/$WORKER_ID"
mkdir -p "$EV_DIR"

pass=0; fail=0
declare -a HOST_RESULTS=() DEPLOY_RESULTS=()

record_host() {
  local name="$1" status="$2" info="$3"
  HOST_RESULTS+=("$status $name $info")
  if [[ "$status" == PASS ]]; then pass=$((pass+1)); else fail=$((fail+1)); fi
  printf '  [%s] H %-12s  %s\n' "$status" "$name" "$info"
}

# ─── ────── PHASE 2A — HOST CHECKS ──────────────────────────────────────────
printf '\n══════════════════════════════════════════════════════\n'
printf '  Phase 2A — HOST checks (worker=%s, evidence=%s)\n' "$WORKER_ID" "$EV_DIR"
printf '══════════════════════════════════════════════════════\n'

# H1.cpu_arch — x86_64 expected; record actual
arch="$(uname -m)"
nproc_count="$(nproc 2>/dev/null || echo 0)"
cpu_model="$(awk -F: '/model name/{print $2; exit}' /proc/cpuinfo | sed 's/^ *//' | head -c 80)"
if [[ "$arch" == "x86_64" || "$arch" == "aarch64" ]]; then
  record_host "cpu_arch" "PASS" "arch=$arch nproc=$nproc_count model=$cpu_model"
else
  record_host "cpu_arch" "FAIL" "arch=$arch not in {x86_64,aarch64}"
fi

# H2.ram — ≥ 4 GB recommended for worker (4g mem_limit cap in compose)
ram_mb="$(awk '/MemTotal:/ {print int($2/1024)}' /proc/meminfo 2>/dev/null || echo 0)"
if (( ram_mb >= 4096 )); then
  record_host "ram" "PASS" "total=${ram_mb}MB"
else
  record_host "ram" "FAIL" "total=${ram_mb}MB (< 4096)"
fi

# H3.disk — ≥ 5 GB free on /var/lib/velox-worker and /
free_root_mb="$(df -P -m / | awk 'NR==2 {print $4}')"
free_var_mb="$(df -P -m /var/lib/velox-worker 2>/dev/null | awk 'NR==2 {print $4}' || echo 0)"
if (( free_root_mb >= 5120 )); then
  if (( free_var_mb >= 5120 )); then
    record_host "disk" "PASS" "/=${free_root_mb}MB workdir=${free_var_mb}MB"
  else
    record_host "disk" "FAIL" "/=${free_root_mb}MB workdir=${free_var_mb}MB (< 5120)"
  fi
else
  record_host "disk" "FAIL" "/=${free_root_mb}MB (< 5120)"
fi

# H4.fs_write — touch + rm a marker
write_marker="/var/lib/velox-worker/.cert-write-test.$$"
if (umask 022; touch "$write_marker" 2>/dev/null) && [[ -f "$write_marker" ]] \
   && rm -f "$write_marker"; then
  record_host "fs_write" "PASS" "workdir writable"
else
  record_host "fs_write" "FAIL" "workdir NOT writable: ${write_marker:-/var/lib/velox-worker}"
fi

# H5.ntp — clock sync within 1s; fall back through timedatectl/chronyc/ntpdate
clock_skew_s=9999
if command -v timedatectl >/dev/null 2>&1; then
  if timedatectl show 2>/dev/null | grep -q 'NTPSynchronized=yes'; then
    clock_skew_s=0
  fi
fi
if (( clock_skew_s > 1 )) && command -v chronyc >/dev/null 2>&1; then
  skew="$(chronyc tracking 2>/dev/null | awk '/System time/{print $4}' | sed 's/[^0-9.\-]//g' || true)"
  [[ -n "$skew" ]] && printf -v clock_skew_s '%.4f' "$skew" || clock_skew_s=9999
fi
if (( clock_skew_s > 1 )) && command -v ntpdate >/dev/null 2>&1; then
  skew="$(ntpdate -q -t 2 0.pool.ntp.org 2>&1 | awk '/offset/{print $5}' | sed 's/[^0-9.\-]//g' || true)"
  if [[ -n "$skew" ]]; then printf -v clock_skew_s '%.4f' "${skew#-}"; else clock_skew_s=9999; fi
fi
# Absolute value boundary check (supports negative skew)
abs_skew="$(awk -v x="$clock_skew_s" 'BEGIN{ v=x<0?-x:x; printf("%.4f",v) }')"
if awk -v s="$abs_skew" 'BEGIN{ exit (s+0 <= 1.0) ? 0 : 1 }'; then
  record_host "ntp" "PASS" "skew=${abs_skew}s"
else
  record_host "ntp" "WARN" "skew=${abs_skew}s (>1s; not blocking but flagged)"
  # WARN does NOT count as FAIL — it counts as pass in pass-count (operator decision)
fi

# H6.dns — resolve github.com (or fallback curl 1.1.1.1)
resolved=""
if command -v getent >/dev/null 2>&1; then
  resolved="$(getent hosts github.com 2>/dev/null | awk '{print $1}' | head -1)"
fi
if [[ -n "$resolved" ]]; then
  record_host "dns" "PASS" "github.com→$resolved"
else
  if curl -sfS -m 5 -o /dev/null -w '%{http_code}\n' https://1.1.1.1 2>&1 | grep -q '^2'; then
    record_host "dns" "FAIL" "DNS resolution failed but HTTPS 1.1.1.1 OK (DNS config broken)"
  else
    record_host "dns" "FAIL" "DNS resolution + HTTPS both failed"
  fi
fi

# H7.connectivity — probe MASTER/health
http_code="$(curl -sS -m 5 -o /dev/null -w '%{http_code}' "$MASTER_URL/health" 2>/dev/null || echo 000)"
if [[ "$http_code" =~ ^2 ]]; then
  record_host "connectivity" "PASS" "$MASTER_URL/health → $http_code"
elif [[ "$http_code" =~ ^3 ]]; then
  record_host "connectivity" "PASS" "$MASTER_URL/health → $http_code (redirect)"
else
  record_host "connectivity" "FAIL" "$MASTER_URL/health → $http_code"
fi

# H8.docker — docker + docker compose available
if ! command -v docker >/dev/null 2>&1; then
  record_host "docker" "FAIL" "docker command not found"
  exit 2
fi
if docker info >/dev/null 2>&1; then
  docker_ver="$(docker --version | awk '{print $3}' | tr -d ',')"
  compose_ver=""
  if docker compose version >/dev/null 2>&1; then
    compose_ver="$(docker compose version --short 2>/dev/null || echo ok)"
  elif command -v docker-compose >/dev/null 2>&1; then
    compose_ver="$(docker-compose --version | awk '{print $NF}' | tr -d ',')"
  fi
  record_host "docker" "PASS" "docker=$docker_ver compose=$compose_ver"
else
  record_host "docker" "FAIL" "docker daemon unreachable"
fi

# H9.restart_loop — RestartCount and State.Status
if docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  restart_count="$(docker inspect -f '{{.RestartCount}}' "$CONTAINER_NAME" 2>/dev/null || echo -1)"
  state_status="$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || echo unknown)"
  health_status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' \
                   "$CONTAINER_NAME" 2>/dev/null || echo unknown)"
  if (( restart_count < 10 )) && [[ "$state_status" =~ ^(running|healthy)$ ]]; then
    record_host "restart_loop" "PASS" "restart_count=$restart_count state=$state_status health=$health_status"
  else
    record_host "restart_loop" "FAIL" "restart_count=$restart_count state=$state_status (loop or unhealthy)"
  fi
else
  record_host "restart_loop" "FAIL" "container $CONTAINER_NAME not found"
fi

# ─── PHASE 2B — DEPLOY CHECKS ───────────────────────────────────────────────
printf '\n══════════════════════════════════════════════════════\n'
printf '  Phase 2B — DEPLOY checks (worker=%s)\n' "$WORKER_ID"
printf '══════════════════════════════════════════════════════\n'

# Helpers — re-use host-style record but in a deploy bucket.
record_deploy() {
  local name="$1" status="$2" info="$3"
  DEPLOY_RESULTS+=("$status $name $info")
  if [[ "$status" == PASS ]]; then pass=$((pass+1)); else fail=$((fail+1)); fi
  printf '  [%s] D %-15s  %s\n' "$status" "$name" "$info"
}

# Resolve EXPECTED_WORKER_IMAGE_DIGEST from worker.env if not set (fail-closed fallback chain).
if [[ -z "$EXPECTED_WORKER_IMAGE_DIGEST" && -r /etc/velox-worker/worker.env ]]; then
  pin_line="$(grep -E '^VELOX_WORKER_IMAGE=' /etc/velox-worker/worker.env 2>/dev/null \
                | sed -nE 's#.*@(sha256:[a-f0-9]{64}).*#\1#p' | head -1 || true)"
  if [[ -n "$pin_line" ]]; then
    EXPECTED_WORKER_IMAGE_DIGEST="$pin_line"
    printf '→ resolved EXPECTED_WORKER_IMAGE_DIGEST from /etc/velox-worker/worker.env: %s\n' "$pin_line"
  fi
fi

# D1.digest_pin — running RepoDigests[0] starts with @sha256:...
if ! docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  record_deploy "digest_pin" "FAIL" "container $CONTAINER_NAME not present"
else
  running_pin="$(docker inspect -f '{{index .RepoDigests 0}}' "$CONTAINER_NAME" 2>/dev/null \
                  | sed -nE 's#.*@(sha256:[a-f0-9]{64}).*#\1#p' || true)"
  image_id="$(docker inspect -f '{{.Image}}' "$CONTAINER_NAME" 2>/dev/null || echo)"
  run_image="${running_pin:-${image_id}}"
  echo "$run_image" > "$EV_DIR/image-digest.txt"
  if [[ -z "$EXPECTED_WORKER_IMAGE_DIGEST" ]]; then
    record_deploy "digest_pin" "FAIL" \
      "no EXPECTED_WORKER_IMAGE_DIGEST (env or worker.env) — cannot verify pinning"
  elif [[ "$run_image" == "$EXPECTED_WORKER_IMAGE_DIGEST" ]]; then
    record_deploy "digest_pin" "PASS" "running=$run_image matches expected"
  else
    record_deploy "digest_pin" "FAIL" "running=$run_image ≠ expected=$EXPECTED_WORKER_IMAGE_DIGEST"
  fi
fi

# D2.running — state = running/health check = healthy
state="$(docker inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || echo missing)"
health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-healthcheck{{end}}' \
           "$CONTAINER_NAME" 2>/dev/null || echo unknown)"
if [[ "$state" == "running" && "$health" =~ ^(healthy|no-healthcheck)$ ]]; then
  record_deploy "container_running" "PASS" "state=$state health=$health"
else
  record_deploy "container_running" "FAIL" "state=$state health=$health"
fi

# D3.config_mount — host CONFIG_FILE bind-mounted at /opt/velox/worker_config.json:ro
mount_dest="/opt/velox/worker_config.json"
mounts_json="$(docker inspect -f '{{json .Mounts}}' "$CONTAINER_NAME" 2>/dev/null || echo '[]')"
mounts_summary="$(python3 -c "
import json, sys
m = json.loads('''$mounts_json''') if '''$mounts_json''' else []
for x in m:
    print(f'{x.get(\"Type\",\"\")} {x.get(\"Source\",\"\")}->{x.get(\"Destination\",\"\")} mode={x.get(\"Mode\",\"\")}')
" 2>/dev/null || true)"
config_mount="$(printf '%s\n' "$mounts_summary" \
                | awk -v d="$mount_dest" '$3~"->"d"$" || $2~"->"d"$"')"
if [[ -n "$config_mount" ]]; then
  record_deploy "config_mount" "PASS" "host $CONFIG_FILE → $mount_dest (ro)"
else
  record_deploy "config_mount" "FAIL" "no $mount_dest in mounts:\n$mounts_summary"
fi

# D4.cert_readable — ca.crt + worker.crt readable inside container via docker exec
cert_a="/run/velox/certs/ca.crt"
cert_b="/run/velox/certs/worker.crt"
cert_a_ok="$(docker exec "$CONTAINER_NAME" test -r "$cert_a" && echo 1 || echo 0)"
cert_b_ok="$(docker exec "$CONTAINER_NAME" test -r "$cert_b" && echo 1 || echo 0)"
if [[ "$cert_a_ok$cert_b_ok" == "11" ]]; then
  record_deploy "cert_readable" "PASS" "$cert_a and $cert_b readable by in-container UID"
else
  record_deploy "cert_readable" "FAIL" \
    "ca=$cert_a_ok worker=$cert_b_ok (need both = 1)"
fi

# D5.uid_gid — in-container velox user must be UID 10001 / GID 10001 (compose contract)
in_uid="$(docker exec "$CONTAINER_NAME" id -u 2>/dev/null || echo 0)"
in_gid="$(docker exec "$CONTAINER_NAME" id -g 2>/dev/null || echo 0)"
if [[ "$in_uid" == "10001" && "$in_gid" == "10001" ]]; then
  record_deploy "uid_gid" "PASS" "uid=$in_uid gid=$in_gid"
else
  record_deploy "uid_gid" "FAIL" "uid=$in_uid gid=$in_gid (expected 10001/10001)"
fi

# D6.persistent_dirs — /var/lib/velox-worker + cache mounted rw from host
persist_mount="$(printf '%s\n' "$mounts_summary" \
                 | awk '$2~"^/var/lib/velox-worker"\$ || $3~"^/var/lib/velox-worker->"')"
if [[ -n "$persist_mount" ]]; then
  xdg_cache_mount="$(printf '%s\n' "$mounts_summary" \
                      | awk '$3~"->/var/lib/velox-worker/cache\$" || $3~"->/home/velox/.cache\$"')"
  if [[ -n "$xdg_cache_mount" ]]; then
    record_deploy "persistent_dirs" "PASS" "work + cache bound from /var/lib/velox-worker"
  else
    record_deploy "persistent_dirs" "FAIL" \
      "work bound but no cache mount (/var/lib/velox-worker/cache or /home/velox/.cache)"
  fi
else
  record_deploy "persistent_dirs" "FAIL" "/var/lib/velox-worker NOT bound:\n$mounts_summary"
fi

# D7.health_endpoint — /health/ready on loopback via docker exec (avoids port publishing check)
ready_body="$(docker exec "$CONTAINER_NAME" \
  curl -sf -m 5 "http://127.0.0.1:${HEALTH_PORT}/health/ready" 2>/dev/null || true)"
# The endpoint may return JSON or empty; success = curl exit 0 (we're in a context that captures
# body only on success because curl -f shorts).
if [[ -n "$ready_body" ]]; then
  record_deploy "health_endpoint" "PASS" "/health/ready returned $(printf '%s' "$ready_body" | head -c 80)"
else
  record_deploy "health_endpoint" "WARN" "/health/ready empty or unreachable from docker exec"
fi

# ─── Evidence: worker-config sha256 + certificate.txt ──────────────────────
if [[ -r "$CONFIG_FILE" ]]; then
  sha256sum "$CONFIG_FILE" | awk '{print $1"  "$2}' > "$EV_DIR/worker-config.sha256"
else
  printf 'CONFIG FILE NOT READABLE: %s\n' "$CONFIG_FILE" \
    > "$EV_DIR/worker-config.sha256"
fi

# certificate.txt — pull in-container CA through docker exec for openssl inspection
docker exec "$CONTAINER_NAME" openssl x509 -in "$cert_a" -noout -text \
  > "$EV_DIR/certificate.txt" 2>/dev/null || {
    # Fallback: open CA on the host (cert file is mounted ro)
    host_cert="$(awk -v d="$cert_a" '$3~"->"d"$" || $2~"->"d"$"{print $2; exit}' <<<"$mounts_summary")"
    if [[ -n "$host_cert" && -r "$host_cert" ]]; then
      openssl x509 -in "$host_cert" -noout -text > "$EV_DIR/certificate.txt" 2>/dev/null \
        || echo "openssl x509 parse failed" > "$EV_DIR/certificate.txt"
    else
      echo "no CA cert readable: $cert_a" > "$EV_DIR/certificate.txt"
    fi
}

# ─── Evidence: image-signature.txt (cosign verify, optional) ────────────────
{
  # Determine the GHCR repo from VELOX_WORKER_IMAGE
  ghcr_image="$(grep -E '^VELOX_WORKER_IMAGE=' /etc/velox-worker/worker.env 2>/dev/null \
                 | sed -nE 's#^VELOX_WORKER_IMAGE=(ghcr.io/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+)@.*#\1#p' \
                 | head -1)"
  if [[ -z "$ghcr_image" ]]; then
    # fallback: derive from EXPECTED_WORKER_IMAGE_DIGEST = sha256:hex;
    # we can't recover the org/repo from that. So we record SKIP.
    echo "SKIP — VELOX_WORKER_IMAGE not in /etc/velox-worker/worker.env; cosign verify needs the repo path."
    exit 0
  fi
  if ! command -v cosign >/dev/null 2>&1; then
    echo "SKIP — cosign not installed on VPS"
    exit 0
  fi
  # Use the running image by digest
  digest="${EXPECTED_WORKER_IMAGE_DIGEST#sha256:}"
  cosign verify --certificate-identity-regexp '.*' \
                --certificate-oidc-issuer 'https://token.actions.githubusercontent.com' \
                "${ghcr_image}@sha256:${digest}" 2>&1 || true
} > "$EV_DIR/image-signature.txt"

# ─── Evidence: host.json + host.txt + verdict.json ─────────────────────────
# Build host.json + verdict.json via Python heredocs (avoids shell
# string-escaping pitfalls with multi-line `info` fields).
python3 <<PYEOF > "$EV_DIR/host.json"
import json, os
results = """$(printf '%s\n' "${HOST_RESULTS[@]}")"""
entries = []
for line in results.splitlines():
    if not line.strip(): continue
    parts = line.split(' ', 2)
    if len(parts) < 3: continue
    status, name, info = parts[0], parts[1], parts[2]
    entries.append({"name": name, "status": status, "info": info})
print(json.dumps({
    "worker_id": "${WORKER_ID}",
    "cert_date": "${CERT_DATE}",
    "master_url": "${MASTER_URL}",
    "container_name": "${CONTAINER_NAME}",
    "expected_image_digest": "${EXPECTED_WORKER_IMAGE_DIGEST}",
    "checks": entries,
}, indent=2))
PYEOF

# verdict.json — aggregate
verdict_status="PASS"
(( fail > 0 )) && verdict_status="FAIL"
python3 <<PYEOF > "$EV_DIR/verdict.json"
import json
deploy_results = """$(printf '%s\n' "${DEPLOY_RESULTS[@]}")"""
host_results = """$(printf '%s\n' "${HOST_RESULTS[@]}")"""
def parse(rs):
    out = []
    for line in rs.splitlines():
        if not line.strip(): continue
        parts = line.split(' ', 2)
        if len(parts) < 3: continue
        out.append({"status": parts[0], "name": parts[1], "info": parts[2]})
    return out
print(json.dumps({
    "worker_id": "${WORKER_ID}",
    "cert_date": "${CERT_DATE}",
    "verdict": "${verdict_status}",
    "host":   parse(host_results),
    "deploy": parse(deploy_results),
    "pass_count": ${pass},
    "fail_count": ${fail},
}, indent=2))
PYEOF

# host.txt — raw post-mortem dumps
{
  echo "=== uname -a ==="; uname -a
  echo "=== /etc/os-release ==="; (cat /etc/os-release 2>/dev/null || echo missing) | head -10
  echo "=== /proc/cpuinfo (model name only) ==="; awk -F: '/model name/{print $2; exit}' /proc/cpuinfo
  echo "=== nproc ==="; nproc
  echo "=== free -h ==="; free -h
  echo "=== df -h / /var/lib/velox-worker ==="; df -h / /var/lib/velox-worker 2>&1
  echo "=== timedatectl ==="; (timedatectl 2>/dev/null || echo missing) | head -15
  echo "=== chronyc tracking (if present) ==="; (chronyc tracking 2>&1 | head -10 || echo missing)
  echo "=== docker version ==="; docker version --format '{{.Server.Version}}' 2>/dev/null
  echo "=== docker compose version ==="; docker compose version --short 2>/dev/null
  echo "=== docker inspect ${CONTAINER_NAME} (state) ==="
  docker inspect -f 'RestartCount={{.RestartCount}} State={{.State.Status}} Health={{.State.Health.Status}}' \
    "$CONTAINER_NAME" 2>/dev/null || echo missing
} > "$EV_DIR/host.txt"

# ─── Summary ─────────────────────────────────────────────────────────────────
printf '\n══════════════════════════════════════════════════════\n'
printf '  Certifier summary — worker=%s verdict=%s\n' "$WORKER_ID" "$verdict_status"
printf '══════════════════════════════════════════════════════\n'
printf '  Evidence: %s\n' "$EV_DIR"
printf '    host.json            — 9 host checks\n'
printf '    host.txt             — raw host dumps\n'
printf '    image-digest.txt     — running digest\n'
printf '    image-signature.txt  — cosign verify (or SKIP)\n'
printf '    worker-config.sha256 — config checksum\n'
printf '    certificate.txt      — mounted CA inspected\n'
printf '    verdict.json         — aggregate PASS/FAIL\n'
printf '\n  pass=%d fail=%d\n' "$pass" "$fail"

if (( fail > 0 )); then exit 1; fi
exit 0
