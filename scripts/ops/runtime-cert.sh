#!/usr/bin/env bash
# scripts/ops/runtime-cert.sh — Fase 2 runtime certification for a worker.
#
# Collects, for a single worker host (SSH) + the master REST record:
#   worker_id, host, container ID, image tag, image digest (running + master
#   registered), bundle hash (env + on-disk), binary version, engine version,
#   systemd active state, active since, NRestarts, restart-in-last-N-min,
#   bootstrap gate ([BOOTSTRAP_REPORT] verdict + engine selftest baseline),
#   registered/session_active from the master.
#
# Usage:
#   scripts/ops/runtime-cert.sh <worker_id> <host> <ssh_user> [master_host] [master_user]
#   scripts/ops/runtime-cert.sh --fleet                    # all 4 workers
#   scripts/ops/runtime-cert.sh --help
#
# Env overrides:
#   RESTART_WINDOW_S=300   restart-stability window (default 300s)
#   MASTER_HOST / MASTER_USER  (defaults: 51.91.11.36 / pierone)
#
# Output: JSON on stdout (one document per worker, no secrets).
# Exit:   0 = all checks green, 1 = at least one FAIL, 2 = usage.
# =============================================================================

set -uo pipefail

SSH_COMMON=(-i "$HOME/.ssh/id_ed25519_velox" -o StrictHostKeyChecking=accept-new \
  -o BatchMode=yes -o ConnectTimeout=8 -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)

MASTER_HOST="${MASTER_HOST:-51.91.11.36}"
MASTER_USER="${MASTER_USER:-pierone}"
RESTART_WINDOW_S="${RESTART_WINDOW_S:-300}"

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

if [[ "${1:-}" == "--fleet" ]]; then
  rc=0
  while read -r wid host user; do
    echo "════════════════════════════════════════════════════════════"
    echo "CERT $wid ($host)"
    echo "════════════════════════════════════════════════════════════"
    "$0" "$wid" "$host" "$user" || rc=1
  done <<'FLEET'
host_57_129_132_133 57.129.132.133 pierone
host_57_131_20_173 57.131.20.173 pierone
velox-worker-13197 149.56.131.97 pierone
velox-worker-523925eb 51.222.204.158 ubuntu
FLEET
  exit "$rc"
fi

WORKER_ID="${1:-}"
HOST="${2:-}"
SSH_USER="${3:-}"
if [[ -z "$WORKER_ID" || -z "$HOST" || -z "$SSH_USER" ]]; then
  echo "usage: runtime-cert.sh <worker_id> <host> <ssh_user> | --fleet | --help" >&2
  exit 2
fi

# ─── Worker host facts (heredoc over SSH — no quoting pitfalls) ──────────────
HOST_JSON="$(ssh "${SSH_COMMON[@]}" "$SSH_USER@$HOST" bash -s -- "$WORKER_ID" "$RESTART_WINDOW_S" <<'REMOTE'
set -u
WID="$1"
WIN_S="$2"

# Canonical short name first (exact match), then active/activating loaded
# unit, then any loaded worker unit. Excludes ghost not-found units and
# stale double-prefix units from bad deploys.
SHORT="${WID#velox-worker-}"
unit="$(systemctl list-units --type=service --all --no-legend 2>/dev/null \
  | awk -v u="velox-worker-worker_${SHORT}.service" '$2=="loaded" && $1==u {print $1; exit}')"
[ -z "$unit" ] && unit="$(systemctl list-units --type=service --all --no-legend 2>/dev/null \
  | awk '$2=="loaded" && ($3=="active" || $3=="activating") && $1 ~ /^velox-worker-/ && $1 !~ /auto-update|watchdog|\.mount|worker_velox-worker/ {print $1; exit}')"
[ -z "$unit" ] && unit="$(systemctl list-units --type=service --all --no-legend 2>/dev/null \
  | awk '$2=="loaded" && $1 ~ /^velox-worker-worker_/ && $1 !~ /worker_velox-worker/ {print $1; exit}')"
[ -z "$unit" ] && unit="velox-worker-worker_${SHORT}.service"

active="$(systemctl is-active "$unit" 2>/dev/null || echo unknown)"
active_since="$(systemctl show "$unit" -p ActiveEnterTimestamp --value 2>/dev/null)"
n_restarts="$(systemctl show "$unit" -p NRestarts --value 2>/dev/null || echo 0)"
recent_restarts="$(journalctl -u "$unit" --since "-$WIN_S seconds" --no-pager 2>/dev/null | grep -c 'Started Velox Worker' || true)"

# Canonical per-host env file: prefer the file referenced by ExecStart, else
# the canonical /etc/velox-worker-worker_<short>.env.
env_file="$(systemctl show "$unit" -p ExecStart --value 2>/dev/null \
  | grep -oE -- '--env-file [^ ]+' | awk '{print $2}' | head -1)"
[ -z "$env_file" ] && env_file="$(ls /etc/velox-worker-worker_*.env 2>/dev/null | grep -F "$(echo "$unit" | sed 's/^velox-worker-//; s/\.service$//')" | head -1)"
[ -z "$env_file" ] && env_file="/etc/velox-worker-worker_${WID}.env"

env_bundle_hash="$(sudo -n grep -E '^VELOX_BUNDLE_HASH=' "$env_file" 2>/dev/null | cut -d= -f2-)"
env_bundle_ver="$(sudo -n grep -E '^VELOX_BUNDLE_VERSION=' "$env_file" 2>/dev/null | cut -d= -f2-)"
env_image="$(sudo -n grep -E '^VELOX_WORKER_IMAGE=' "$env_file" 2>/dev/null | cut -d= -f2-)"

cid="$(sudo -n docker ps --format '{{.ID}} {{.Names}}' 2>/dev/null | grep 'velox-worker' | awk '{print $1}' | head -1)"
img_tag="$(sudo -n docker inspect --format '{{.Config.Image}}' "$cid" 2>/dev/null || echo unknown)"
img_id="$(sudo -n docker inspect --format '{{.Image}}' "$cid" 2>/dev/null || echo unknown)"
running_digest="$(sudo -n docker inspect --format '{{index .RepoDigests 0}}' "$cid" 2>/dev/null | sed 's|.*@||')"

bundle_disk="$(cat /opt/velox/current/RemoteCodex/BUNDLE_HASH.txt 2>/dev/null | tr -d '[:space:]')"
[ -z "$bundle_disk" ] && bundle_disk="$(cat /opt/velox-worker/BUNDLE_HASH.txt 2>/dev/null | tr -d '[:space:]')"

baseline_file="$(find /var/lib/velox/workers -name engine_selftest_baseline.sha256 2>/dev/null | head -1)"
baseline_content="$(cat "$baseline_file" 2>/dev/null | awk '{print $1}')"
engine_sha="$(sha256sum /usr/local/bin/velox_video_engine 2>/dev/null | awk '{print $1}')"

jl="$(sudo -n journalctl -u "$unit" --no-pager 2>/dev/null || journalctl -u "$unit" --no-pager 2>/dev/null || true)"
boot_verdict="$(printf '%s' "$jl" | grep -oE '"verdict": ?"(OK|READY|FAIL)"' | tail -1 | grep -oE '(OK|READY|FAIL)')"
boot_fail="$(printf '%s' "$jl" | grep -oE 'bootstrap gate failed[^"]*' | tail -1)"

bin_ver="$(sudo -n docker exec "$cid" sh -c 'cat /app/VERSION.txt 2>/dev/null' 2>/dev/null)"
container_bundle="$(sudo -n docker exec "$cid" sh -c 'cat /app/RemoteCodex/BUNDLE_HASH.txt 2>/dev/null' 2>/dev/null | tr -d '[:space:]')"

python3 -c '
import json,sys
h = {
  "unit": sys.argv[1], "active": sys.argv[2], "active_since": sys.argv[3],
  "n_restarts": sys.argv[4], "restarts_in_"+sys.argv[10]+"s": sys.argv[5],
  "container_id": sys.argv[6], "image_tag": sys.argv[7], "image_id": sys.argv[8],
  "running_digest": sys.argv[21], "env_file": sys.argv[9],
  "env_bundle_hash": sys.argv[11], "env_bundle_version": sys.argv[12],
  "env_image": sys.argv[13], "bundle_disk": sys.argv[14],
  "baseline_file": sys.argv[15], "baseline_content": sys.argv[16],
  "engine_bin_sha": sys.argv[17], "boot_verdict": sys.argv[18],
  "boot_fail": sys.argv[19], "bin_ver": sys.argv[20], "container_bundle": sys.argv[22],
}
print(json.dumps(h))
' "$unit" "$active" "$active_since" "$n_restarts" "$recent_restarts" \
  "$cid" "$img_tag" "$img_id" "$env_file" "$WIN_S" \
  "$env_bundle_hash" "$env_bundle_ver" "$env_image" "$bundle_disk" \
  "$baseline_file" "$baseline_content" "$engine_sha" "$boot_verdict" "$boot_fail" "$bin_ver" \
  "$running_digest" "$container_bundle"
REMOTE
)" || HOST_JSON='{"error":"ssh host failed"}'

# ─── Master-side record (admin WorkerCard via sudo on master host) ──────────
MASTER_JSON="$(ssh "${SSH_COMMON[@]}" "$MASTER_USER@$MASTER_HOST" bash -s -- "$WORKER_ID" <<'REMOTE'
set -u
WID="$1"
tok="$(sudo -n grep -oE '^VELOX_ADMIN_TOKEN=.*' /etc/velox-server.env 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'")"
port="$(sudo -n grep -oE '^VELOX_MASTER_PORT=[0-9]+' /etc/velox-server.env 2>/dev/null | cut -d= -f2)"
[ -z "$port" ] && port=8000
curl -sS -m 10 -H "Authorization: Bearer $tok" "http://127.0.0.1:$port/api/v1/admin/workers/$WID" 2>/dev/null \
  | jq -c '{worker_id, status, session_active, image_digest, software_version, desired_version, health, last_heartbeat_at, last_restart_at, deployment_state, active_jobs, max_active_jobs, last_smoke_status, last_smoke_at, executor, executor_version}'
REMOTE
)" || MASTER_JSON='{"error":"ssh master failed"}'

# ─── Verdict ─────────────────────────────────────────────────────────────────
python3 - "$WORKER_ID" "$HOST" "$RESTART_WINDOW_S" "$HOST_JSON" "$MASTER_JSON" <<'PY'
import json, sys, datetime

worker_id, host, win_s = sys.argv[1], sys.argv[2], int(sys.argv[3])
try:
    h = json.loads(sys.argv[4])
except Exception:
    h = {"error": sys.argv[4][:200]}
try:
    m = json.loads(sys.argv[5])
except Exception:
    m = {"error": sys.argv[5][:200]}

checks = []

def check(name, ok, detail):
    checks.append({"name": name, "pass": bool(ok), "detail": detail})

active = h.get("active")
check("service_active", active == "active", f"systemd active={active} (unit={h.get('unit')})")
check("session_active", m.get("session_active") is True, f"master session_active={m.get('session_active')} status={m.get('status')}")
check("registered", m.get("status") == "CONNECTED", f"master status={m.get('status')}")

try:
    recent = int(h.get(f"restarts_in_{win_s}s", "0"))
except (TypeError, ValueError):
    recent = -1
check("no_restart_5min", recent == 0, f"restarts in last {win_s}s={recent} NRestarts={h.get('n_restarts')}")

boot_verdict = (h.get("boot_verdict") or "").upper()
boot_fail = h.get("boot_fail") or ""
check("bootstrap_gate_pass", ("OK" in boot_verdict or "READY" in boot_verdict) and not boot_fail,
      f"boot_verdict={boot_verdict or '<none>'} boot_fail={boot_fail or '<none>'}")

registered_digest = m.get("image_digest") or ""
check("image_digest_pinned", len(registered_digest) == 64,
      f"master image_digest={registered_digest[:24]}… ({len(registered_digest)} chars)")

# bundle coherence: env == disk == container (when container is up)
env_bh = h.get("env_bundle_hash") or ""
disk_bh = h.get("bundle_disk") or ""
ctr_bh = h.get("container_bundle") or ""
coherent = bool(env_bh) and env_bh == disk_bh and (not ctr_bh or ctr_bh == env_bh)
check("bundle_hash_coherent", coherent,
      f"env={env_bh[:36]}… disk={disk_bh[:36]}… container={ctr_bh[:36]}…")

all_ok = all(c["pass"] for c in checks)
report = {
    "worker_id": worker_id,
    "host": host,
    "checked_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "restart_window_s": win_s,
    "host_facts": h,
    "master_record": m,
    "checks": checks,
    "verdict": "PASS" if all_ok else "FAIL",
}
print(json.dumps(report, indent=2))
sys.exit(0 if all_ok else 1)
PY
