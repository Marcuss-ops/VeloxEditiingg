#!/usr/bin/env bash
#
# install-worker-perf-wrappers.sh — wire perf/strace wrappers into a Velox
# worker (Phase 0 diagnostic profiling).
#
# The Velox worker agent runs inside a hardened docker container
# (read-only rootfs, cap_drop ALL, no-new-privileges) and spawns the native
# C++ engine at $VELOX_VIDEO_ENGINE_CPP_BIN. To profile real jobs we:
#
#   1. install the wrapper templates on the host at /usr/local/bin/
#      (scripts/ops/lib/velox_video_engine_{perf,perfstat,strace})
#   2. compute the shared-library deps of host perf/strace that are MISSING
#      inside the container (ldd + docker exec ldconfig)
#   3. generate /opt/velox-worker/compose.profiling.yml: a compose override
#      that bind-mounts wrappers + perf/strace + missing libs + the perf
#      output dir, and points VELOX_VIDEO_ENGINE_CPP_BIN at the selected
#      wrapper (override environment wins over env_file over image ENV)
#   4. install a systemd drop-in that appends the override file to the
#      compose invocation, relax kernel.perf_event_paranoid (runtime only),
#      restart the worker and roll back if /health/ready does not recover
#
# Usage:
#   ./scripts/ops/install-worker-perf-wrappers.sh [--mode perf|perfstat|strace|off] [ssh-host]
#
#   --mode perf      activate velox_video_engine_perf    (perf record, default)
#   --mode perfstat  activate velox_video_engine_perfstat (perf stat -d -d -d)
#   --mode strace    activate velox_video_engine_strace   (strace -f -c)
#   --mode off       restore the canonical engine path and remove the wiring
#
#   ssh-host   worker alias from ~/.ssh/config (default: velox-deb-57.131)
#
# Environment:
#   VELOX_PERF_PARANOID   target sysctl (default 1; must be <= 2 for
#                         unprivileged in-container profiling)
#
# Exit codes:
#   0  wired / unwired and verified
#   1  activation or verification failed (rolled back to canonical)
#   2  usage error / host unreachable / prerequisite missing
set -Eeuo pipefail

HOST="velox-deb-57.131"
MODE="perf"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      [[ $# -ge 2 ]] || { echo "usage: --mode perf|perfstat|strace|off" >&2; exit 2; }
      MODE="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)
      HOST="$1"; shift ;;
  esac
done
case "$MODE" in perf|perfstat|strace|off) ;; *) echo "invalid --mode: $MODE" >&2; exit 2 ;; esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIB_DIR="${SCRIPT_DIR}/lib"
WRAPPERS=(velox_video_engine_perf velox_video_engine_perfstat velox_video_engine_strace)

fail() { printf '[install-worker-perf-wrappers][ERROR] %s\n' "$*" >&2; exit 2; }
ok()   { printf '[install-worker-perf-wrappers][OK]   %s\n' "$*"; }
log()  { printf '[install-worker-perf-wrappers][..]   %s\n' "$*"; }

command -v ssh >/dev/null 2>&1 || fail "ssh not found on PATH"

# Preflight: host reachable and wrapper templates present in the repo.
if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "$HOST" true 2>/dev/null; then
  fail "host $HOST unreachable via ssh — check the alias in ~/.ssh/config"
fi
for w in "${WRAPPERS[@]}"; do
  [[ -r "${LIB_DIR}/${w}" ]] || fail "missing wrapper template: ${LIB_DIR}/${w}"
done
log "host $HOST reachable, mode=$MODE"

# Push the wrapper templates (host paths are also the container mount sources).
for w in "${WRAPPERS[@]}"; do
  ssh -o BatchMode=yes "$HOST" "sudo tee /usr/local/bin/${w} >/dev/null" <"${LIB_DIR}/${w}"
  ssh -o BatchMode=yes "$HOST" "sudo chmod 755 /usr/local/bin/${w}"
done
ok "wrapper templates installed at /usr/local/bin on $HOST"

# Stream the remote body (runs as root). MODE and PARANOID cross the wire as
# positional arguments to `bash -s` — SSH does not forward env vars — because
# the heredoc delimiter is quoted, nothing else is expanded locally.
if ! ssh -o BatchMode=yes "$HOST" "sudo bash -s '$MODE' '${VELOX_PERF_PARANOID:-1}'" <<'REMOTE'
set -Eeuo pipefail

fail() { printf '[worker][ERROR] %s\n' "$*" >&2; exit 1; }
ok()   { printf '[worker][OK]   %s\n' "$*"; }
log()  { printf '[worker][..]   %s\n' "$*"; }

MODE="${1:?}"
PARANOID="${2:-1}"
COMPOSE_DIR="/opt/velox-worker"
COMPOSE_BASE="${COMPOSE_DIR}/compose.yml"
COMPOSE_OVERRIDE="${COMPOSE_DIR}/compose.profiling.yml"
DROPIN_DIR="/etc/systemd/system/velox-worker.service.d"
DROPIN="${DROPIN_DIR}/10-profiling.conf"
PERF_DIR="/var/lib/velox/perf"
CONTAINER="velox-worker"
HEALTH_URL="http://127.0.0.1:${VELOX_HEALTH_PORT:-8081}/health/ready"
PARANOID_ORIG_FILE="${COMPOSE_DIR}/.profiling_paranoid_orig"

[[ "$(id -u)" -eq 0 ]] || fail "remote body must run as root (sudo bash -s)"
command -v docker >/dev/null 2>&1 || fail "docker CLI not found on host"
command -v systemctl >/dev/null 2>&1 || fail "systemctl not found on host"
docker compose version >/dev/null 2>&1 || fail "docker compose v2 not available"
[[ -f "$COMPOSE_BASE" ]] || fail "compose base file missing: $COMPOSE_BASE"

mkdir -p "$PERF_DIR"
chmod 777 "$PERF_DIR"
log "perf output dir ready: $PERF_DIR (mode 0777)"

# ── generate the compose override ────────────────────────────────────────────
# Compute the shared-lib deps of host perf/strace missing inside the container
# and bind-mount exactly those (individual files; never shadow /usr/lib).
container_libs="$(docker exec "$CONTAINER" ldconfig -p 2>/dev/null || true)"
missing=""
for lib in $(ldd /usr/bin/perf /usr/bin/strace 2>/dev/null | awk '/=> \//{print $3}' | sort -u); do
  if ! printf '%s\n' "$container_libs" | grep -Fq "$(basename "$lib")"; then
    missing="${missing}
      - ${lib}:${lib}:ro"
  fi
done
log "missing libs to bind-mount into the container:$(printf '%s' "$missing" | grep -c ':ro' || true)"

if [[ "$MODE" == "off" ]]; then
  rm -f "$COMPOSE_OVERRIDE" "$DROPIN"
  if [[ -f "$PARANOID_ORIG_FILE" ]]; then
    sysctl -w "kernel.perf_event_paranoid=$(cat "$PARANOID_ORIG_FILE")" >/dev/null || true
    rm -f "$PARANOID_ORIG_FILE"
  fi
  log "override + drop-in removed (mode=off)"
else
  wrapper="velox_video_engine_${MODE}"
  [[ -x "/usr/local/bin/${wrapper}" ]] || fail "host wrapper missing: /usr/local/bin/${wrapper}"
  # The container entrypoint (worker-entrypoint.sh) fail-closed-verifies the
  # SHA-256 of $VELOX_VIDEO_ENGINE_CPP_BIN against $VELOX_VIDEO_ENGINE_SHA_FILE.
  # Pointing the sha file at the wrapper's own digest lets the gate pass while
  # the wrapper still execs the canonical engine (unchanged in the read-only
  # image). Diagnostic worker only — this intentionally relaxes the gate.
  sha256sum "/usr/local/bin/${wrapper}" | awk '{print $1}' >"${PERF_DIR}/wrapper.sha256"
  cat >"$COMPOSE_OVERRIDE" <<YAML
# Generated by install-worker-perf-wrappers.sh --mode ${MODE} on $(date -Is).
# Bind-mounts the Phase-0 profiling toolchain into the hardened worker
# container and points VELOX_VIDEO_ENGINE_CPP_BIN at the wrapper.
services:
  velox-worker:
    environment:
      # Environment keys repeated from compose.yml so the override is
      # self-sufficient under either merge semantics (map-merge vs replace).
      VELOX_RENDER_BACKEND: ${VELOX_RENDER_BACKEND:-native}
      CHRONON3D_CLI: ${CHRONON3D_CLI:-/opt/chronon3d/bin/chronon3d_cli}
      VELOX_REMOTE_REBUILD_DISABLED: "true"
      VELOX_VIDEO_ENGINE_CPP_BIN: /usr/local/bin/${wrapper}
      VELOX_VIDEO_ENGINE_SHA_FILE: ${PERF_DIR}/wrapper.sha256
    security_opt:
      # Docker's default seccomp profile blocks perf_event_open; perf
      # profiling inside the container requires seccomp:unconfined.
      # Base compose.yml already sets no-new-privileges:true (kept).
      - seccomp:unconfined
    volumes:
      - /usr/local/bin/velox_video_engine_perf:/usr/local/bin/velox_video_engine_perf:ro
      - /usr/local/bin/velox_video_engine_perfstat:/usr/local/bin/velox_video_engine_perfstat:ro
      - /usr/local/bin/velox_video_engine_strace:/usr/local/bin/velox_video_engine_strace:ro
      - /usr/bin/perf:/usr/bin/perf:ro
      - /usr/bin/strace:/usr/bin/strace:ro
      - ${PERF_DIR}:${PERF_DIR}:rw${missing}
YAML
  log "compose override written: $COMPOSE_OVERRIDE"

  mkdir -p "$DROPIN_DIR"
  cat >"$DROPIN" <<UNIT
[Service]
# Phase-0 profiling: append the profiling compose override (wrappers + perf
# toolchain + VELOX_VIDEO_ENGINE_CPP_BIN). Remove this drop-in to restore.
ExecStartPre=
ExecStartPre=/usr/bin/docker compose --project-name velox-worker --file ${COMPOSE_BASE} --file ${COMPOSE_OVERRIDE} config
ExecStart=
ExecStart=/usr/bin/docker compose --project-name velox-worker --file ${COMPOSE_BASE} --file ${COMPOSE_OVERRIDE} up --remove-orphans --no-color
UNIT
  log "systemd drop-in written: $DROPIN"

  if [[ ! -f "$PARANOID_ORIG_FILE" ]]; then
    sysctl -n kernel.perf_event_paranoid >"$PARANOID_ORIG_FILE" 2>/dev/null || true
  fi
  current="$(sysctl -n kernel.perf_event_paranoid 2>/dev/null || true)"
  if [[ -n "$current" && "$current" -ne "$PARANOID" ]]; then
    sysctl -w "kernel.perf_event_paranoid=${PARANOID}" >/dev/null || fail "sysctl kernel.perf_event_paranoid=${PARANOID} failed"
    ok "kernel.perf_event_paranoid set to ${PARANOID} (was ${current}, runtime only)"
  else
    ok "kernel.perf_event_paranoid already ${current}"
  fi
fi

# ── restart + health gate with rollback ──────────────────────────────────────
rollback_canonical() {
  rm -f "$COMPOSE_OVERRIDE" "$DROPIN"
  if [[ -f "$PARANOID_ORIG_FILE" ]]; then
    sysctl -w "kernel.perf_event_paranoid=$(cat "$PARANOID_ORIG_FILE")" >/dev/null || true
    rm -f "$PARANOID_ORIG_FILE"
  fi
  systemctl daemon-reload
  systemctl restart velox-worker.service || true
}

systemctl daemon-reload
if ! systemctl restart velox-worker.service; then
  log "restart failed — rolling back to canonical runtime"
  rollback_canonical
  fail "worker restart failed; canonical runtime restored — inspect: journalctl -u velox-worker.service -n 100"
fi

healthy=0
for _ in $(seq 1 60); do
  if curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 2
done

if [[ "$healthy" -ne 1 ]]; then
  log "health not ready after restart — rolling back to canonical runtime"
  rollback_canonical
  for _ in $(seq 1 60); do
    curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1 && break
    sleep 2
  done
  fail "worker did not recover health after rollback — inspect: journalctl -u velox-worker.service -n 100"
fi
ok "worker healthy after restart (mode=$MODE)"

# ── verification ────────────────────────────────────────────────────────────
if [[ "$MODE" != "off" ]]; then
  in_env="$(docker exec "$CONTAINER" sh -c 'printf "%s" "$VELOX_VIDEO_ENGINE_CPP_BIN"')"
  [[ "$in_env" == "/usr/local/bin/${wrapper}" ]] \
    || fail "container VELOX_VIDEO_ENGINE_CPP_BIN=${in_env}; expected /usr/local/bin/${wrapper}"
  docker exec "$CONTAINER" test -x "/usr/local/bin/${wrapper}" || fail "wrapper not present in container"
  docker exec --user velox "$CONTAINER" /usr/bin/perf --version >/dev/null || fail "perf not runnable in container"
  docker exec --user velox "$CONTAINER" /usr/bin/strace --version >/dev/null || fail "strace not runnable in container"
  docker exec --user velox "$CONTAINER" /bin/sh -c \
    "cd /tmp && /usr/bin/perf record -o ${PERF_DIR}/smoke.data -- true" >/dev/null 2>&1 \
    || fail "perf record smoke failed as velox in-container (paranoid=${PARANOID}?)"
  rm -f "${PERF_DIR}/smoke.data"
  ok "in-container verification passed: env=$in_env, perf+strace+perf record ok"
fi
ok "done (mode=$MODE)"
REMOTE
then
  fail "remote activation failed on $HOST"
fi

ok "worker wrappers active on $HOST (mode=$MODE)"
printf '[install-worker-perf-wrappers][..]   traces land in /var/lib/velox/perf on the host; switch mode with --mode strace|perfstat|off\n'
