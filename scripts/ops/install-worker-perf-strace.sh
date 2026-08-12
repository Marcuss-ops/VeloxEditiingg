#!/usr/bin/env bash
#
# install-worker-perf-strace.sh — install perf + strace on a single Velox
# worker for Phase 0 diagnostic profiling.
#
# Targets ONE diagnostic worker only. It streams a remote body to the host
# (same convention as scripts/ops/probe-worker-facts.sh), detects the distro,
# installs kernel perf tools + strace via apt, then verifies both binaries
# with a version check and a functional smoke test.
#
# Usage:
#   ./scripts/ops/install-worker-perf-strace.sh [ssh-host]
#
#   ssh-host   worker alias from ~/.ssh/config (default: velox-deb-57.131)
#
# Environment:
#   VELOX_PERF_PARANOID=1   raise kernel.perf_event_paranoid to 1 (sysctl)
#                           so perf can profile user processes. Default 0:
#                           read-only check, warning only.
#
# Prerequisites:
#   - ssh alias with passwordless login (see ~/.ssh/config)
#   - passwordless sudo on the target host
#   - apt-based distro (Ubuntu / Debian)
#
# Exit codes:
#   0  installed (or already present) and verified
#   1  install or verification failed on the host
#   2  usage error, unsupported distro, or host unreachable
set -Eeuo pipefail

HOST="${1:-velox-deb-57.131}"
PERF_PARANOID="${VELOX_PERF_PARANOID:-0}"

fail() { printf '[install-worker-perf-strace][ERROR] %s\n' "$*" >&2; exit 2; }
ok()   { printf '[install-worker-perf-strace][OK]   %s\n' "$*"; }
log()  { printf '[install-worker-perf-strace][..]   %s\n' "$*"; }

case "$HOST" in
  -h|--help|help)
    sed -n '2,26p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

command -v ssh >/dev/null 2>&1 || fail "ssh not found on PATH"

# Reachability check before streaming anything.
if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "$HOST" true 2>/dev/null; then
  fail "host $HOST unreachable via ssh — check the alias in ~/.ssh/config"
fi
log "host $HOST reachable, streaming install..."

# The remote body runs as root (passwordless sudo) and must stay self-contained:
# every variable, function and heredoc is streamed verbatim via the quoted
# 'REMOTE' delimiter, so nothing is expanded locally.
if ! PERF_PARANOID="$PERF_PARANOID" ssh -o BatchMode=yes "$HOST" "sudo bash -s" <<'REMOTE'
set -Eeuo pipefail

fail() { printf '[worker][ERROR] %s\n' "$*" >&2; exit 1; }
ok()   { printf '[worker][OK]   %s\n' "$*"; }

install_apt_packages() {
  local distro="$1"

  if command -v strace >/dev/null 2>&1; then
    ok "strace already installed: $(command -v strace)"
  else
    apt-get update -qq || fail "apt-get update failed"
    apt-get install -y --no-install-recommends strace >/dev/null || fail "apt-get install strace failed"
    ok "strace installed"
  fi

  if command -v perf >/dev/null 2>&1; then
    ok "perf already installed: $(command -v perf)"
    return
  fi

  apt-get update -qq || fail "apt-get update failed"
  local candidates=() pkg installed=""
  if [[ "$distro" == "ubuntu" ]]; then
    # Version-specific kernel tools may not exist for every running kernel;
    # fall back through the metapackage chain instead of failing hard.
    candidates=("linux-tools-$(uname -r)" "linux-tools-generic" "linux-tools-common")
  else
    candidates=("linux-perf")
  fi
  for pkg in "${candidates[@]}"; do
    if apt-get install -y --no-install-recommends "$pkg" >/dev/null 2>&1; then
      installed="$pkg"
      break
    fi
  done
  [[ -n "$installed" ]] || fail "could not install any perf package (tried: ${candidates[*]})"
  ok "perf installed via $installed"
}

verify_tools() {
  command -v perf >/dev/null 2>&1 || fail "perf still missing after install"
  command -v strace >/dev/null 2>&1 || fail "strace still missing after install"

  printf '[worker] %s\n' "$(perf --version)"
  printf '[worker] %s\n' "$(strace --version | head -1)"

  perf stat -- true >/dev/null 2>&1 || fail "perf functional smoke test failed (perf stat -- true)"
  ok "perf smoke test passed"

  strace -c -o /dev/null -- true 2>/dev/null || fail "strace functional smoke test failed"
  ok "strace smoke test passed"
}

tune_paranoid() {
  local current
  current="$(sysctl -n kernel.perf_event_paranoid 2>/dev/null || printf '%s' '-1')"
  if [[ "${PERF_PARANOID:-0}" == "1" ]]; then
    if [[ "$current" != "1" ]]; then
      sysctl -w kernel.perf_event_paranoid=1 >/dev/null || fail "could not set kernel.perf_event_paranoid=1"
      ok "kernel.perf_event_paranoid set to 1 (was $current)"
    else
      ok "kernel.perf_event_paranoid already 1"
    fi
  elif [[ -n "$current" && "$current" -ge 2 ]]; then
    printf '[worker][WARN] kernel.perf_event_paranoid=%s: perf profiling of other processes is restricted for non-root. Re-run with VELOX_PERF_PARANOID=1 to relax.\n' "$current"
  fi
}

[[ "$(id -u)" -eq 0 ]] || fail "remote body must run as root (sudo bash -s)"
. /etc/os-release || fail "cannot read /etc/os-release"

case "$ID" in
  ubuntu|debian)
    install_apt_packages "$ID"
    ;;
  *)
    fail "unsupported distro: $ID — only Ubuntu/Debian are supported"
    ;;
esac

verify_tools
tune_paranoid
ok "perf + strace installed and verified"
REMOTE
then
  fail "install or verification failed on $HOST (rc=$?)"
fi

ok "perf + strace installed and verified on $HOST"
printf '[install-worker-perf-strace][..]   next: run scripts/benchmarks/native-render-baseline.sh or attach the Phase-0 perf/strace wrapper via VELOX_VIDEO_ENGINE_CPP_BIN\n'
