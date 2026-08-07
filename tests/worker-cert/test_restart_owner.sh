#!/usr/bin/env bash
# Offline regression tests for tests/worker-cert/lib/restart_owner.sh.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tests/worker-cert/lib/restart_owner.sh
source "$ROOT/lib/restart_owner.sh"

valid_output=$'systemd_is_enabled=enabled\nsystemd_is_active=active\nsystemd_restart=always\nsystemd_restart_sec=10s\ndocker_restart_policy=no'
canonical_restart_owner_check_output "$valid_output" >/dev/null
[[ "$RESTART_OWNER_SYSTEMD_ENABLED" == enabled ]]
[[ "$RESTART_OWNER_SYSTEMD_ACTIVE" == active ]]
[[ "$RESTART_OWNER_SYSTEMD_RESTART" == always ]]
[[ "$RESTART_OWNER_SYSTEMD_RESTART_SEC" == 10s ]]
[[ "$RESTART_OWNER_DOCKER_POLICY" == no ]]

valid_output_seconds=$'systemd_is_enabled=enabled\nsystemd_is_active=active\nsystemd_restart=always\nsystemd_restart_sec=10\ndocker_restart_policy=no'
canonical_restart_owner_check_output "$valid_output_seconds" >/dev/null

for bad in \
  $'systemd_is_enabled=disabled\nsystemd_is_active=active\nsystemd_restart=always\nsystemd_restart_sec=10s\ndocker_restart_policy=no' \
  $'systemd_is_enabled=enabled\nsystemd_is_active=inactive\nsystemd_restart=always\nsystemd_restart_sec=10s\ndocker_restart_policy=no' \
  $'systemd_is_enabled=enabled\nsystemd_is_active=active\nsystemd_restart=on-failure\nsystemd_restart_sec=10s\ndocker_restart_policy=no' \
  $'systemd_is_enabled=enabled\nsystemd_is_active=active\nsystemd_restart=always\nsystemd_restart_sec=5s\ndocker_restart_policy=no' \
  $'systemd_is_enabled=enabled\nsystemd_is_active=active\nsystemd_restart=always\nsystemd_restart_sec=10s\ndocker_restart_policy=always'; do
  if canonical_restart_owner_check_output "$bad" >/dev/null 2>&1; then
    printf 'FAIL: invalid restart-owner fixture unexpectedly passed\n' >&2
    exit 1
  fi
done

canonical_restart_owner_check_command 'printf "%s\\n" "systemd_is_enabled=enabled" "systemd_is_active=active" "systemd_restart=always" "systemd_restart_sec=10s" "docker_restart_policy=no"' >/dev/null
if canonical_restart_owner_check_command 'exit 7' >/dev/null 2>&1; then
  printf 'FAIL: failing inspection command unexpectedly passed\n' >&2
  exit 1
fi
if canonical_restart_owner_check_command $'printf valid\ninvalid' >/dev/null 2>&1; then
  printf 'FAIL: multiline inspection command unexpectedly passed\n' >&2
  exit 1
fi

# The recovery runner must reject a bad restart-owner probe before it can
# submit a job or execute the operator stop command. Use a temporary readable
# DB/token so the runner reaches its preflight without contacting a Master.
tmp_dir="$(mktemp -d)"
trap 'rm -rf -- "$tmp_dir"' EXIT
: >"$tmp_dir/velox.db"
if VELOX_ADMIN_TOKEN=test VELOX_DB_PATH="$tmp_dir/velox.db" \
   VELOX_MASTER_URL=http://127.0.0.1:1 \
   RECOVERY_DESTINATION_ID=destination \
   bash "$ROOT/worker_offline_recovery.sh" \
     --target-worker-id worker-test \
     --target-worker-stop-cmd "touch '$tmp_dir/stop-ran'" \
     --target-worker-inspect-cmd "printf 'systemd_is_enabled=disabled\\n'" \
     >"$tmp_dir/recovery.log" 2>&1; then
  printf 'FAIL: invalid restart-owner preflight unexpectedly passed\n' >&2
  exit 1
fi
[[ ! -e "$tmp_dir/stop-ran" ]]
! grep -q 'submitted: job_id=' "$tmp_dir/recovery.log"
grep -q 'restart-owner contract preflight' "$tmp_dir/recovery.log"

printf 'PASS: restart-owner contract tests\n'
