#!/usr/bin/env bash
# Read-only remote probe for worker_offline_recovery.sh.
# Emits only restart-owner metadata; it never starts, stops, or restarts a unit.

set -euo pipefail

unit=velox-worker.service
container=velox-worker

printf 'systemd_is_enabled=%s\n' "$(systemctl is-enabled "$unit" 2>/dev/null)"
printf 'systemd_is_active=%s\n' "$(systemctl is-active "$unit" 2>/dev/null)"
printf 'systemd_restart=%s\n' "$(sudo -n systemctl show "$unit" -p Restart --value 2>/dev/null)"
printf 'systemd_restart_sec=%s\n' "$(sudo -n systemctl show "$unit" -p RestartUSec --value 2>/dev/null)"
printf 'docker_restart_policy=%s\n' "$(docker inspect -f '{{.HostConfig.RestartPolicy.Name}}' "$container" 2>/dev/null)"
