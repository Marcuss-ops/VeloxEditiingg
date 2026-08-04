#!/usr/bin/env bash
# worker-failure-log.sh — streamed over SSH (bash -s -- <unit> <date> <since_t> <until_t>).
# Args must be space-free (date=2026-08-04, since_t=10:45:00, until_t=10:52:00);
# SSH re-splits remote args, so the space-separated journalctl timestamp is
# rebuilt INSIDE the remote shell.
# Extracts the FIRST real error from a worker unit's journal within a window,
# skipping systemd noise like "Main process exited, status=1".
set -u
UNIT="$1"
D="$2"
SINCE_T="$3"
UNTIL_T="$4"
SINCE="$D $SINCE_T"
UNTIL="$D $UNTIL_T"

J="$(sudo -n journalctl -u "$UNIT" --since "$SINCE" --until "$UNTIL" --no-pager 2>/dev/null || true)"

echo "=== WINDOW: $SINCE → $UNTIL (unit $UNIT) ==="
echo "--- total lines: $(printf '%s' "$J" | grep -c .) ---"

echo "--- FIRST 15 lines (chronological) ---"
printf '%s' "$J" | head -15

echo
echo "--- FIRST REAL ERROR (skipping 'Main process exited' / systemd[1] boilerplate) ---"
FIRST_REAL="$(printf '%s' "$J" \
  | grep -E 'Error:|error:|\[ERROR\]|FATAL|fatal|panic:|refused|timed out|timeout|mismatch|not found|No such|invalid|denied|failed to|unexpected|credential|secret|bootstrap gate|CIRCUIT_BREAKER' \
  | grep -vE 'Main process exited|systemd\[1\]|Failed with result|systemd: Failed' \
  | head -1)"
if [ -n "$FIRST_REAL" ]; then
  echo "$FIRST_REAL"
else
  echo "<none found — no application-level error in window>"
fi

echo
echo "--- BOOTSTRAP GATE lines (selftest/baseline/BOOTSTRAP/verdict) ---"
printf '%s' "$J" | grep -iE 'bootstrap|selftest|baseline|BOOTSTRAP_REPORT|engine_self_render|verdict' | head -12

echo
echo "--- app-level markers (counts) ---"
echo "boot_fail=$(printf '%s' "$J" | grep -c 'BOOTSTRAP_FAIL' || true)"
echo "baseline_mismatch=$(printf '%s' "$J" | grep -c 'engine_selftest_baseline_mismatch' || true)"
echo "cred_failed=$(printf '%s' "$J" | grep -c 'credential validation failed' || true)"
echo "circuit_open=$(printf '%s' "$J" | grep -c 'CIRCUIT_BREAKER.*open' || true)"
echo "config_error=$(printf '%s' "$J" | grep -c 'CONFIG_ERROR' || true)"
