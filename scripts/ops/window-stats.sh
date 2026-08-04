#!/usr/bin/env bash
# window-stats.sh — streamed over SSH (bash -s -- <unit> <date> <since_t> <until_t>).
# Args must be space-free (e.g. 2026-08-04 09:45:00 → date=2026-08-04,
# since_t=09:45:00, until_t=10:52:00). SSH re-splits remote args, so the
# space-separated journalctl timestamp is rebuilt INSIDE the remote shell.
set -u
U="$1"
D="$2"
SINCE_T="$3"
UNTIL_T="$4"
SINCE="$D $SINCE_T"
UNTIL="$D $UNTIL_T"

J="$(sudo -n journalctl -u "$U" --since "$SINCE" --until "$UNTIL" --no-pager 2>/dev/null || true)"
N="$(printf '%s' "$J" | grep -c . || true)"
echo "unit=$U window=$SINCE → $UNTIL lines=$N"
echo "--- first / last ts ---"
printf '%s' "$J" | grep -oE "^$D [0-9:]+" | head -1
printf '%s' "$J" | grep -oE "^$D [0-9:]+" | tail -1
echo "--- systemd lifecycle markers (counts) ---"
echo "started=$(printf '%s' "$J" | grep -c 'Started velox' || true)"
echo "exited=$(printf '%s' "$J" | grep -c 'Main process exited' || true)"
echo "restarts_sched=$(printf '%s' "$J" | grep -c 'Scheduled restart' || true)"
echo "--- app-level markers (counts) ---"
echo "boot_fail=$(printf '%s' "$J" | grep -c 'BOOTSTRAP_FAIL' || true)"
echo "baseline_mismatch=$(printf '%s' "$J" | grep -c 'engine_selftest_baseline_mismatch' || true)"
echo "cred_failed=$(printf '%s' "$J" | grep -c 'credential validation failed' || true)"
echo "circuit_open=$(printf '%s' "$J" | grep -c 'CIRCUIT_BREAKER.*open' || true)"
echo "config_error=$(printf '%s' "$J" | grep -c 'CONFIG_ERROR' || true)"
echo "--- distinct CONNECT worker_ids ---"
printf '%s' "$J" | grep -oE '\[[a-zA-Z0-9_-]+\] \[CONNECT\]' | sort -u | head
