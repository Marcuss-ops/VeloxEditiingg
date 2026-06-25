#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/check-share-cert.sh — RW-PROD-001 §3 A7 anti-condivisione cert
# ─────────────────────────────────────────────────────────────────────────────
# Caught duplicate worker.crt files in the Ansible inventory or in a flat
# worker-certs directory. Two physical hosts sharing the SAME client cert
# is an identity-collision failure mode: both can claim to be each other's
# worker_id and the master cannot attribute work correctly.
#
# Modes:
#   dir <DIR>            scan <DIR>/**/*.crt for duplicate SHA-256 fingerprints
#   inventory <FILE>     FILE is a plain list "<host>:<crt-path>" entries
#
# Exit codes:
#   0   OK — every worker cert is unique by fingerprint
#   1   DUPLICATE — at least two hosts share the same cert
#   2   USAGE/INPUT — invalid args or missing path / unreadable input
#
# Output:
#   * To stdout (one line per dup pair when found)
#   * The structured evidence file (when --json <PATH> is given) contains the
#     full {fingerprint_sha256, serial, hosts[]} mapping so ops can decide
#     whether to revoke, rotate, or move and re-issue.
#
# This script is intentionally pure POSIX bash + openssl (no jq dependency
# for the plain output). Python is only invoked for --json mode.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

OPENSSL="${OPENSSL:-openssl}"
JSON_OUT=""

usage() {
  cat <<USAGE
Usage: $0 [dir|inventory] <TARGET> [--json <evidences.json>]

Modes:
  dir <DIR>             Scan every *.crt under <DIR> (recursive).
  inventory <FILE>      FILE is "<host>:<abs-path-to-worker.crt>" lines, blanks and
                        lines starting with '#' ignored.

Examples:
  $0 dir /opt/velox/certs/workers
  $0 inventory deploy/inventory/hosts.ini --json ops/rw-prod-001-cert-sharing.json
  find /opt/velox/certs/workers -name worker*.crt | \\
    $0 inventory /dev/stdin

Exit codes: 0 OK | 1 DUPLICATE | 2 USAGE/INPUT
USAGE
  exit 2
}

[[ $# -ge 2 ]] || usage
MODE="$1"; shift
TARGET="$1"; shift

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json) JSON_OUT="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "[check-share-cert] unknown flag: $1" >&2; usage ;;
  esac
done

command -v "$OPENSSL" >/dev/null 2>&1 || {
  echo "[check-share-cert] FATAL: openssl not found at '$OPENSSL'" >&2; exit 2; }

# ─── collect (host, fp, serial) triples ─────────────────────────────────────
collect_dir() {
  local dir="$1"
  [[ -d "$dir" ]] || { echo "[check-share-cert] FATAL: dir $dir not found" >&2; exit 2; }
  # Use -print0 + IFS read so paths with spaces/special chars don't break the loop.
  find "$dir" -type f -name '*.crt' -print0 | while IFS= read -r -d '' crt; do
    local fp serial
    fp="$("$OPENSSL" x509 -in "$crt" -noout -fingerprint -sha256 2>/dev/null | cut -d'=' -f2)"
    serial="$("$OPENSSL" x509 -in "$crt" -noout -serial 2>/dev/null | cut -d'=' -f2)"
    [[ -z "$fp" ]] && {
      echo "[check-share-cert] WARN: $crt unreadable (fingerprint empty); skipping" >&2; continue; }
    local host
    host="$(basename "$crt" .crt)"
    printf '%s\t%s\t%s\n' "$host" "$fp" "$serial"
  done
}

collect_inventory() {
  local file="$1"
  [[ -f "$file" ]] || { echo "[check-share-cert] FATAL: inventory $file not found" >&2; exit 2; }
  while IFS= read -r line; do
    line="${line%%#*}"             # strip comments
    line="${line%%$'\r'}"          # strip CR (Windows-edited inventory)
    [[ -z "$line" ]] && continue
    if [[ "$line" != *:* ]]; then
      echo "[check-share-cert] WARN: malformed line (no 'host:path'): $line" >&2; continue
    fi
    local host="${line%%:*}" path="${line#*:}"
    [[ -f "$path" ]] || {
      echo "[check-share-cert] WARN: cert $path for host $host missing on disk; skipping" >&2
      continue
    }
    local fp serial
    fp="$("$OPENSSL" x509 -in "$path" -noout -fingerprint -sha256 2>/dev/null | cut -d'=' -f2)"
    serial="$("$OPENSSL" x509 -in "$path" -noout -serial 2>/dev/null | cut -d'=' -f2)"
    [[ -z "$fp" ]] && {
      echo "[check-share-cert] WARN: $path unreadable (fingerprint empty); skipping" >&2; continue; }
    printf '%s\t%s\t%s\n' "$host" "$fp" "$serial"
  done < "$file"
}

TMP="$(mktemp -t check-share-cert.XXXXXX)"
trap 'rm -f "$TMP" "$TMP.grouped"' EXIT

case "$MODE" in
  dir)       collect_dir "$TARGET"      > "$TMP" ;;
  inventory) collect_inventory "$TARGET" > "$TMP" ;;
  *) usage ;;
esac

# Group by fingerprint (col 2). AWK does the heavy lifting: collect hosts
# per fp, then emit each fp that has >=2 hosts.
awk -F'\t' '
  {
    host=$1; fp=$2; serial=$3
    n[fp]++
    serials[fp]=serial
    hosts[fp]=hosts[fp] " " host
  }
  END {
    rc=0
    for (fp in n) {
      if (n[fp] > 1) {
        printf "DUPLICATE\t%s\t%s\thosts:%s\n", fp, serials[fp], hosts[fp] > "/dev/stderr"
        rc=1
        printf "%s\t%s\t%s\n", fp, serials[fp], hosts[fp] > "/tmp/check-share-cert.grouped_actual"
      }
    }
    # Total scanned for the JSON output side.
    print NR > "/tmp/check-share-cert.count_actual"
    exit rc
  }
' "$TMP" 2>"$TMP.grouped" || rc=1 || true
awk_rc=$?

# awk's `exit rc` is reported via WEXITSTATUS but this bash is overcomplicated
# by our out-of-band writes; use the legacy approach: count DUPLICATE lines.
dup_count="$(grep -c '^DUPLICATE' "$TMP.grouped" 2>/dev/null || echo 0)"
total_count="$(wc -l < "$TMP" | tr -d ' ')"

echo "[check-share-cert] scanned $total_count cert(s); duplicates=$dup_count"
if [[ "$dup_count" -gt 0 ]]; then
  echo "[check-share-cert] FAIL — the following fingerprints are SHARED across distinct hosts:"
  cat "$TMP.grouped"
fi


[[ "$dup_count" -gt 0 ]] && exit 1
exit 0

# ─── Optional: JSON evidence for the audit log (RW-PROD-001 §6) ─────────────
if [[ -n "$JSON_OUT" ]] && command -v python3 >/dev/null 2>&1; then
  SCANNED="$total_count" python3 - "$TMP" "$JSON_OUT" <<'PY'
import json, os, sys
tsv_path, out_path = sys.argv[1], sys.argv[2]
scanned = int(os.environ.get("SCANNED", "0"))
groups = {}
try:
    with open(tsv_path) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 3:
                continue
            host, fp, serial = parts[0], parts[1], parts[2]
            groups.setdefault(fp, {"fingerprint_sha256": fp, "serial": serial, "hosts": []})
            groups[fp]["hosts"].append(host)
except OSError:
    pass
report = {
    "scanned": scanned,
    "duplicates": [g for g in groups.values() if len(g["hosts"]) > 1],
    "unique":     [g for g in groups.values() if len(g["hosts"]) == 1],
}
with open(out_path, "w") as out_f:
    json.dump(report, out_f, indent=2, sort_keys=True)
PY
elif [[ -n "$JSON_OUT" ]]; then
  warn "A7: --json requested but python3 not present; skipping evidence write"
fi

# ─── Detect duplicates (RW-PROD-001 §1 pain-point #6) ────────────────────────
# Strategy: sort the TSV by fingerprint column and group consecutive duplicates.
# Pure POSIX sort + awk; no fancy GNU-isms, no /dev/stderr fakery.
TMP_GROUPED="$(mktemp -t check-share-cert.grouped.XXXXXX)"
trap 'rm -f "$TMP" "$TMP_GROUPED"' EXIT
awk -F'\t' '{print $2 "\t" $1 "\t" $3}' "$TMP" | sort > "$TMP_GROUPED"

# Count how many fingerprint groups have >= 2 hosts.
dup_count="$(awk -F'\t' '
  { fp=$1; if (fp == prev) { n++; hosts = hosts " " $2 } else { if (n > 1) print n, hosts; n=1; hosts=$2; prev=fp } }
  END { if (n > 1) print n, hosts }
' "$TMP_GROUPED" | tee "$TMP_GROUPED.dup_lines" | wc -l | tr -d ' ')"

echo "[check-share-cert] scanned $total_count cert(s); duplicates=$dup_count"
if [[ "${dup_count:-0}" -gt 0 ]]; then
  echo "[check-share-cert] FAIL — the following fingerprints are SHARED across distinct hosts:"
  cat "$TMP_GROUPED.dup_lines" | awk '{printf "  fingerprint-group-of-%d: %s\n", $1, $2}'
fi
