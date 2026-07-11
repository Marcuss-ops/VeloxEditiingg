#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/check-share-cert.sh — RW-PROD-001 §3 A7 anti-condivisione cert
# ─────────────────────────────────────────────────────────────────────────────
# Catches duplicate worker.crt files in the Ansible inventory or in a flat
# worker-certs directory. Two physical hosts sharing the SAME client cert
# is an identity-collision failure mode: both can claim to be each other's
# worker_id and the master cannot attribute work correctly.
#
# Modes:
#   dir <DIR>            scan <DIR>/**/*.crt for duplicate SHA-256 fingerprints
#   inventory <FILE>     FILE is "host:abs-path-to-worker.crt" lines,
#                        blanks and #-comments ignored.
#   self-test            runs a small suite with temp certs; exit 0 = tool OK
#
# Exit codes:
#   0   OK — every worker cert is unique by fingerprint
#   1   DUPLICATE — at least two hosts share the same cert
#   2   USAGE/INPUT — invalid args / missing path / unreadable input
#   3   TOOLING — openssl missing
#
# Output:
#   * stdout: one-line summary + per-duplicate details when found
#   * --json <PATH>: structured evidence file with fingerprint→hosts mapping
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

OPENSSL="${OPENSSL:-openssl}"

# ─── Self-test function (defined before main logic) ────────────────────────
self_test_impl() {
  set -euo pipefail
  local TMPDIR SCRIPT
  TMPDIR="$(mktemp -d -t check-share-cert-selftest.XXXXXX)"
  trap 'rm -rf "$TMPDIR"' EXIT
  SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

  log_selftest() { printf '[check-share-cert:self-test] %s\n' "$*" >&2; }

  # Create a minimal CA.
  local ca_key="$TMPDIR/ca.key" ca_crt="$TMPDIR/ca.crt"
  "$OPENSSL" genrsa -out "$ca_key" 2048 2>/dev/null
  "$OPENSSL" req -x509 -new -nodes -key "$ca_key" -sha256 -days 1 \
    -subj "/CN=test-ca" -out "$ca_crt" 2>/dev/null

  make_cert() {
    local name="$1" cn="$2"
    local key="$TMPDIR/${name}.key" csr="$TMPDIR/${name}.csr" crt="$TMPDIR/${name}.crt"
    "$OPENSSL" genrsa -out "$key" 2048 2>/dev/null
    "$OPENSSL" req -new -key "$key" -subj "/CN=$cn" -out "$csr" 2>/dev/null
    "$OPENSSL" x509 -req -in "$csr" -CA "$ca_crt" -CAkey "$ca_key" \
      -CAcreateserial -sha256 -days 7 -out "$crt" 2>/dev/null
    echo "$crt"
  }

  # Test 1: all unique certs → exit 0
  log_selftest "Test 1/3: unique certs → exit 0"
  local d1="$TMPDIR/unique"
  mkdir -p "$d1"
  local i
  for i in worker-a worker-b worker-c; do
    make_cert "$i" "$i" > /dev/null
    cp "$TMPDIR/${i}.crt" "$d1/${i}.crt"
  done
  if "$SCRIPT" dir "$d1" >/dev/null 2>&1; then
    log_selftest "  PASS: unique certs → exit 0"
  else
    log_selftest "  FAIL: unique certs should exit 0"
    exit 1
  fi

  # Test 2: duplicate certs → exit 1
  log_selftest "Test 2/3: duplicate certs → exit 1"
  local d2="$TMPDIR/dup"
  mkdir -p "$d2"
  local shared_crt
  shared_crt="$(make_cert "shared" "shared-host")"
  cp "$shared_crt" "$d2/host-a.crt"
  cp "$shared_crt" "$d2/host-b.crt"
  if ! "$SCRIPT" dir "$d2" >/dev/null 2>&1; then
    log_selftest "  PASS: duplicate certs → exit 1"
  else
    log_selftest "  FAIL: duplicate certs should exit 1"
    exit 1
  fi

  # Test 3: --json output
  log_selftest "Test 3/3: --json evidence file"
  local json_out="$TMPDIR/evidence.json"
  "$SCRIPT" dir "$d1" --json "$json_out" >/dev/null 2>&1
  if [[ -f "$json_out" ]]; then
    local scanned
    scanned="$(python3 -c 'import json,sys; d=json.load(open(sys.argv[1])); print(d.get("scanned",0))' "$json_out" 2>/dev/null || echo 0)"
    if [[ "$scanned" -ge 3 ]]; then
      log_selftest "  PASS: JSON evidence with scanned=$scanned"
    else
      log_selftest "  FAIL: JSON evidence scanned=$scanned (expected >=3)"
      exit 1
    fi
  else
    log_selftest "  FAIL: JSON evidence file not created"
    exit 1
  fi

  log_selftest "ALL SELF-TESTS PASSED"
  exit 0
}

# ─── Usage ──────────────────────────────────────────────────────────────────
usage() {
  cat <<USAGE
Usage: $0 [dir|inventory|self-test] <TARGET> [--json <evidences.json>]

Modes:
  dir <DIR>             Scan every *.crt under <DIR> (recursive).
  inventory <FILE>      FILE is "<host>:<abs-path-to-worker.crt>" lines, blanks and
                        lines starting with '#' ignored.
  self-test             Run CI self-test (no target needed).

Examples:
  $0 dir /opt/velox/certs/workers
  $0 inventory deploy/inventory/hosts.ini --json ops/rw-prod-001-cert-sharing.json
  $0 self-test

Exit codes: 0 OK | 1 DUPLICATE | 2 USAGE/INPUT | 3 TOOLING
USAGE
  exit 2
}

[[ $# -ge 1 ]] || usage
MODE="$1"; shift

# ── Self-test dispatch ──────────────────────────────────────────────────────
if [[ "$MODE" == "self-test" ]]; then
  self_test_impl
  # unreachable; self_test_impl exits
fi

# ── Normal modes ────────────────────────────────────────────────────────────
[[ $# -ge 1 ]] || usage
TARGET="$1"; shift
JSON_OUT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --json) JSON_OUT="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "[check-share-cert] unknown flag: $1" >&2; usage ;;
  esac
done

command -v "$OPENSSL" >/dev/null 2>&1 || {
  echo "[check-share-cert] FATAL: openssl not found at '$OPENSSL'" >&2; exit 3; }

# ─── collect (host, fp, serial) triples ─────────────────────────────────────
collect_dir() {
  local dir="$1"
  [[ -d "$dir" ]] || { echo "[check-share-cert] FATAL: dir $dir not found" >&2; exit 2; }
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
    line="${line%%#*}"
    line="${line%%$'\r'}"
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
trap 'rm -f "$TMP"' EXIT

case "$MODE" in
  dir)       collect_dir "$TARGET"      > "$TMP" ;;
  inventory) collect_inventory "$TARGET" > "$TMP" ;;
  *) usage ;;
esac

total_count="$(wc -l < "$TMP" | tr -d ' ')"
[[ "$total_count" -eq 0 ]] && {
  echo "[check-share-cert] scanned 0 cert(s); SKIP (nothing to check)" >&2
  exit 0
}

# ─── Group by fingerprint and detect duplicates ─────────────────────────────
TMP_GROUPED="$(mktemp -t check-share-cert.grouped.XXXXXX)"
trap 'rm -f "$TMP" "$TMP_GROUPED"' EXIT

awk -F'\t' '{print $2 "\t" $1 "\t" $3}' "$TMP" | sort > "$TMP_GROUPED"

DUP_LINES="$(mktemp -t check-share-cert.dups.XXXXXX)"
trap 'rm -f "$TMP" "$TMP_GROUPED" "$DUP_LINES"' EXIT

awk -F'\t' '
  BEGIN { n=0; hosts=""; prev="" }
  {
    fp=$1; host=$2
    if (fp == prev) {
      n++; hosts = hosts " " host
    } else {
      if (n > 1) printf "DUPLICATE\t%d\t%s\t%s\n", n, prev, hosts
      n=1; hosts=host; prev=fp
    }
  }
  END {
    if (n > 1) printf "DUPLICATE\t%d\t%s\t%s\n", n, prev, hosts
  }
' "$TMP_GROUPED" > "$DUP_LINES"

dup_count="$(wc -l < "$DUP_LINES" | tr -d ' ')"
echo "[check-share-cert] scanned $total_count cert(s); duplicates=$dup_count"

if [[ "$dup_count" -gt 0 ]]; then
  echo "[check-share-cert] FAIL — the following fingerprints are SHARED across distinct hosts:"
  while IFS=$'\t' read -r _ n fp hosts; do
    echo "  fingerprint-group-of-${n}: $fp  hosts:${hosts}"
  done < "$DUP_LINES"
fi

# ─── JSON evidence (RW-PROD-001 §6) ────────────────────────────────────────
if [[ -n "$JSON_OUT" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$TMP" "$JSON_OUT" "$total_count" "$dup_count" <<'PY'
import json, sys
tsv_path, out_path = sys.argv[1], sys.argv[2]
scanned = int(sys.argv[3])
total_dups = int(sys.argv[4])

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
    "duplicates_count": total_dups,
    "duplicates": [g for g in groups.values() if len(g["hosts"]) > 1],
    "unique": [g for g in groups.values() if len(g["hosts"]) == 1],
}
with open(out_path, "w") as out_f:
    json.dump(report, out_f, indent=2, sort_keys=True)
PY
    echo "[check-share-cert] JSON evidence written to $JSON_OUT"
  else
    echo "[check-share-cert] WARN: --json requested but python3 not present; skipping evidence write" >&2
  fi
fi

if [[ "$dup_count" -gt 0 ]]; then
  exit 1
fi
exit 0
