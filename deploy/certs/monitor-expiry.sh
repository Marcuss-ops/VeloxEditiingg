#!/usr/bin/env bash
# deploy/certs/monitor-expiry.sh
# =============================================================================
# Certificate expiry monitor for Velox production PKI.
#
# Scans /opt/velox/certs/ for PEM certificates, reads their expiry dates,
# and reports status via JSON on stdout. Exit code reflects the worst
# status found:
#
#   0   OK — all certs have > 14 days remaining
#   1   WARNING — at least one cert has ≤ 14 days
#   2   CRITICAL — at least one cert has ≤ 2 days
#   3   EXPIRED — at least one cert has expired
#
# Usage:
#   ./monitor-expiry.sh                  # Human-readable table
#   ./monitor-expiry.sh --json            # JSON for monitoring systems
#   ./monitor-expiry.sh --json --dir /custom/cert/path
#
# Environment:
#   OPENSSL  openssl binary path (default: openssl)
#   CERT_DIR root directory to scan (default: /opt/velox/certs)
#
# Integration:
#   Cron: 0 */6 * * * /opt/velox/scripts/monitor-expiry.sh --json
#   Alert: pipe JSON to alert-cert-expiry.sh
# =============================================================================

set -euo pipefail

OPENSSL="${OPENSSL:-openssl}"
CERT_DIR="${CERT_DIR:-/opt/velox/certs}"
JSON_MODE=0
[[ "${1:-}" == "--json" ]] && { JSON_MODE=1; shift; }
[[ "${1:-}" == "--dir" ]] && { CERT_DIR="$2"; shift 2; }

command -v "$OPENSSL" >/dev/null 2>&1 || {
  echo '{"error":"openssl not found"}' >&2; exit 3; }

# ─── Core: read expiry from a PEM file ──────────────────────────────────────
# Returns: serial,cn,fingerprint,end_epoch,end_rfc3339
inspect_cert() {
  local path="$1"
  local enddate serial cn fingerprint
  enddate="$("$OPENSSL" x509 -in "$path" -enddate -noout 2>/dev/null | cut -d= -f2- || true)"
  if [[ -z "$enddate" ]]; then return 1; fi
  serial="$("$OPENSSL" x509 -in "$path" -serial -noout 2>/dev/null | cut -d= -f2 || echo "?")"
  cn="$("$OPENSSL" x509 -in "$path" -subject -noout 2>/dev/null | sed -n 's/.*CN *= *//p' || echo "?")"
  fingerprint="$("$OPENSSL" x509 -in "$path" -fingerprint -sha256 -noout 2>/dev/null | cut -d= -f2 || echo "?")"
  local epoch
  epoch="$(date -d "$enddate" +%s 2>/dev/null || date -j -f "%b %d %H:%M:%S %Y %Z" "$enddate" +%s 2>/dev/null || echo 0)"
  local rfc3339
  rfc3339="$(date -d "$enddate" -Iseconds 2>/dev/null || date -j -f "%b %d %H:%M:%S %Y %Z" "$enddate" +%Y-%m-%dT%H:%M:%S%z 2>/dev/null || echo "$enddate")"
  echo "$serial|$cn|$fingerprint|$epoch|$rfc3339"
}

# ─── Status classifier ──────────────────────────────────────────────────────
classify() {
  local days_left="$1"
  if (( days_left <= 0 ));  then echo "expired";  return 3; fi
  if (( days_left <= 2 ));  then echo "critical"; return 2; fi
  if (( days_left <= 7 ));  then echo "warning";  return 1; fi
  if (( days_left <= 14 )); then echo "warning";  return 1; fi
  echo "ok"
  return 0
}

# ─── Main ───────────────────────────────────────────────────────────────────
# Wrapped in main() so `local` is valid (PR 6 fix). Original script used
# `local` at top-level, which Bash rejects with `local: can only be used
# in a function`. The wrap also fixes several `set -e` interactions:
#
#   (1) exit code of `classify` is preserved via `if … else … ec=$?`.
#       The previous `status="$(classify …)" || true` always recorded
#       $?==0 from `true`, never classify's real exit — masking warning/
#       critical/expired outcomes. (`$?` inside the `else` branch of an
#       `if` is the failing condition's exit code.)
#
#   (2) case fall-through `;&` is removed — a single cert cannot be in
#       multiple categories. We now assign exactly one counter per cert.
#
#   (3) `(( count++ ))` is replaced with `count=$((count + 1))`; the
#       former evaluates to 0 on first hit per counter, returns 1 under
#       set -e, and aborts the script.
#
#   (4) `(( ec > worst_exit )) && worst_exit=$ec` is moved into an `if`
#       for the same reason — worst_exit starts at 0 and ec==0 on the
#       first OK cert, so `(( 0 > 0 ))` would have aborted the script
#       before any cert was even scanned.
main() {
  local first path cn days_left status expires_at cert_path
  local info serial fingerprint end_epoch end_rfc3339 ec
  local worst_exit critical_count warning_count expired_count now_epoch
  declare -a CERTS=()
  worst_exit=0
  critical_count=0
  warning_count=0
  expired_count=0
  now_epoch="$(date +%s)"

  # Find all .crt files, skip the revoked dir.
  while IFS= read -r -d '' cert_path; do
    [[ "$cert_path" == *"/revoked/"* ]] && continue
    info="$(inspect_cert "$cert_path")" || continue
    IFS='|' read -r serial cn fingerprint end_epoch end_rfc3339 <<< "$info"
    days_left=$(( (end_epoch - now_epoch) / 86400 ))

    # Capture classify's exit code without triggering `set -e`: inside
    # an if-then-else, exit-on-error is suppressed for both arms, and
    # `$?` inside the else branch equals classify's actual exit code.
    if status="$(classify "$days_left")"; then
      ec=0
    else
      ec=$?
    fi

    CERTS+=("$(printf '{"path":"%s","cn":"%s","serial":"%s","fingerprint":"%s","expires_at":"%s","days_left":%d,"status":"%s"}' \
      "$cert_path" "$cn" "$serial" "$fingerprint" "$end_rfc3339" "$days_left" "$status")")

    # Single-category counter increment. No fall-through: classify()
    # already guarantees exactly one label per cert.
    case "$status" in
      expired)  expired_count=$((expired_count + 1))  ;;
      critical) critical_count=$((critical_count + 1)) ;;
      warning)  warning_count=$((warning_count + 1))  ;;
    esac

    if (( ec > worst_exit )); then
      worst_exit=$ec
    fi
  done < <(find "$CERT_DIR" -name '*.crt' -type f -print0 2>/dev/null || true)

  # ─── Output ─────────────────────────────────────────────────────────────────
  if (( JSON_MODE == 1 )); then
    printf '{"certs":['
    first=1
    for entry in "${CERTS[@]}"; do
      if (( first )); then first=0; else printf ','; fi
      printf '%s' "$entry"
    done
    printf '],"critical_count":%d,"warning_count":%d,"expired_count":%d,"total_count":%d}\n' \
      "$critical_count" "$warning_count" "$expired_count" "${#CERTS[@]}"
  else
    # Human-readable table (requires python3 for JSON parsing)
    if ! command -v python3 >/dev/null 2>&1; then
      echo '{"error":"python3 required for table mode — use --json instead"}' >&2
      exit 3
    fi
    printf '%-55s %-20s %-12s %-8s %s\n' "PATH" "CN" "DAYS LEFT" "STATUS" "EXPIRES"
    printf '%.0s-' {1..110}; echo
    for entry in "${CERTS[@]}"; do
      path="$(echo "$entry" | python3 -c "import sys,json;print(json.load(sys.stdin)['path'])" 2>/dev/null || echo "?")"
      cn="$(echo "$entry" | python3 -c "import sys,json;print(json.load(sys.stdin)['cn'])" 2>/dev/null || echo "?")"
      days_left="$(echo "$entry" | python3 -c "import sys,json;print(json.load(sys.stdin)['days_left'])" 2>/dev/null || echo "?")"
      status="$(echo "$entry" | python3 -c "import sys,json;print(json.load(sys.stdin)['status'])" 2>/dev/null || echo "?")"
      expires_at="$(echo "$entry" | python3 -c "import sys,json;print(json.load(sys.stdin)['expires_at'])" 2>/dev/null || echo "?")"
      printf '%-55s %-20s %-12s %-8s %s\n' "${path:0:55}" "${cn:0:20}" "$days_left" "$status" "$expires_at"
    done
    echo ""
    echo "Summary: ${#CERTS[@]} certs — $expired_count expired, $critical_count critical, $warning_count warning"
  fi

  exit "$worst_exit"
}

main "$@"
