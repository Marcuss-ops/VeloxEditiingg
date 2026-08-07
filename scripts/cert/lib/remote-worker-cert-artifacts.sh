# remote-worker-cert-artifacts.sh — artifact evidence helpers.
# Loaded by scripts/cert/remote-worker-cert-config.sh.

rw_die() {
  printf 'remote-worker-cert: %s\n' "$*" >&2
  return 1
}

# Evidence output is initialized only for a CLI certification run. Sourcing this
# file remains side-effect free until rw_init_artifacts is called.
rw_init_artifacts() {
  RW_RUN_ID="${RW_RUN_ID:-${VELOX_CERT_RUN_ID:-cert-$(date -u +%Y%m%dT%H%M%SZ)-$$}}"
  RW_ARTIFACT_DIR="${RW_ARTIFACT_DIR:-${VELOX_CERT_ARTIFACT_DIR:-${TMPDIR:-/tmp}/velox-cert-${RW_RUN_ID}}}"
  mkdir -p -- "$RW_ARTIFACT_DIR" || return 1
  : >"${RW_ARTIFACT_DIR}/commands.log" || return 1
  printf '%s\n' "run_id=${RW_RUN_ID} mode=${RW_CERT_MODE:-unknown}" >>"${RW_ARTIFACT_DIR}/commands.log"
  jq -n --arg run_id "$RW_RUN_ID" --arg status NOT_RUN \
    '{run_id:$run_id,status:$status,operations:[]}' >"${RW_ARTIFACT_DIR}/operations.json"
  jq -n --arg run_id "$RW_RUN_ID" --arg status NOT_RUN \
    '{run_id:$run_id,status:$status}' >"${RW_ARTIFACT_DIR}/artifact-ffprobe.json"
  for snapshot in worker-before worker-after master-before master-after; do
    jq -n --arg run_id "$RW_RUN_ID" --arg status NOT_OBSERVED \
      '{run_id:$run_id,status:$status}' >"${RW_ARTIFACT_DIR}/${snapshot}.json"
  done
  export RW_RUN_ID RW_ARTIFACT_DIR
}

rw_log_command() {
  [[ -n "${RW_ARTIFACT_DIR:-}" ]] || return 0
  # Callers pass only method/path or an already-sanitized remote command.
  # Credentials and request bodies are intentionally never logged.
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >>"${RW_ARTIFACT_DIR}/commands.log"
}

rw_snapshot_json() {
  local kind="$1" body="$2" normalized
  [[ -n "${RW_ARTIFACT_DIR:-}" ]] || return 0
  if jq -e . >/dev/null 2>&1 <<<"$body"; then
    normalized="$body"
  else
    normalized="$(jq -cn --arg raw "$body" '{raw:$raw}')"
  fi
  printf '%s\n' "$normalized" >"${RW_ARTIFACT_DIR}/${kind}-after.json"
  if [[ ! -s "${RW_ARTIFACT_DIR}/${kind}-before.json" ]] || jq -e '.status == "NOT_OBSERVED"' "${RW_ARTIFACT_DIR}/${kind}-before.json" >/dev/null 2>&1; then
    printf '%s\n' "$normalized" >"${RW_ARTIFACT_DIR}/${kind}-before.json"
  fi
  printf '%s\n' "$normalized" >"${RW_ARTIFACT_DIR}/${kind}.json"
}

rw_record_operation() {
  local method="$1" path="$2" http_status="$3" body="${4:-}" operation_json
  [[ -n "${RW_ARTIFACT_DIR:-}" ]] || return 0
  operation_json="$(jq -cn --arg run_id "${RW_RUN_ID:-}" --arg method "$method" --arg path "$path" \
    --arg http_status "$http_status" --arg body "$body" \
    '{run_id:$run_id,method:$method,path:$path,http_status:($http_status|tonumber? // $http_status),response:(try ($body|fromjson) catch {raw:$body})}')"
  jq --argjson operation "$operation_json" '.operations += [$operation] | .status="RECORDED"' \
    "${RW_ARTIFACT_DIR}/operations.json" >"${RW_ARTIFACT_DIR}/operations.json.tmp" \
    && mv -f -- "${RW_ARTIFACT_DIR}/operations.json.tmp" "${RW_ARTIFACT_DIR}/operations.json"
}

rw_record_artifact_ffprobe() {
  local status="$1" artifact_file="${2:-}" sha256="${3:-}" verifier_report="${4:-}" diagnostic="${5:-}"
  [[ -n "${RW_ARTIFACT_DIR:-}" ]] || return 0
  if [[ -n "$verifier_report" && -r "$verifier_report" ]] && jq -e . "$verifier_report" >/dev/null 2>&1; then
    jq --arg run_id "${RW_RUN_ID:-}" --arg status "$status" --arg file "$artifact_file" \
      --arg sha256 "$sha256" --arg diagnostic "$diagnostic" \
      '. + {run_id:$run_id,status:$status,artifact_file:$file,sha256:$sha256,diagnostic:$diagnostic}' \
      "$verifier_report" >"${RW_ARTIFACT_DIR}/artifact-ffprobe.json"
  else
    jq -n --arg run_id "${RW_RUN_ID:-}" --arg status "$status" --arg file "$artifact_file" \
      --arg sha256 "$sha256" --arg diagnostic "$diagnostic" \
      '{run_id:$run_id,status:$status,artifact_file:$file,sha256:$sha256,diagnostic:$diagnostic}' \
      >"${RW_ARTIFACT_DIR}/artifact-ffprobe.json"
  fi
}

rw_junit_escape() {
  sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g; s/'"'"'/\&apos;/g'
}

rw_write_junit() {
  local report_file="$1" junit_file="$2" mode="$3" report_json
  report_json="$(cat "$report_file" 2>/dev/null || printf '%s' '{}')"
  python3 - "$junit_file" "$mode" "$report_json" <<'PY'
import json, sys
from xml.sax.saxutils import escape, quoteattr
out, mode, raw = sys.argv[1:]
try:
    report = json.loads(raw)
except json.JSONDecodeError:
    report = {"overall": "FAIL", "checks": [{"id": "REPORT", "status": "FAIL", "diagnostic": "invalid report JSON"}]}
checks = report.get("checks") or []
failures = sum(1 for c in checks if c.get("status") == "FAIL")
if report.get("overall") == "FAIL" and not failures:
    failures = 1
with open(out, "w", encoding="utf-8") as fh:
    fh.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    fh.write('<testsuite name="velox.remote_worker.%s" tests="%d" failures="%d">\n' % (escape(mode), max(1, len(checks)), failures))
    if checks:
        for check in checks:
            name = str(check.get("id") or check.get("name") or "check")
            status = check.get("status", "FAIL")
            diagnostic = str(check.get("diagnostic") or "")
            fh.write('  <testcase name=%s>' % quoteattr(name))
            if status == "FAIL":
                fh.write('<failure message=%s>%s</failure>' % (quoteattr(diagnostic[:500]), escape(diagnostic)))
            fh.write('</testcase>\n')
    else:
        if report.get("overall") == "PASS":
            fh.write('  <testcase name="certification"/>\n')
        else:
            fh.write('  <testcase name="certification"><failure message="no checks"/></testcase>\n')
    fh.write('</testsuite>\n')
PY
}

rw_finalize_artifacts() {
  local raw_report="$1" rc="$2" mode="$3" report_file="${RW_ARTIFACT_DIR}/report.json"
  local overall
  if ! jq -e . "$raw_report" >/dev/null 2>&1; then
    jq -n --arg run_id "${RW_RUN_ID:-}" --arg mode "$mode" --arg status FAIL --arg diagnostic "runner emitted invalid JSON" \
      '{run_id:$run_id,mode:$mode,overall:$status,checks:[{id:"REPORT",name:"report",status:$status,diagnostic:$diagnostic}],result:null}' >"$report_file"
  else
    overall="$(jq -r --arg fallback "$([[ "$rc" -eq 0 ]] && printf PASS || printf FAIL)" '.overall // $fallback' "$raw_report")"
    jq --arg run_id "${RW_RUN_ID:-}" --arg mode "$mode" --argjson exit_code "$rc" \
      --arg artifact_dir "${RW_ARTIFACT_DIR:-}" --arg overall "$overall" \
      '{run_id:$run_id,mode:$mode,overall:$overall,exit_code:$exit_code,artifact_dir:$artifact_dir,checks:(.checks // []),result:.}' \
      "$raw_report" >"${report_file}.tmp" && mv -f -- "${report_file}.tmp" "$report_file"
  fi
  rw_write_junit "$report_file" "${RW_ARTIFACT_DIR}/report.junit.xml" "$mode"
  jq --arg status "$(jq -r '.overall' "$report_file")" '.status=$status' "${RW_ARTIFACT_DIR}/operations.json" >"${RW_ARTIFACT_DIR}/operations.json.tmp" \
    && mv -f -- "${RW_ARTIFACT_DIR}/operations.json.tmp" "${RW_ARTIFACT_DIR}/operations.json"
}

