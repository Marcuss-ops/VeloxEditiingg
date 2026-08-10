#!/usr/bin/env bash
# shellcheck disable=SC2034
# Shared globals are supplied by the certify-remote-fleet.sh entrypoint.
# Report generation and destructive-artifact redaction for fleet certification.
# Sourced by the Bash entrypoint; relies on shared globals only.

write_fleet_junit() {
  local report="$1" output="$2" mode="$3"
  python3 - "$report" "$output" "$mode" <<'PY'
import json
import sys
from xml.sax.saxutils import escape, quoteattr

report_path, output_path, mode = sys.argv[1:]
try:
    report = json.load(open(report_path, encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    report = {"overall": "FAIL", "workers": []}
workers = report.get("workers") or []
failures = sum(1 for worker in workers if worker.get("status") != "PASS")
with open(output_path, "w", encoding="utf-8") as out:
    out.write('<?xml version="1.0" encoding="UTF-8"?>\n')
    out.write('<testsuite name=%s tests="%d" failures="%d">\n' %
              (quoteattr("velox.remote_worker.fleet." + mode), max(1, len(workers)), failures))
    if workers:
        for worker in workers:
            worker_id = str(worker.get("worker_id", "unknown"))
            status = str(worker.get("status", "FAIL"))
            diagnostic = str(worker.get("diagnostic", ""))
            out.write('  <testcase name=%s>' % quoteattr(worker_id))
            if status != "PASS":
                out.write('<failure message=%s>%s</failure>' %
                          (quoteattr(diagnostic[:500]), escape(diagnostic)))
            out.write('</testcase>\n')
    else:
        out.write('  <testcase name="fleet"><failure message="no workers"/></testcase>\n')
    out.write('</testsuite>\n')
PY
}

redact_destructive_artifacts() {
  local step_dir="$1" command="$2"
  [[ -n "$command" && -d "$step_dir" ]] || return 0
  python3 - "$step_dir" "$command" <<'PY'
from pathlib import Path
import json
import sys

root = Path(sys.argv[1])
secret = sys.argv[2]
for name in ("stdout.log", "stderr.log"):
    path = root / name
    if path.is_file():
        path.write_text(path.read_text(errors="replace").replace(secret, "[REDACTED_DESTRUCTIVE_COMMAND]"))
report = root / "report.json"
if report.is_file():
    try:
        data = json.loads(report.read_text())
        if isinstance(data, dict) and "stop_cmd" in data:
            data["stop_cmd"] = "[REDACTED_DESTRUCTIVE_COMMAND]"
            report.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    except (OSError, json.JSONDecodeError):
        pass
PY
}
