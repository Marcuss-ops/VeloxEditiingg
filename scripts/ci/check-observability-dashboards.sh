#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
DASHBOARDS="$ROOT/dashboards"
SQL="$ROOT/prometheus/observability-dashboards.sql"

python3 - "$DASHBOARDS" "$SQL" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
sql_path = pathlib.Path(sys.argv[2])
required = {
    "attempt-explorer.json",
    "worker-ranking.json",
    "version-regression.json",
    "recoverable-time.json",
    "cold-warm-cache.json",
    "parallelism-efficiency.json",
    "quality-vs-speed.json",
    "waste-analysis.json",
}
for name in required:
    path = root / name
    if not path.is_file():
        raise SystemExit(f"missing dashboard: {name}")
    with path.open(encoding="utf-8") as fh:
        dashboard = json.load(fh)
    if not dashboard.get("title") or not dashboard.get("panels"):
        raise SystemExit(f"dashboard lacks title/panels: {name}")

unsafe = re.compile(r"(?:^|[,{}])\s*(job_id|task_id|attempt_id|artifact_id|sha256|video_title|error_message)\s*(?:=|!~|=~|!=)")
allowed = {
    "executor_id", "executor_version", "worker_class", "worker_id", "phase",
    "status", "source_type", "result", "outcome", "waste_type", "reason",
    "exit_code", "case", "action", "le", "quantile",
}
label_pattern = re.compile(r"[,{]\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|!)")
aggregation_pattern = re.compile(r"\\b(?:by|without)\\s*\\(([^)]*)\\)")
def expressions(value):
    if isinstance(value, dict):
        if isinstance(value.get("expr"), str):
            yield value["expr"]
        for child in value.values():
            yield from expressions(child)
    elif isinstance(value, list):
        for child in value:
            yield from expressions(child)
for path in sorted(root.glob("*.json")):
    with path.open(encoding="utf-8") as fh:
        dashboard = json.load(fh)
    for expr in expressions(dashboard):
        if unsafe.search(expr):
            raise SystemExit(f"unsafe high-cardinality Prometheus label in {path.name}: {expr}")
        for label in label_pattern.findall(expr):
            if label not in allowed:
                raise SystemExit(f"unknown/unapproved Prometheus label {label!r} in {path.name}")
        for clause in aggregation_pattern.findall(expr):
            for label in (part.strip() for part in clause.split(',')):
                if label and label not in allowed:
                    raise SystemExit(f"unknown/unapproved Prometheus aggregation label {label!r} in {path.name}")

sql = sql_path.read_text(encoding="utf-8")
for heading in (
    "1. ATTEMPT EXPLORER", "2. WORKER RANKING", "3. VERSION REGRESSION",
    "4. RECOVERABLE TIME", "5. COLD VS WARM CACHE", "6. PARALLELISM EFFICIENCY",
    "7. QUALITY VS SPEED", "8. WASTE",
):
    if heading not in sql:
        raise SystemExit(f"missing SQL dashboard section: {heading}")
print(f"validated {len(required)} observability dashboards and 8 SQL sections")
PY
