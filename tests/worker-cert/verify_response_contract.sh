#!/usr/bin/env bash
set -euo pipefail

job_id="${1:-}"
response_json="${5:-}"
[[ -n "$job_id" && -r "$response_json" ]] || exit 2
jq -e --arg job_id "$job_id" '.status == "SUCCEEDED" and .job_id == $job_id and (.task_id | length) > 0 and (.attempt_id | length) > 0 and (.worker_id | length) > 0' "$response_json" >/dev/null
