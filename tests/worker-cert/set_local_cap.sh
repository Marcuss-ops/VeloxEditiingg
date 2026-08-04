#!/usr/bin/env bash
set -euo pipefail

cap="${1:-}"
[[ "$cap" =~ ^[1-9][0-9]*$ ]] || { echo "cap must be a positive integer" >&2; exit 2; }
config=/var/lib/velox-worker/worker_config.json
sed -E -i "s/(\"max_active_jobs\": )[0-9]+/\1${cap}/" "$config"
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$config"
systemctl restart velox-worker
