#!/usr/bin/env bash
# diff-job-payloads.sh — semantic diff of two Velox jobs' render inputs.
#
# Compares the compiled TaskSpec payloads (scenes clip/stock per scene_id,
# durations, overlays, runtime assets/audio) plus the placement
# diagnostics and final artifacts of JOB_A vs JOB_B. Bytes-identical
# outputs with different declarations (e.g. a stock->clip fix under a
# final narration mix) are explained, not just reported.
#
# Usage:
#   scripts/ops/diff-job-payloads.sh job_AAA job_BBB
#
# Env:
#   VELOX_DB_PATH   master SQLite path (default:
#                   /opt/velox/current/.velox/data/velox.db). The DB file is
#                   opened read-only; run with sudo when it is root-owned.
#   SKIP_DIAG=1     skip the docker-logs diagnostic lookup.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <job-a> <job-b>" >&2
  exit 2
fi
JOB_A="$1"
JOB_B="$2"
DB_PATH="${VELOX_DB_PATH:-/opt/velox/current/.velox/data/velox.db}"

export DIFF_JOB_A="$JOB_A" DIFF_JOB_B="$JOB_B" DIFF_DB_PATH="$DB_PATH"

python3 - <<'PY'
import json, os, sqlite3, sys
from collections import Counter

db_path = os.environ["DIFF_DB_PATH"]
jobs = [os.environ["DIFF_JOB_A"], os.environ["DIFF_JOB_B"]]
try:
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
except Exception as e:
    print(f"cannot open DB readonly at {db_path}: {e}", file=sys.stderr)
    print("hint: run with sudo, or set VELOX_DB_PATH", file=sys.stderr)
    sys.exit(1)

def load_spec(job_id):
    row = con.execute(
        "SELECT task_id FROM tasks WHERE job_id=? ORDER BY created_at DESC LIMIT 1",
        (job_id,)).fetchone()
    if not row:
        return None, f"no task for {job_id}"
    spec = con.execute(
        "SELECT payload_json FROM task_specs WHERE task_id=?", (row[0],)).fetchone()
    if not spec:
        return None, f"no task_spec for task {row[0]}"
    return json.loads(spec[0]), None

def scene_shape(s):
    has_clip = isinstance(s.get("clip"), dict)
    stock = s.get("stock")
    n_stock = len(stock) if isinstance(stock, list) else (1 if isinstance(stock, dict) else 0)
    return (s.get("kind"), has_clip, n_stock)

specs = {}
for job in jobs:
    spec, err = load_spec(job)
    if err:
        print(f"{job}: {err}", file=sys.stderr)
        sys.exit(1)
    specs[job] = spec

for job in jobs:
    d = specs[job]
    scenes = json.loads(d["scenes_json"]) if isinstance(d.get("scenes_json"), str) else d.get("scenes", [])
    shapes = Counter(scene_shape(s) for s in scenes)
    print(f"== {job}")
    print(f"   scenes={len(scenes)} duration_s={sum(s.get('duration_seconds', 0) for s in scenes):.2f} "
          f"overlays={len(d.get('overlays') or [])} "
          f"runtime_assets={len(d.get('runtime_assets') or [])} "
          f"runtime_audio={bool(d.get('runtime_audio'))} copy_only={d.get('copy_only')}")
    print(f"   scene shapes (kind, has_clip, n_stock): {dict(shapes)}")

# Per-scene declaration diff by scene_id.
a = {s.get("scene_id"): s for s in (json.loads(specs[jobs[0]]["scenes_json"]))}
b = {s.get("scene_id"): s for s in (json.loads(specs[jobs[1]]["scenes_json"]))}
only_a = sorted(set(a) - set(b))
only_b = sorted(set(b) - set(a))
changed = sorted(sid for sid in set(a) & set(b) if scene_shape(a[sid]) != scene_shape(b[sid]))
print(f"\nscenes only in {jobs[0]}: {len(only_a)} {only_a[:10]}")
print(f"scenes only in {jobs[1]}: {len(only_b)} {only_b[:10]}")
print(f"scenes with different clip/stock declaration: {len(changed)}")
for sid in changed[:20]:
    print(f"   {sid}: {jobs[0]}={scene_shape(a[sid])} {jobs[1]}={scene_shape(b[sid])}")

# Companion diffs.
for key in ("overlays", "runtime_assets"):
    la = len(specs[jobs[0]].get(key) or [])
    lb = len(specs[jobs[1]].get(key) or [])
    flag = "" if la == lb else "  <-- DIFFERS"
    print(f"{key}: {jobs[0]}={la} {jobs[1]}={lb}{flag}")
ra = specs[jobs[0]].get("runtime_audio") or {}
rb = specs[jobs[1]].get("runtime_audio") or {}
print(f"runtime_audio: {jobs[0]}={json.dumps(ra)[:160]}")
print(f"               {jobs[1]}={json.dumps(rb)[:160]}")

# Final artifacts (direct job link on artifacts.job_id).
print("\nfinal artifacts:")
for job in jobs:
    status = con.execute(
        "SELECT status FROM jobs WHERE job_id=?", (job,)).fetchone()
    rows = con.execute(
        "SELECT type, status, sha256, size_bytes FROM artifacts "
        "WHERE job_id=? AND type IN ('final_video','engine_progress_sidecar') "
        "ORDER BY type", (job,)).fetchall()
    print(f"   {job}: job={status[0] if status else '?'}")
    for r in rows:
        print(f"      artifact={r[0]}/{r[1]} sha={str(r[2])[:16]} size={r[3]}")
PY

if [[ "${SKIP_DIAG:-0}" != "1" ]]; then
  echo
  echo "placement diagnostics:"
  for job in "$JOB_A" "$JOB_B"; do
    docker logs velox-server 2>&1 | grep -a "RENDER_INPUT_DIAGNOSTIC" | grep -a "$job" | tail -1 || echo "  $job: no diagnostic line (docker logs unavailable?)"
  done
fi
