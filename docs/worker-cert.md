# Worker Certification Matrix

> **Purpose.** Single source of truth for which workers in the fleet are
> cert-ready. Each row in the matrix below is updated incrementally after
> a smoke run by reading `tests/worker-cert/workers/<worker_id>/smoke.json`
> + `pass_criteria.json` and bumping the 3 columns. The PASS criterion
> flow at the bottom of this doc describes what "cert-ready" means in
> observable terms; the matrix tracks who-meets-it.

## Fleet matrix (4 targets)

The 4 worker-cert targets per the user-spec checklist. Each cell carries
a binary verdict that reflects the most recent smoke run; check the
**Evidence** column for the `job_id` / `task_id` / `attempt_id` lineage.

| Worker | Connessione verificata | Executor verificato | Render completo verificato | Evidence (job / status) |
| --- | :-: | :-: | :-: | --- |
| `host_57_129_132_133` | ✅ | ✅ | ✅ | `job_6138f86480ce8762` → SUCCEEDED |
| `host_57_131_20_173` | ✅ | ✅ | Da testare | placeholder — run `pass_criteria.sh` |
| `velox-worker-13197` | ✅ | ✅ | Da testare | placeholder — run `pass_criteria.sh` |
| `velox-worker-523925eb` | ✅ | ✅ | Da testare | placeholder — run `pass_criteria.sh` |

**Column semantics.**

| Column | Asserts | Source |
| --- | --- | --- |
| Connessione verificata | `GET /api/v1/workers/<worker_id>.status == "CONNECTED"` AND `.session_active == true` | `pass_criteria.sh` steps 1+2 |
| Executor verificato | `executors[]` includes `id=scene.composite.v1` `version=1` AND a real-asset smoke completes (`render_time_ms > 0`) | `pass_criteria.sh` steps 3+8 |
| Render completo verificato | `pass_criteria.sh` reports a SUCCEEDED job with the artifact verified by `verify_artifact.sh` (`ffprobe` + SHA-256 round-trip + master side `status=SUCCEEDED`) | `pass_criteria.sh` steps 9+10 |

## PASS criterion flow (canonical 10 steps)

The "cert-ready" verdict for a worker is the conjunction of all 10 steps
being satisfied in sequence. Each step has an observable evidence
source — green CI is mechanical, not heuristic.

| # | Step | Asserts on | Evidence (canonical) |
| --- | --- | --- | --- |
| 1 | CONNECTED | `GET /api/v1/workers/<worker_id>.status` | `status=CONNECTED` |
| 2 | session_active | `.session_active == true` | worker_sessions ACTIVE row visible |
| 3 | `scene.composite.v1@1` advertised | `.executors[]` includes the executor | hello/heartbeat capabilities match `pkg/api.CapabilitySchemaVersion = 1` |
| 4 | TaskLeaseGranted | master log + `smoke_scrape_lease` | `[GRPC] TaskLeaseGranted sent to worker <ID> ... task=<TID> attempt=<AID> lease=<LID>` |
| 5 | TaskAccepted | master log | `[GRPC] TaskAccepted task=<TID>` |
| 6 | asset scaricati | master log + `task_attempt_metrics.download_ms` | download_ms > 0 OR asset_paths populated |
| 7 | RUNNING | master log OR `worker_task_runtime.runtime_status` | `runtime_status=RUNNING` |
| 8 | rendering | master log | `engine_render_started` OR `StageExecutor` marker |
| 9 | artifact verificato | `verify_artifact.sh` rc=0 on downloaded artifact | h.264 video + AAC audio + duration/w/fps thresholds + master side `status=SUCCEEDED` |
| 10 | SUCCEEDED | `GET /api/v1/jobs/<job_id>.status` | `status=SUCCEEDED` AND `artifact_url` populated |

**Binary verdict:** `tests/worker-cert/pass_criteria.sh <worker_id>` returns
`exit 0` (PASS) on full green, `exit 10+<step>` on the first failing step
(steps are recorded in `workers/<worker_id>/pass_criteria.json` so an
operator reading a post-mortem can identify which step regressed without
re-running).

## Update procedure (after each smoke)

The matrix is meant to be updated incrementally. After running a smoke,
the operator runs one of the `jq` snippets below and bumps the
corresponding 3 cells. The snippets read from the canonical per-worker
JSON output of the worker-cert harness — no manual copy-paste needed.

### Cert-ready verdict (binary)

```bash
# For a single worker:
jq -er '.verdict' tests/worker-cert/workers/<worker_id>/pass_criteria.json
# → "PASS" (all 10 steps) | "FAIL" (first failing step recorded)

# For all 4 workers:
for w in host_57_129_132_133 host_57_131_20_173 velox-worker-13197 velox-worker-523925eb; do
  printf '%-30s %s\n' "$w" \
    "$(jq -er '.verdict // "NOT_RUN"' tests/worker-cert/workers/$w/pass_criteria.json 2>/dev/null)"
done
```

### Per-worker first-failing step (when verdict != PASS)

```bash
jq -er '"first_failing_step=\(.first_failing_step) name=\(.steps[.first_failing_step - 1].name)"' \
  tests/worker-cert/workers/<worker_id>/pass_criteria.json
```

### Render timing + size (for the "Render completo verificato" cell)

```bash
jq -er '"artifact_size_bytes=\(.artifact_size_bytes) render_ms_engine=\(.render_ms_engine)"' \
  tests/worker-cert/workers/<worker_id>/pass_criteria.json
```

### CI integration (recommended for `tests/e2e/grpc-control-plane/run.sh`)

Wire `pass_criteria.sh` as a post-flight assertion per case row. After
each case-N matrix row completes, run `pass_criteria.sh <worker_id>` and
assert exit 0; surface the failing step in the JUnit XML so the CI
matrix reports a precise root cause.

## Cross-references

- `tests/worker-cert/smoke_one.sh` — per-worker SUCCEEDED smoke (writes `smoke.json`)
- `tests/worker-cert/pass_criteria.sh` — 10-step PASS criterion flow (writes `pass_criteria.json`)
- `tests/worker-cert/verify_artifact.sh` — offline `ffprobe` + master-side status check
- `tests/worker-cert/build_real_payload.py` — canonical real-asset payload (no `velox-asset://voiceovers/<file>.mp3`)
- `tests/worker-cert/fixtures/assets.json` — curated asset_ids for the smoke harness
- `tests/worker-cert/lib/pluck.sh` — M2M provisioning + worker-list + lease-scraping helpers
- `docs/worker_deployment.md` — operator-side deployment runbook
- `docs/operations/04-veloxediting-final-smoke-checklist.md` — e2e checklist (precedes worker-cert)

## Update history (chronological)

| Date | Worker | Step regressed / Smoked | Source |
| --- | --- | --- | --- |
| 2026-07-25 | `host_57_129_132_133` | Render ✅ — full 10-step PASS via `job_6138f86480ce8762` | historical (master log + smoke.json) |

> **Convention.** Append a row each time a smoke or pass_criteria run
> completes. Newest at the bottom. Do not delete rows — keep the
> regression trail visible.
