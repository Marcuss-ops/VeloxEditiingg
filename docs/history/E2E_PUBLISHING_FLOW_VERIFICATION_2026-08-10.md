# E2E Publishing Flow — Historical Verification Evidence

> Dated verification evidence moved from
> [`E2E_PUBLISHING_FLOW.md`](../E2E_PUBLISHING_FLOW.md).
> The parent runbook keeps a navigable compatibility section and the live
> operator procedure; this archive keeps the 2026-08-10 observation record.

<a id="6-operational-verification-of-the-live-execution-projection"></a>

## 6. Operational verification of the live execution projection

### 6.1 Run record — 2026-08-10 UTC

This run was performed against the local deployment of `main` without changing
workers or services. The admin bearer was read from the existing
`.velox/production.env` file and was never printed. Pre-flight showed four
`CONNECTED/HEALTHY` workers, each with `active_jobs=0`.

The canonical real-asset smoke was used, rather than synthetic
`velox-asset://clips/...` paths:

```bash
VELOX_MASTER_URL=http://127.0.0.1:8000 \
TOKEN_FILE=/home/pierone/Projects/company/VeloxEditiingg/.velox/production.env \
SMOKE_DESTINATION_ID=drive-smoke \
SMOKE_OUT_ROOT=/tmp/velox-operational-smoke-output \
SMOKE_POLL_TIMEOUT_S=180 \
  tests/worker-cert/smoke_one.sh velox-worker-13197
```

While the smoke was running, the operator monitor paired samples from:

```text
GET /api/v1/admin/jobs/{job_id}
scripts/fleetctl job inspect {job_id} --json
```

`fleetctl job inspect` intentionally uses the same canonical admin route. The
comparison therefore verifies the CLI/read-model path and JSON projection, but
is not an independent second backend.

### 6.2 Observed evidence

| Measure | Observed result | Verdict |
|---|---|---|
| Admin authentication | `GET /api/v1/admin/workers` → HTTP `200`, approximately 2 ms; M2M provisioning → HTTP `201` | PASS |
| Job submission | `POST /api/v1/jobs` → HTTP `202`; job `job_7949e532269dd6c4` created | PASS |
| Lease identity | `worker_id=velox-worker-13197`, `attempt_id` and `lease_id` present; `started_at=2026-08-10T11:20:27Z` | PASS for lease/read-model identity |
| Render terminal state | `FAILED` after 63 s; the smoke did not reach `SUCCEEDED` | FAIL — no successful render certification |
| Paired read-model snapshots | 45 admin/fleetctl response pairs; normalized selected fields matched in 44/45 pairs | PARTIAL; the divergent pair was not classified, so investigate it before claiming full convergence |
| `execution.attempts[]` | Both projections showed zero attempts while `PENDING` and one attempt at terminal failure; terminal attempt carried the same `attempt_id` and `worker_id` | PASS for observed durable projection convergence |
| Top-level live overlay | `execution.attempt_id`, `worker_id`, `phase`, `progress`, `live_metrics` and `last_progress_at` were absent in the captured terminal-failure path | NOT CERTIFIED — the job was no longer live |
| M2M cleanup | Ephemeral client DELETE → HTTP `200` | PASS |

The raw, unsanitized HTTP response files remain outside the repository at:

```text
/tmp/admin-job-operational-canonical-20260810T111937Z/
```

A sanitized reconstruction is stored at:

```text
/tmp/admin-job-operational-canonical-20260810T111937Z/reconstructed-samples.jsonl
```

These files are temporary operator evidence and are deliberately not committed;
they can contain job payload details. Protect them during analysis and remove
the raw response files and sanitized derivative after the evidence-retention
window (or immediately when no longer needed), according to local policy.

### 6.3 Latency and staleness result

The monitor captured the response pairs but did **not** persist the per-request
`curl -w '%{time_total}'` and `fleetctl` wall-clock timings. Consequently no
HTTP-latency percentile or average is claimed from this run. File modification
times are not a valid substitute for request timings and must not be used as a
latency SLO measurement.

The captured job also had no `last_progress_at` value in the live overlay: it
failed before a live-progress sample could be certified. Therefore
`now - last_progress_at` was not measurable and no staleness PASS is claimed.
For the next successful render, the monitor MUST persist for every pair:

```text
sampled_at
admin_http_status
admin_latency_s
fleetctl_latency_s
execution.attempt_id
execution.worker_id
execution.phase
execution.progress
execution.live_metrics
execution.last_progress_at
last_progress_staleness_s = sampled_at - last_progress_at
```

The operational acceptance target remains: progress updates should arrive every
1–2 seconds or on phase/segment transitions, and the observed
`last_progress_at` staleness should stay within that cadence plus transport
margin. A successful `SUCCEEDED` smoke with these fields and timings is still
required before closing the live-observability verification.

### 6.4 Follow-up required

1. Diagnose the worker/render cause of `job_7949e532269dd6c4` reaching
   `FAILED`; this run is not evidence of a successful render.
2. Re-run the canonical smoke with a corrected monitor that writes request
   timings before post-processing.
3. Require at least one non-terminal sample with non-empty
   `execution.attempt_id`, `worker_id`, `phase`, `progress`, `live_metrics` and
   `last_progress_at`, followed by terminal convergence.
4. Record latency min/average/max (and p95 where the evidence harness supports
   it) plus min/average/max staleness in the next report.
