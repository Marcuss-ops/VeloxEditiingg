# E2E publishing flow — operator runbook

> **Audience:** SRE / on-call. **Scope:** the canonical end-to-end wire proof
> for the new Velox → InstaEdit publishing flow on `main`, exercised by
> `scripts/e2e/publishing_flow_smoke.sh`.
> **Owner:** Velox core platform. **Review cadence:** after any change
> to `DataServer/internal/handlers/server/pipeline/publishing_targets.go`,
> to `internal/jobs/enqueue/delivery_plan_validator.go`, to
> `internal/deliveries/state.go` on the InstaEdit side, or to the
> `/api/v1/jobs` wire contract.

This runbook is the operator-facing companion to
`scripts/e2e/publishing_flow_smoke.sh`. It explains **what** the smoke
exercises, **what** it does NOT exercise, **how** to invoke it against a
production-like environment, **what** each exit code means, and **what**
the canonical failure modes look like.

The smoke is the operator-side **wire proofing** of the publishing flow.
It is NOT a substitute for the regression-net at the application layer
(`DataServer/internal/socialclient/*` and the matching InstaEdit
resolver tests). See §5 below for the
complementary-pinning cross-reference.

---

## 1. What the smoke exercises (Velox → InstaEdit)

The smoke is a single executable bash + curl + jq script. It walks the
canonical publishing chain in 6 numbered phases plus cleanup:

| # | Phase | Endpoint | Why |
|---|---|---|---|
| 1 | Auth bootstrap | `POST /api/v1/admin/m2m/keys` (Velox admin) | Mint an ephemeral M2M client with `jobs.submit` scope. Reused across all subsequent calls. `cleanup` trap DELETEs it on every exit path. |
| 2 | Catalog discovery | `POST /api/v1/publishing/targets` (Velox M2M) | Worker asks the principal API for "what CAN I publish to in workspace X?" — picks the first entry where `can_post == true` AND `capabilities.upload_video == true`. Captures the Velox-side opaque `destination_id` and the cross-repo `external_destination_id`. |
| 3 | Job submission | `POST /api/v1/jobs` (Velox M2M) | Submit a minimal one-clip + one-voiceover scene with `delivery_plan[0].destination_id = <velox-side id>`. The opaque id is the authoritative pick; the metadata block does NOT echo channel / platform_account_id — that would violate the contract. |
| 4 | Job polling | `GET /api/v1/jobs/{id}` (Velox M2M) | Exponential backoff 1s → 2s → 4s → 8s → 16s, capped at `$PUBLISHING_POLL_TIMEOUT_S` (default **300s = 5 minutes** because the canonical render + chunked upload takes real time). |
| 5 | External-delivery discovery | best-effort body scan | The Velox job body may carry `deliveries[].remote_id` / `external_delivery_id` / `social_delivery_id` (any/all are tolerated). If absent, the script not-skips step 6; it logs and proceeds. |
| 6 | Private-upload verification (optional) | `GET ${INSTAEDIT_BASE_URL}/api/v1/integrations/velox/deliveries/{remote_id}` (InstaEdit) | Polls until `status == "PRIVATE_UPLOADED"`. Skipped silently when `INSTAEDIT_BASE_URL` is unset. |

A run completes successfully (exit 0) iff:

* phase 1 minted a M2M bearer and the trap will clean it up;
* phase 2 found at least one publishable target satisfying `can_post=true && capabilities.upload_video=true`;
* phase 3 submitted a job under that destination_id;
* phase 4 polled until the job reached `SUCCEEDED` (terminal) without `FAILED` / `CANCELLED` or timeout;
* phase 5 discovered the `remote_id` AND phase 6 reached `PRIVATE_UPLOADED` UNLESS `INSTAEDIT_BASE_URL` is unset (in which case phase 6 is skipped and the smoke still exits 0).

The smoke deliberately does NOT depend on a specific Velox enumeration
endpoint for `remote_id`. It mines the polled job response for any of
the well-known field names so a future API version drift cannot break
the smoke silently; instead, the `jq -er` paths return null and we log
a notice + proceed.

---

## 2. Prerequisites

* `bash` ≥ 4 (uses `[[ ]]` and process substitution),
* `curl` (with HTTPS support for production URL),
* `jq` ≥ 1.5,
* `VELOX_ADMIN_TOKEN` (or `TOKEN_FILE` pointing at a dotenv that exports
  `VELOX_ADMIN_TOKEN=…`) — the **only** secret the smoke reads directly
  with the admin scope; all downstream HTTP calls use the ephemeral M2M
  secret minted in phase 1,
* `PUBLISHING_WORKSPACE_ID` — an integer workspace id whose catalog has
  at least one channel satisfying the four publish-readiness conditions:
  * workspace binding enabled,
  * platform account active (`status='active'`, `reauth_required_at IS NULL`),
  * OAuth valid (caller's interpretation; at the resolver boundary
    the proxy is `status='active'` and the worker layer audits
    `expires_at > NOW()` + scope),
  * external destination enabled (`external_destinations.enabled = true`,
    `source_system='velox'`).
  This last block is regression-pinned in
  `InstaeditLogin/internal/deliveries/target_resolver_publish_ready_test.go`
  — see §5.

The smoke assumes the master has at least one connected worker
registered to `VELOX_ALLOWED_WORKERS`. Without a worker, the polling
phase will time out (exit 8).

---

## 3. Environment variables

| Variable | Default | Required? | Purpose |
|---|---|---|---|
| `VELOX_MASTER_URL` | `http://127.0.0.1:8080` | no | Velox master base URL. Strip trailing slash internally. |
| `VELOX_ADMIN_TOKEN` | (none) | **yes** | Admin bearer for phase 1 M2M issuance. Source precedence: env > `TOKEN_FILE` dotenv (mirrors `creator_push_smoke.sh`). |
| `TOKEN_FILE` | (none) | opt | Path to a dotenv file with `KEY=VALUE` lines. Reused by all the smokes. |
| `PUBLISHING_WORKSPACE_ID` | (none) | **yes** | Integer workspace id used by /targets. |
| `PUBLISHING_PLATFORM` | `youtube` | opt | Platform string passed to /targets. |
| `PUBLISHING_PLATFORM_ACCOUNT_ID` | (none) | opt | Integer scalar filter — useful when a workspace has many channels. |
| `INSTAEDIT_BASE_URL` | (none) | opt (recommended for prod) | Enables step 6 (PRIVATE_UPLOADED poll). Unset = master-only smoke; exit 0 after Velox SUCCEEDED. |
| `PUBLISHING_POLL_TIMEOUT_S` | `300` | opt | Hard cap on job polling. 5 min default because the canonical chunked upload is real-time. NEVER set below 60s in production. |
| `PUBLISHING_PRIVATE_TIMEOUT_S` | `300` | opt | Hard cap on InstaeditLogin private-upload polling. |
| `PUBLISHING_IDEM_KEY` | `pub-smoke-${epoch}-${pid}` | opt | Stable idempotency key override — for CI matrix re-runs that assert the same job. |
| `PUBLISHING_FLOW_DEBUG` | `0` | opt | `1` enables curl verbose + response dumps. |
| `PUBLISHING_FLOW_DRY` | `0` | opt | `1` prints the would-be request bodies + exits 0 without any live HTTP call. |

---

## 4. Step-by-step

### 4.1 One-shot dry run (no live calls)

```bash
PUBLISHING_FLOW_DRY=1 PUBLISHING_WORKSPACE_ID=42 ./scripts/e2e/publishing_flow_smoke.sh
```

Prints the would-be phase-1 + phase-2 request bodies and exits 0.
Useful in CI smoke matrices that don't have a live master.

### 4.2 Master-only smoke (no InstaEdit side net access)

```bash
VELOX_ADMIN_TOKEN=… PUBLISHING_WORKSPACE_ID=42 \
  ./scripts/e2e/publishing_flow_smoke.sh
```

Run from any host that can reach `VELOX_MASTER_URL`. Skips step 6
because `INSTAEDIT_BASE_URL` is unset. Exit 0 after Velox SUCCEEDED.

### 4.3 Full cross-repo smoke (recommended for production smoke matrices)

```bash
VELOX_ADMIN_TOKEN=… PUBLISHING_WORKSPACE_ID=42 \
INSTAEDIT_BASE_URL=https://instaedit.example.com \
PUBLISHING_POLL_TIMEOUT_S=600 PUBLISHING_PRIVATE_TIMEOUT_S=600 \
  ./scripts/e2e/publishing_flow_smoke.sh
```

Exits 0 only after both Velox SUCCEEDED and InstaeditLogin
PRIVATE_UPLOADED. Operators chaining this into a daily cron should
also set `PUBLISHING_IDEM_KEY=wdy-<YYYY-MM-DD>` so a re-run of the
cron within the same day picks up the same job (idempotency contract
re-applies cleanly).

---

## 5. Exit code map and troubleshooting

| Exit | What failed | Operator action |
|---|---|---|
| 0 | All reachable phases converged on success (Velox SUCCEEDED + InstaEdit PRIVATE_UPLOADED when both endpoints are reachable). | None. |
| 2 | Missing env or wrong flag. | Print the `FATAL:` line to find which variable / flag was wrong. |
| 3 | `curl` could not reach `VELOX_MASTER_URL` (or `INSTAEDIT_BASE_URL` in step 6) before `-m` timeout. | Check network egress; check service health (`curl -sf ${MASTER_URL}/ready`). |
| 4 | HTTP non-{200,202} on a Velox endpoint. | Print the response body that the smoke dumps. Common 401 ⇒ admin token rotated; 422 ⇒ contract drift on /api/v1/jobs. |
| 5 | `/publishing/targets` did not surface a `can_post=true && capabilities.upload_video=true` entry. | Inspect the targets array. Common cause: every row is `reauth_required` (channel requires OAuth re-consent) or `external_destination.enabled=false`. Re-run the `target_resolver_publish_ready_test.go` baseline on InstaEdit side to diagnose. |
| 6 | HTTP 202 from `/api/v1/jobs` but `job_id` is empty. | Wire-shape drift on the jobs handler. Roll-back the recent commit touching `job_submit.go` and re-run. |
| 7 | Job reached `FAILED` or `CANCELLED` while polling. | Print the last response body. If the job is `FAILED` with `last_error_code=DESTINATION_UNMAPPED`, the InstaEdit-side `external_destinations` row has been pruned — co-ordinate with the InstaEdit repo to seed it. |
| 8 | Polling exhausted `PUBLISHING_POLL_TIMEOUT_S` without a terminal state. | Raise the cap (the canonical 5-minute budget covers render + upload); check the master log for the dispatched job; verify at least one worker is registered. |
| 9 | PRIVATE_UPLOADED was not reached on InstaeditLogin side within `PUBLISHING_PRIVATE_TIMEOUT_S` (only checked when `INSTAEDIT_BASE_URL` is set). | Inspect the InstaeditLogin internal log for the worker pub path; common cause: YouTube API rate-limit or `blocked_auth` from a stale channel. |

---

## 5. Cross-references

* Regression-pinning: `InstaeditLogin/internal/deliveries/target_resolver_publish_ready_test.go`
  exercises the canonical happy-path through the unified TargetResolver
  + ListWorkspaceTargets + CapabilitiesForTarget. A drift on the
  four publish-readiness conditions breaks that pinning test first.
* Social-client boundary: `VeloxEditiingg/DataServer/internal/socialclient/config.go`
  honors only `SOCIAL_API_URL / SOCIAL_API_TOKEN / SOCIAL_API_TIMEOUT_MS
  / SOCIAL_API_RETRIES / SOCIAL_CALLBACK_BASE_URL`. The canonical env
  validator on the *Velox* master side is `deploy/validate-master-env.sh`
  — `bash -n` confirmed, parse-int positive-int helper, regex-tightened
  legacy `SOCIAL_GATEWAY_*` alias detection.
* Single-script M2M-provisioning anchor: `scripts/api/jobs_smoke.sh`
  (the same `resolve_token` + `trap cleanup EXIT` + exponential backoff
  patterns are inherited).
* Operator-facing admin surface (used by the smoke via
  `VELOX_ADMIN_TOKEN`): `DataServer/api/openapi.yaml::bearerAdminToken`
  — referenced by `scripts/api/validate_openapi.py`.
* Spec doc: `docs/velox-instaedit-contract.md` (cross-repo state
  machine invariants, opacity rules, the `PRIVATE_UPLOADED` boundary).

---

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

## 7. Anti-patterns (do NOT do)

* **Do NOT** echo `VELOX_ADMIN_TOKEN` or the ephemeral M2M bearer at any
  log level — the smoke's `*redacted*` marker is a hard rule; flag a
  regression to `code-reviewer-minimax-m3` if a future re-edit re-introduces
  a leak path.
* **Do NOT** extend the smoke to do a real YouTube API call. The smoke
  is the wire layer between Velox master and InstaeditLogin; the actual
  YouTube upload is owned by the InstaeditLogin worker.
* **Do NOT** scope `PUBLISHING_POLL_TIMEOUT_S < 60s` — the chunked upload
  on a real worker routinely takes 30–60s.
* **Do NOT** omit the M2M-cleanup trap. A leaked M2M client is a secret
  in `cfg_auth_m2m` and pollutes the next operator's audit.
