# VeloxEditing final render smoke checklist

This runbook is the operational checklist for proving that the external
PipelineGen → Velox Master → remote worker render path works end to end.

Important routing rule: PipelineGen must never submit directly to a worker.
It submits to the Master only:

```text
PipelineGen
  -> POST /api/v1/jobs on Velox Master
  -> Velox queue
  -> any eligible connected worker
  -> artifact returned to Master
```

Worker 51 is the operator/local machine in the current setup. Do not pin or
force jobs to Worker 51 during smoke. The smoke must prove normal scheduling
to the available worker pool. A worker is eligible only if it is `CONNECTED`,
has `session_active=true`, and advertises `scene.composite.v1@1`.

## 0. Quick start: launch the live smoke

Current live Master:

```text
http://51.91.11.36:8000
```

The remote client running the smoke must be allow-listed twice:

- network firewall: TCP `8000` inbound to the Master;
- application allowlist: `VELOX_ALLOWED_WORKER_IPS` on the Master.

As of 2026-07-28, the following PipelineGen/smoke client IPs are allowed for
HTTP intake:

```text
45.63.21.236
77.93.152.122
```

Only the Master HTTP port is required for PipelineGen:

```text
PipelineGen/client -> 51.91.11.36:8000
```

Do not require PipelineGen to reach port `9000`. Port `9000` is for worker
gRPC sessions only.

From the remote client, verify reachability first:

```bash
export VELOX_MASTER_URL="http://51.91.11.36:8000"
curl -fsS "${VELOX_MASTER_URL}/ready"
```

Expected response:

```json
{"checks":6,"status":"ready"}
```

Then export a M2M token with `jobs.submit` scope. This must be the
`plaintext_secret` returned when creating the M2M client; it is not the admin
token.

```bash
export VELOX_M2M_TOKEN="<plaintext_secret_for_jobs.submit>"
```

Never commit or paste the real token into a tracked file. For an operator-run
smoke, retrieve it from the Master-side secret handoff or mint a short-lived
client via:

```http
POST /api/v1/admin/m2m/keys
```

using the Master admin token. The smoke itself must use only the returned M2M
`plaintext_secret`.

Run the operational smoke:

```bash
scripts/with-velox-auth \
  tests/operational/comedian_clips_generate_voiceover_subtitles.sh
```

The first successful submit response must contain:

```json
{
  "ok": true,
  "accepted_from": "api_v1_jobs",
  "dispatch_status": "queued_for_workers",
  "job_id": "job_..."
}
```

If the smoke fails before submit:

| Symptom | Meaning | Fix |
| --- | --- | --- |
| `connection refused` | Master is not reachable at host/port, or wrong URL. | Check `VELOX_MASTER_URL`, service status, and listener on `:8000`. |
| timeout | Firewall or upstream network block. | Add the client source IP to UFW/provider firewall for TCP `8000`. |
| `403 public access forbidden` | Application allowlist rejected the client IP. | Add the client source IP to `VELOX_ALLOWED_WORKER_IPS` and restart Master. |
| `VELOX_M2M_TOKEN=unset` | Client has no submit credential. | Export a valid M2M `plaintext_secret` with `jobs.submit`. |
| `401` from `/api/v1/jobs` | Token is wrong, inactive, expired, or not known by this Master DB. | Mint/export a new M2M token. |
| `422 destination_id` | Delivery destination is not registered/enabled. | Register and pass the exact destination ID selected for this run. |

For the current live smoke, use:

```json
{
  "delivery_plan": [
    {
      "destination_id": "<explicit-destination-id>",
      "priority": 1,
      "retry_budget": 3
    }
  ]
}
```

After submit, poll:

```bash
curl -fsS \
  -H "Authorization: Bearer ${VELOX_M2M_TOKEN}" \
  "${VELOX_MASTER_URL}/api/v1/jobs/${JOB_ID}"
```

The useful progression is:

```text
PENDING -> RUNNING -> SUCCEEDED
```

`202 Accepted` alone is not enough. Confirm worker-side processing through
task/attempt rows, Master logs (`TaskOffer`, `TaskAccepted`,
`TaskLeaseGranted`), and a non-empty artifact.

## 1. Final VeloxEditing job

- [ ] Prepare one final valid, versioned JSON payload or manifest.
- [ ] Include a unique and repeatable `idempotency_key`.
- [ ] Include the complete script.
- [ ] Include all scenes in deterministic order.
- [ ] Attach each scene to the correct clip.
- [ ] Attach each scene to the correct voiceover.
- [ ] Attach the correct SRT or ASS subtitles.
- [ ] Use real assets that are already `READY`.
- [ ] Use valid Velox asset references:

```text
velox-asset://<asset_id>
```

- [ ] Do not use local paths or path-like Velox references:

```text
velox-asset://voiceovers/file.mp3
C:\video\clip.mp4
/home/user/audio.mp3
```

- [ ] Use a registered and enabled destination.
- [ ] For the current live smoke use:

```json
"destination_id": "<explicit-destination-id>"
```

- [ ] Validate JSON before submit.
- [ ] Save an immutable copy of the submitted JSON.
- [ ] Save SHA-256 of the submitted payload or manifest.
- [ ] Submit via:

```http
POST /api/v1/jobs
```

- [ ] Receive `202 Accepted`.
- [ ] Save returned `job_id`.
- [ ] Re-submit the same payload with the same `idempotency_key` and confirm
      it does not create a duplicate job.

## 2. Worker pool and dispatch

Before submit:

- [ ] Master is ready.
- [ ] At least one non-Worker-51 worker is connected.
- [ ] Connected worker has `session_active=true`.
- [ ] Connected worker status is `CONNECTED`.
- [ ] Connected worker heartbeat is recent.
- [ ] Connected worker is allowed by Master policy.
- [ ] Connected worker advertises:

```text
scene.composite.v1@1
```

- [ ] gRPC credentials are valid.
- [ ] TLS certificates are valid.
- [ ] Worker work directory is writable.
- [ ] Worker has enough free disk.
- [ ] FFmpeg and renderer are available.
- [ ] Worker is not saturated.

Expected flow:

- [ ] PipelineGen submits the job to the Master.
- [ ] Master validates payload or `manifest_ref`.
- [ ] Master creates Job, Task, and TaskSpec.
- [ ] Job enters queue.
- [ ] A connected eligible worker receives the lease.
- [ ] Log contains `TaskOffer`.
- [ ] Log contains `TaskAccepted`.
- [ ] Log contains `TaskLeaseGranted`.
- [ ] Job reaches `RUNNING`.
- [ ] Worker validates payload and assets.
- [ ] Worker downloads or resolves clip, voiceover, and subtitles.
- [ ] Worker verifies hashes, duration, and format where provided.
- [ ] Any subworker work is dispatched only through the supported dispatcher.
- [ ] Every subtask has a task ID and attempt ID.
- [ ] Subworkers report heartbeat and progress when used.
- [ ] No scene is processed twice.
- [ ] Subworker failure triggers controlled retry.
- [ ] Job does not remain indefinitely in `LEASED` or `RUNNING`.
- [ ] Scene outputs are composed in original order.
- [ ] Worker returns final artifact to the Master.
- [ ] Job reaches `SUCCEEDED`.

Minimum proof that a remote worker actually worked:

- [ ] `tasks.worker_id` is an eligible remote worker, not Worker 51.
- [ ] A `task_id` exists.
- [ ] An `attempt_id` exists.
- [ ] Master log contains `TaskOffer`.
- [ ] Master log contains `TaskAccepted`.
- [ ] Master log contains `TaskLeaseGranted`.
- [ ] Worker/render log shows renderer start.
- [ ] Worker/render log shows completion or failure.
- [ ] Worker payload hash matches submitted payload/manifest hash when the
      receipt endpoint is available.
- [ ] Final artifact exists.
- [ ] Final artifact size is greater than zero.

## 3. Performance and analytics

- [ ] Record submit time.
- [ ] Record enqueue time.
- [ ] Record lease time.
- [ ] Record render start time.
- [ ] Record completion time.
- [ ] Calculate queue time.
- [ ] Calculate asset download time.
- [ ] Calculate render time.
- [ ] Calculate total end-to-end time.
- [ ] Record selected worker.
- [ ] Record average and peak CPU.
- [ ] Record average and peak RAM.
- [ ] Record temporary disk usage.
- [ ] Record download speed.
- [ ] Record retry count.
- [ ] Record successful and failed scene count.
- [ ] Record produced video duration.
- [ ] Record final artifact size.
- [ ] Record precise error if status is `FAILED`.
- [ ] Verify logs and metrics do not expose tokens or credentials.
- [ ] Save final smoke result JSON:

```json
{
  "smoke_test": "velox_final_render",
  "status": "passed",
  "job_id": "job_...",
  "worker_id": "host_...",
  "queue_time_ms": 1200,
  "render_time_ms": 28400,
  "total_time_ms": 32100,
  "scene_count": 12,
  "retry_count": 0,
  "artifact_size_bytes": 56393
}
```

## 4. Important phrases, names, and keywords

For every scene:

- [ ] Receive important phrases from PipelineGen.
- [ ] Receive special names.
- [ ] Receive important words.
- [ ] Every element has a stable ID.
- [ ] Every element has `scene_id`.
- [ ] Every element has start timestamp.
- [ ] Every element has duration.
- [ ] Every element has graphic preset.
- [ ] Every element uses a standard role.

Example layer payload:

```json
{
  "layers": [
    {
      "id": "important-phrase-001",
      "type": "text",
      "role": "important_phrase",
      "text": "Una scoperta incredibile",
      "start_seconds": 4.2,
      "duration_seconds": 2.5,
      "preset": "important_phrase_v1",
      "animation": "fade_up"
    },
    {
      "id": "special-name-001",
      "type": "text",
      "role": "name",
      "text": "Jackie Chan",
      "start_seconds": 8.1,
      "duration_seconds": 3,
      "preset": "person_name_v1",
      "animation": "slide_left"
    }
  ]
}
```

Visual checks:

- [ ] Text appears in the correct scene.
- [ ] Text does not appear too early or too late.
- [ ] Text does not cover subtitles or faces.
- [ ] Text stays inside frame bounds.
- [ ] Required fonts exist on the worker.
- [ ] Unicode renders correctly.
- [ ] Accents and special characters render correctly.
- [ ] Animations are smooth.
- [ ] No duplicate layer is rendered.
- [ ] No layer remains visible beyond its expected duration.
- [ ] Running the same payload twice is deterministic.

## Final PASS criterion

The smoke passes only when all of the following are true:

```text
Master READY
+ at least one eligible non-Worker-51 worker CONNECTED
+ valid payload
+ READY assets
+ POST returns 202
+ job enters queue
+ eligible worker receives lease
+ job reaches RUNNING
+ clip, voiceover, and subtitles are resolved/downloaded
+ important layers are applied
+ job reaches SUCCEEDED
+ artifact exists and is valid
+ analytics are saved
```

Stopping at `202 Accepted` proves only Master acceptance. It does not prove
that a remote worker processed the job.
