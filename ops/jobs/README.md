# Fixed Direct Jobs

## Official generator benchmarks

Frozen certification workloads (registry: `tests/benchmarks/video-generator/cases/registry.json`):

| Benchmark | Payload | Submit |
|---|---|---|
| minimal | `benchmark-minimal.generate.json` | `submit_benchmark_minimal.sh` (M2M `/api/v1/jobs`, needs `VELOX_BENCHMARK_CLIP_URL` + `VELOX_BENCHMARK_VOICEOVER_URL`) |
| heavy | `benchmark-heavy.generate.json` | `submit_benchmark_heavy.sh` (M2M `/api/v1/jobs`, needs the 7 `VELOX_BENCHMARK_*_URL` asset vars) |
| pathological | `benchmark-pathological.generate.json` | `submit_benchmark_pathological.sh` (M2M `/api/v1/jobs`, expects clean terminal FAILED) |

The minimal/heavy payloads carry frozen placeholder asset URLs; the submit
scripts replace them from `VELOX_BENCHMARK_*_URL` env vars. Override the
idempotency key with `VELOX_BENCHMARK_IDEM_KEY` for cold/warm cache runs.
M2M scripts mint an ephemeral M2M client via the admin surface
(`VELOX_ADMIN_TOKEN`) and delete it on exit, mirroring `scripts/api/jobs_smoke.sh`.

### Delivery destination requirement

`delivery_plan` is **required** at enqueue, and every `destination_id` must
exist as an enabled row in the deployment's `delivery_destinations` table
(otherwise the job is rejected with `DESTINATION_NOT_FOUND`). The frozen
payloads reference `destination_id: "drive"`; if your deployment uses a
different id, override it with
`VELOX_BENCHMARK_DELIVERY_DESTINATION=<your-destination-id>`. The scripts
deliberately never strip the plan.


## Tyson overlay-position test

`mike_tyson_intro_stock.creator-push.json` remains the untouched canonical
Tyson source payload. To test five new Drive overlay assets at different
positions over the existing intro clip and stock timeline, run:

```bash
./ops/jobs/submit_mike_tyson_overlay_positions.sh
```

The runner creates a temporary Creator Push payload, keeps
`preserve_final_audio`, submits it through `/api/v1/creator/jobs`, and polls
the admin-only `/api/v1/admin/jobs/<job_id>` status surface until the render
reaches a terminal state. The public `/api/v1/jobs/<job_id>` route remains
M2M-owned and is not valid for a Creator Push job. The five
windows are 1–6s, 30–35s, 65–70s, 120–125s and 200–205s at 24 fps.

## fMP4 final assembly jobs

fMP4 is selected per job by the producer-owned `CompiledRenderPlanV2`, not by
the worker flag alone. The plan must contain:

```text
output.profile_id=velox-h264-fmp4-stream-v1
final_audio.mode=FINAL_AUDIO_COPY
```

After the producer has emitted the complete plan for a job, verify and submit
it through the canonical M2M intake:

```bash
scripts/ops/verify-fmp4-producer-job.sh --plan /path/to/plan.json
```

For the Tyson job recorded in
`mike_tyson_intro_stock.creator-push.json`, use the dedicated wrapper once
PipelineGen has produced the final V2 plan:

```bash
ops/jobs/verify_mike_tyson_fmp4.sh /path/to/mike-tyson-compiled-render-plan-v2.json
```

The existing Tyson Creator push remains the source/scene input and keeps its
legacy `scene.composite.v1` identity; the wrapper submits the separate final
fMP4 assembly job. It fails before submission if the plan does not explicitly
select the fMP4 profile. Set `VELOX_TYSON_FMP4_DESTINATION` when the deployment
uses a destination other than `drive-production`.

### Overlay timing acceptance

`mike_tyson_intro_stock.creator-push.json` carries the five supplied Drive
assets as frame-native `overlays`: 120–240, 240–321, 321–441, 441–561 and
561–681 at 24 fps (5.000–10.000, 10.000–13.375, 13.375–18.375,
18.375–23.375 and 23.375–28.375 seconds). It starts in `replace` mode for
the packet-copy path. For a Chronon test, change the five `mode` values to
`composite`; the resolver partitions the active windows and Velox still gets
one prepared canonical video track. Overlay audio is excluded through
`audio_mode=preserve_final_audio`, so TTS/music/SFX remain the single final
audio copy.

Each overlay keeps both `asset_id` and `drive_file_id`, plus the supplied
Drive file URL for authoring traceability. Enqueue canonicalizes that URL to
`velox-drive://<drive_file_id>`; the worker downloads it through the existing
authenticated Master asset bridge and never receives a Drive credential in the
job payload.
