# Fixed Direct Jobs

## PRE job intake

`POST /api/v1/jobs/pre` supports both the legacy two-stage flow and a
single-stage flow. The Master decides whether the runtime-asset gate is
already complete from the PRE request:

| PRE request | Response | Next step |
|---|---|---|
| `runtime_assets` is non-empty, or `runtime_payload.runtime_assets` is non-empty | `202 {job_id, dispatch_status: "prefetch_queued"}` | No FINALIZE is required; the worker prefetches the declared runtime assets. |
| `runtime_assets_complete: true` | `202 {job_id, dispatch_status: "prefetch_queued"}` | Explicitly certifies completion, including an intentionally empty asset list. |
| Runtime assets omitted, or empty without the explicit flag | `202 {job_id, dispatch_status: "waiting_runtime_assets"}` | Send FINALIZE later with the runtime assets. |
| `runtime_assets_complete: false` | `202 {job_id, dispatch_status: "waiting_runtime_assets"}` | Explicitly keeps the job pending, even when assets are present. |

The explicit completion flag is available on the canonical `SubmitJobRequest`
wire schema as well as the handler type. `null` and `[]` nested under
`runtime_payload.runtime_assets` do not complete the gate; use
`runtime_assets_complete: true` when an empty list is intentional.

### Two-stage job intake

For jobs whose overlay/audio assets are generated after the initial stock and
clip inputs, use the same job identity for both stages:

```text
POST /api/v1/jobs/pre
  -> 202 {job_id, dispatch_status: "waiting_runtime_assets"}
POST /api/v1/jobs/{job_id}/finalize
  -> 202 {job_id, future_asset_plan: "refresh"}
```

The PRE payload must include `copy_only: true`. This is the explicit
clips.v1 packet-copy admission contract; a later `replace` overlay may select
the editorial re-encode path, but the renderer still requires this input gate
to be present during validation. The PRE stage carries only the stock/clip
timeline; FINALIZE adds the runtime assets to the same job.

For the single-stage branch, include the complete runtime asset declaration in
the PRE payload. For the two-stage branch, omit it (or leave the gate pending)
and use FINALIZE after the generated assets are available.

The finalize payload accepts typed `overlays[]` in `replace` mode with the
canonical half-open window `[start_frame,end_frame)` (for example
`start_frame: 120, end_frame: 240`), plus extensible `runtime_payload` /
`runtime_assets` fields for TTS, BGM and SFX. The Master updates the existing
task atomically and keeps its FutureAssetPlan reservation on the same worker.

Raw `mode: "composite"` overlays are rejected at intake: the native worker
cannot composite them, and downloading the source overlay before discovering
that would waste a prefetch cycle. Generate each finished, video-only MP4
before FINALIZE (the asset must contain the complete picture for its window,
not a transparent layer). For a legacy `clips.v1` job, send one `replace`
overlay covering the complete prepared window; this uses the editorial
re-encode path. For a strict V2 job, add the MP4 to the completed
`render_manifest.assets` and send `visual_replacements[]` with its asset ID
and absolute timeline window. FINALIZE recompiles the V2 plan against the
original PRE timeline and completed asset list, then refreshes prefetch for
the same reserved task.
The PRE video timeline, output contract, and final audio must remain identical;
existing compiled asset identities and metadata must be preserved. The MP4
must match the declared canonical video profile and have a duration matching
its replacement window.

```json
{
  "idempotency_key": "job-123-finalize-v1",
  "render_manifest": { "...": "completed manifest with the PRE assets plus finished MP4 assets" },
  "visual_replacements": [
    {
      "replacement_id": "scene-3-composite",
      "asset_id": "finished-scene-3-mp4",
      "sha256": "<sha256 from the completed manifest>",
      "timeline_start_us": 33833333,
      "timeline_end_us": 38833333,
      "profile_id": "<canonical profile declared by the manifest>"
    }
  ]
}
```

The runtime audio IDs point to entries in the same `runtime_assets` list; the
worker resolves their verified local cache paths and mixes them in one final
audio contract:

```json
{
  "overlays": [
    {
      "id": "overlay-1",
      "asset_id": "drive-overlay-id",
      "start_frame": 120,
      "end_frame": 240,
      "frame_count": 120,
      "mode": "replace",
      "audio_mode": "preserve_final_audio"
    }
  ],
  "runtime_assets": [
    {"asset_id": "tts-1", "kind": "audio", "role": "tts", "url": "velox-drive://..."},
    {"asset_id": "music-1", "kind": "audio", "role": "music", "url": "velox-drive://..."},
    {"asset_id": "sfx-1", "kind": "audio", "role": "sfx", "url": "velox-drive://..."}
  ],
  "runtime_payload": {
    "runtime_audio": {
      "tts_asset_id": "tts-1",
      "music_asset_id": "music-1",
      "sfx_asset_id": "sfx-1",
      "sfx_start_seconds": 2.5
    }
  }
}
```

`music_asset_id` is looped at the default background volume; SFX and TTS use
their declared duration metadata unless an explicit runtime duration is sent.
An ID without a resolved `runtime_assets[*].url` fails closed before render.

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
assets as frame-native `overlays`, each with an explicit half-open window
`[start_frame,end_frame)`: 120–240, 240–321, 321–441, 441–561 and
561–681 at 24 fps (5.000–10.000, 10.000–13.375, 13.375–18.375,
18.375–23.375 and 23.375–28.375 seconds). `frame_count` remains present as
a compatibility projection and must equal `end_frame-start_frame`. It starts in `replace` mode for
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
