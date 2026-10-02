# Creator-initiated job push

> Per l'integrazione architetturale (relazione con `CreatorForwardingRunner`, single-writer invariant), vedi `docs/architecture/current-architecture.md §12 Due percorsi di intake, un solo writer`.
>
> Contratto HTTP machine-readable: `DataServer/api/openapi.yaml` (OpenAPI 3.1.0, operazione `pushCreatorJob`, schema `CreatorPushRequest` / `CreatorPushAcceptedResponse` / `RemotePipelineResult`, security scheme `bearerAdminToken` su `VELOX_ADMIN_TOKEN`). Questo file `.md` è la narrativa; lo yaml è il source-of-truth per generatori client / validator OpenAPI / dashboard Swagger. In caso di divergenza, il comportamento testato in `DataServer/internal/handlers/server/pipeline/creator_push_e2e_test.go` è l'autorità finale.

A creator machine can submit a completed render payload directly to the Velox master. The master does not need to create the creator job first and does not poll the creator.

## Flow

```text
Creator builds voiceover + stock/clips + scenes
        ↓
POST /api/v1/creator/jobs
        ↓
remoteengine typed DTO adapter
        ↓
creatorflow.Resolver
        ↓
creator_forwardings + Job + TaskSpec (atomic)
        ↓
normal Velox worker dispatch
```

The endpoint reuses the same canonical resolver and atomic Job+Task writer used by the existing master-initiated creator flow. It does not introduce a second queue or a second database writer.

## Request

`POST /api/v1/creator/jobs`

Headers:

```text
Authorization: Bearer <VELOX_ADMIN_TOKEN>
Content-Type: application/json
```

Body:

```json
{
  "source_provider": "creator_pc_1",
  "source_job_id": "creator-job-20260725-001",
  "target_executor_id": "scene.composite.v1",
  "payload": {
    "status": "completed",
    "job_id": "creator-job-20260725-001",
    "video_name": "Example video",
    "script_text": "The completed script",
    "voiceover_paths": [
      "velox-asset://voiceovers/example.mp3"
    ],
    "scenes": [
      {
        "text": "Opening scene",
        "clip_link": "velox-asset://clips/opening.mp4",
        "duration_seconds": 7
      }
    ],
    "delivery_plan": [
      {
        "destination_id": "drive",
        "priority": 1,
        "retry_budget": 3
      }
    ]
  }
}
```

`source_provider` defaults to `creator`. `source_job_id` may be omitted when `payload.job_id` is present. `target_executor_id` defaults to `scene.composite.v1`.

The idempotency identity is:

```text
source_provider + source_job_id + target_executor_id
```

Sending the same completed creator job again converges on the same Velox Job and forwarding row.

## Asset rule

Do not send local creator paths such as `C:\clips\video.mp4` or `/home/creator/audio.mp3`. Subworkers cannot read another computer's filesystem. Every voiceover, stock clip, image and subtitle reference must be either:

- a `velox-asset://` reference already resolvable by the master; or
- an HTTP(S) URL reachable by the master and workers.

For a stock pool, `scenes[].stock` may contain one Drive folder reference:

```json
"stock": [
  {"url": "https://drive.google.com/drive/folders/<FOLDER_ID>"}
]
```

The master expands that folder (and nested folders) to its video files before
the payload is persisted. The worker then downloads those file references,
deterministically shuffles the pool per job and scene, loops it until the
scene voiceover ends, and trims the final segment to the exact duration. A
folder with no video files, or a folder job without the Drive listing
capability, is rejected at intake; it is never treated as one media file.

### Canonical Creator upload

To transfer a local file from the Creator computer, upload it first:

```bash
curl -sS -X POST "${VELOX_MASTER_URL}/api/v1/creator/assets" \
  -H "Authorization: Bearer ${VELOX_ADMIN_TOKEN}" \
  -F "kind=stock_clip" \
  -F "file=@./clip.mp4"
```

The `201 Created` response contains `reference`, for example
`velox-asset://<sha256>`. Put that reference in the subsequent job payload.
The upload is size-limited, SHA-256 content-addressed, deduplicated and
stored by the same asset registry used by normal job intake; the binary is
never embedded in the job JSON.

## Response

The master returns `202 Accepted` after the payload has been converted and queued for normal worker dispatch.

```json
{
  "ok": true,
  "accepted_from": "creator_push",
  "source_provider": "creator_pc_1",
  "source_job_id": "creator-job-20260725-001",
  "target_executor_id": "scene.composite.v1",
  "job_id": "job_...",
  "status": "PENDING",
  "dispatch_status": "queued_for_workers"
}
```

A syntactically valid but incomplete creator payload returns `422 Unprocessable Entity` and is not written as a Job.

A syntactically valid payload carrying a soft-deprecated scene
declaration returns `202 Accepted` **plus** a `warnings[]` entry
(`code: "kind_clip_without_clip_asset"`, affected `scene_ids`,
`sunset` date) — see "Scene declaration contract" below. The job is
created normally; the warning tells the generator what to fix before
the sunset hard-rejection.

## Scene declaration contract: `kind`, `clip`, `stock`, and final audio

> Authority: `shared/contract/scene_kind_clip.go` (soft-deprecation +
> sunset), worker `RemoteCodex/native/worker-agent-go/pkg/video/pipelines/clips/scene_timeline.go`
> (render behavior). This section is the narrative; the code is the
> source of truth.

The worker **ignores the informational `kind` field** and renders purely
from the `clip` / `stock` fields:

| Scene shape | Video | Audio |
|---|---|---|
| `clip: {url, ...}` | the clip, `duration_ms` window | `scene_clip_audio` track — **unless a final mix owns the timeline** (see below) |
| `stock: [...]` | background pool, looped to `duration_seconds` | **always mute** (`IncludeAudio: false`, no audio track) |

Two consequences that caused the 2026-10-02 Isabelle incident
(`job_e2adca259c034c1e` — testimony scenes declared `kind: "clip"`
with the asset in `stock`):

1. **`kind: "clip"` without a `clip` object is a declaration bug.**
   The scene compiles as a mute stock background and testimony audio
   can never reach the montage, while the transfer itself stays
   intact (the MP4 verifies fine — it faithfully contains the wrong
   montage). Since 2026-10-02 this shape is **soft-deprecated**: the
   intake still accepts the payload but records
   `pipeline_intake_scene_warnings_total{path,reason="kind_clip_without_clip_asset"}`,
   logs a WARN with the affected `scene_id`s, and echoes a `warnings[]`
   entry (with `sunset: "2026-11-15"`) in the 202 envelope. After the
   sunset date the same shape is rejected with 422. Background scenes
   must use `kind: "stock"`; testimony scenes must carry
   `clip: {url, ...}` with `duration_ms` equal to the scene window.
   Validate before submitting with `POST .../creator/jobs?dry_run=true`
   (or `POST .../jobs?dry_run=true`): the `kind_clip_without_clip`
   list in the summary shows exactly what would warn.

2. **With a final runtime mix present, scene clip audio is dropped by
   design.** When `runtime_audio` carries the final narration mix, the
   worker omits every `scene_clip_audio` track (the mix owns the
   timeline) — so fixing `stock` → `clip` under a final mix yields a
   byte-identical MP4. If testimony sources must be audible, either
   mix them into the final narration before submitting, or submit
   without a final mix (the worker then mixes scene tracks itself;
   the mixed output must still cover the timeline or the packet-mux
   gate fails closed with `audio_duration_mismatch`).

Debug helper: `scripts/ops/diff-job-payloads.sh <job-a> <job-b>`
compares two jobs' TaskSpec payloads (per-scene clip/stock
declarations, overlays, runtime assets/audio, final artifacts) plus
their `[RENDER_INPUT_DIAGNOSTIC]` placement lines, which now also
report `kind_clip_without_clip=` and `clip_audio_omitted=`.

## Which master, which endpoint

Velox renders go to the **Velox master** (`POST /api/v1/creator/jobs`
with `VELOX_ADMIN_TOKEN`, or `POST /api/v1/jobs` with a per-client M2M
secret — see `docs/API-JOBS.md`). Other pipeline services expose
their own job types on their own hosts; a Velox `scene.composite.v1`
payload POSTed there is rejected (`job type is not external-safe`)
before any job exists. When an intake is rejected, check the host +
endpoint pair first.

## Removal: `/api/remote/pipeline` fully retired

The legacy sync-forward endpoint `/api/remote/pipeline` (and the
internal `forwardPipelineResultToWorker` / `syncForwardResult`
machinery it backs) has been **removed** from `main`. The canonical
creator-push intake is `POST /api/v1/creator/jobs` (this document).
External clients that were still POSTing to `/api/remote/pipeline`
now receive `404 Not Found` and MUST migrate.

### Sample migration (curl)

The canonical intake endpoint accepts the same `RemotePipelineResult`
shape the legacy route used internally. A working curl (see also
`scripts/creator_push_smoke.sh` for the executable variant):

```bash
curl -X POST https://velox.example.com/api/v1/creator/jobs \
  -H "Authorization: Bearer $VELOX_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "source_provider": "creator_pc_1",
    "source_job_id": "creator-job-20260725-001",
    "target_executor_id": "scene.composite.v1",
    "payload": {
      "status": "completed",
      "job_id": "creator-job-20260725-001",
      "video_name": "Example video",
      "script_text": "The completed script",
      "voiceover_paths": ["velox-asset://voiceovers/example.mp3"],
      "scenes": [
        {
          "text": "Opening scene",
          "clip_link": "velox-asset://clips/opening.mp4",
          "duration_seconds": 7
        }
      ],
      "delivery_plan": [
        {"destination_id": "drive", "priority": 1, "retry_budget": 3}
      ]
    }
  }'
```

> **Operator warning.** `source_job_id` MUST be unique per invocation
> (it is the idempotency key: `source_provider + source_job_id +
> target_executor_id` resolves to a single forwarding row + Job + Task).
> The example above uses a hard-coded `creator-job-20260725-001` for
> readability only — running the curl verbatim against a production
> master will create a real job. The canonical smoke-test payload in
> `scripts/creator_push_smoke.sh` derives `source_job_id` from the
> operator's hostname + a UUID suffix to avoid collisions; operators
> adapting this example should follow the same pattern (or use a
> UUID-only suffix).

**Source of truth for the canonical contract** (regenerate client
SDKs from here):

- OpenAPI 3.1.0 spec: `DataServer/api/openapi.yaml` — operation
  `pushCreatorJob`, schemas `CreatorPushRequest` /
  `CreatorPushPayload` / `CreatorPushAcceptedResponse` /
  `RemotePipelineResult`, security scheme `bearerAdminToken` on
  `VELOX_ADMIN_TOKEN`.
- Validator (CI-enforced): `scripts/api/validate_openapi.py`.
- Tested behavior (final authority):
  `DataServer/internal/handlers/server/pipeline/creator_push_e2e_test.go`.
