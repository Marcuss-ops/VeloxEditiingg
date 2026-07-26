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
