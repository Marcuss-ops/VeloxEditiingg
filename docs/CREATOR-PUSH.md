# Creator-initiated job push

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
