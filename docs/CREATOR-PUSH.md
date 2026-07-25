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

## Deprecation timeline: `/api/remote/pipeline` → `/api/v1/creator/jobs`

`POST /api/remote/pipeline` (the legacy remote-engine sync-forward
endpoint) is **deprecated** as of `main` HEAD `788a119`. The canonical
creator-push intake is `POST /api/v1/creator/jobs` (this document).
Both paths continue to work; new integrators MUST use the canonical
path.

### Behavior parity (drift-proof)

Both paths share a single typed-DTO normalization step:
`normalizeRemoteEngineIntake` in
`DataServer/internal/handlers/server/pipeline/forwarding.go`. Any
change to the typed DTO conversion (`remoteengine.ParseRemotePipelineResult`
→ `dto.ToWorkerPayload`) or identity derivation
(`source_provider` / `source_job_id` / `target_executor_id` with the
documented fallback chain) runs through this one helper. The two
intake paths cannot diverge — drift is mathematically impossible.

### Migration steps

1. Update your remote-engine worker (or caller) to POST against
   `/api/v1/creator/jobs` instead of `/api/remote/pipeline`.
2. Use the canonical `creatorPushRequest` envelope (see **Request**
   above). The `source_provider` field replaces the implicit
   `"remote_engine"` identity the legacy route hardcoded — this lets
   you multiplex multiple creator machines through one master without
   collisions on the resolver identity key
   (`source_provider + source_job_id + target_executor_id`).
3. Keep `VELOX_ADMIN_TOKEN` as the auth bearer; no secret rotation
   required.

### Observability

Every legacy call stamps (post-CAS — only after the atomic Resolver
commit, mirroring the creator_push observation point):

- Counter
  `pipeline.creator_intake_accepted_total{path="remote_engine_legacy"}`
- Structured log line
  `DEPRECATED_REMOTE_ENGINE_INTAKE path=remote_engine_legacy
   source_provider=... source_job_id=... target_executor_id=...
   job_id=... — use POST /api/v1/creator/jobs`

Operators monitoring the migration should expect the
`remote_engine_legacy` series to trend to **zero** within one release
cycle after all remote-engine workers update. Sustained non-zero
traffic at the v2.0.0 release boundary will block the deletion of
`forwardPipelineResultToWorker` and trigger a follow-up migration
push.

### Sunset

`forwardPipelineResultToWorker` (and the `/api/remote/pipeline` HTTP
route it backs) are scheduled for removal in **v2.0.0**. The removal
commit will NOT land on `main` until the
`pipeline.creator_intake_accepted_total{path="remote_engine_legacy"}`
counter has been observed at **zero** for at least one full release
cycle (i.e. one calendar quarter of production traffic at the
previous release). This data-driven sunset guard prevents a silent
regression in any operator fleet still running the legacy endpoint.

### Cross-references

- Architectural invariant (single Resolver writer):
  `docs/architecture/current-architecture.md §12 Due percorsi di intake, un solo writer`
- Telemetry catalog:
  `DataServer/internal/metrics/catalog_pipeline.go` (entry
  `pipeline.creator_intake_accepted_total`, label set `{path}` with
  valid values `creator_push` / `creator_forwarder` / `remote_engine_legacy`)
- Drift-proof normalizer (the single source of truth for both paths):
  `DataServer/internal/handlers/server/pipeline/creator_push.go::normalizeRemoteEngineIntake`
