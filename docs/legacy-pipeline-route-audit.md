# Legacy pipeline route audit

Date: 2026-08-04
Branch: `main`

## Scope

This audit covers the legacy Velox inbound surfaces:

- `POST /api/script-simple`
- `POST /api/script-multiple`
- `POST /api/remote/pipeline/generate`
- `GET /api/remote/pipeline/status/:trace_id`
- `DELETE /api/remote/pipeline/cancel/:trace_id`

It distinguishes these routes from the `remoteengine.Client` outbound adapter. The latter calls a separate configured upstream service and is not a local Velox producer calling the inbound routes.

## Caller and payload matrix

| Legacy route | In-repository caller | Last request evidence | Payload observed in source | Canonical equivalent |
|---|---|---|---|---|
| `POST /api/script-simple` | No standalone PipelineGen, script, calendar, benchmark, or workstation client found. Route handler calls configured upstream `remoteengine.Client.GenerateSimpleScript`. | No persisted per-route last-request metric/table. Generic access logs exist only while the process is running. | `SimpleScriptRequest`: `topic`, optional `language`, `style`, `duration`, `variables`. | No equivalent local Velox job producer is present. A caller needing a render submits `POST /api/v1/jobs`. |
| `POST /api/script-multiple` | No standalone in-repository producer found. Route handler calls configured upstream `remoteengine.Client.GenerateBatchScripts`. | Same limitation: no persisted last-request evidence. | `BatchScriptRequest`: `topics`, optional `language`, `style`, `variables`. | `POST /api/v1/jobs/batch` for independent render jobs. |
| `POST /api/remote/pipeline/generate` | No standalone PipelineGen/workstation/script caller found. It is an inbound compatibility handler. | Handler logs topic/language/style and generated run/job IDs; no durable route-usage counter or last-payload store. | Untyped map: topic/title/source_text, language, style, scene_count, idempotency_key. | `POST /api/v1/jobs` with the canonical typed job payload. |
| `GET /api/remote/pipeline/status/:trace_id` | No in-repository caller found. | No persisted route-usage evidence. | Path `trace_id`; response was upstream `PipelineStatusResponse`. | `GET /api/v1/jobs/{job_id}`. |
| `DELETE /api/remote/pipeline/cancel/:trace_id` | No in-repository caller found. | No persisted route-usage evidence. | Path `trace_id`; remote cancellation plus local job/worker cancellation. | Use the canonical job action surface documented in `docs/API-JOBS.md`. |

## Producers already canonical

The repository's identifiable operational producers already use canonical surfaces:

- benchmark scripts: `POST /api/v1/jobs`, poll `GET /api/v1/jobs/{job_id}`;
- smoke and worker-cert scripts: `POST /api/v1/jobs`;
- creator workstation smoke: `POST /api/v1/creator/jobs`;
- frontend/API clients: `POST /api/v1/jobs` and `GET /api/v1/jobs/{id}`;
- script-with-images server ingress: `POST /api/v1/script/generate-with-images` (and its `/api/v1/script` aliases).

No tracked producer required a source edit for the five legacy route families. The migration is therefore a route-surface removal plus canonical negative/positive route tests, rather than a fabricated client rewrite.

## Removal decision

The legacy inbound routes have now been removed from the Velox router. The audit decision and mapping above preceded the removal commit; the checked-out revision retains only the canonical routes below:

```text
POST   /api/v1/jobs
POST   /api/v1/jobs/batch
POST   /api/v1/creator/jobs
POST   /api/v1/script/generate-with-images
```

The outbound `remoteengine.Client` paths are not changed by this removal because they target the separately configured remote engine, not this Velox router.

## Verification limitation

A definitive production statement such as “last request was X with payload Y” requires runtime access logs or a persisted route-usage metric/query. This repository has generic access logging but no durable per-legacy-route usage metric or payload audit table. This audit does not invent traffic-zero evidence; deployment operators must retain access logs or query their ingress before deleting a route in an environment with uncontrolled external clients.
