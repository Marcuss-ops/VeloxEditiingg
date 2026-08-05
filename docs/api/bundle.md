# Bundle API

## Retired bundle/update routes

The legacy bundle and worker-update HTTP surfaces have been removed from the
Velox router. Every route below is intentionally unmounted and must return
`HTTP 404` (with or without authentication):

```text
POST /bundle/manifest/generate
GET  /api/worker/v2/manifest
GET  /api/worker/v2/chunk/:chunkName
POST /install_worker/force_regenerate_zip
POST /workers/full_update_linux
POST /workers/update_all_latest_bundle
POST /worker/request_update
```

The handler implementations may remain in the repository for internal
migration tooling and focused unit tests, but they are not public HTTP
surfaces. Do not re-register them in `internal/app.WorkersModule`.

## Canonical replacements

Use the canonical fleet API for worker updates:

```text
POST /api/v1/admin/workers/:worker_id/update
GET  /api/v1/admin/operations/:operation_id
```

The update request requires an immutable GHCR image digest, for example:

```json
{
  "target_digest": "ghcr.io/marcuss-ops/velox-worker@sha256:<64-hex-digits>",
  "reason": "operator-requested worker update"
}
```

Use the worker control protocol for worker registration, command delivery,
heartbeats, and acknowledgements. Bundle rebuilds are internal deployment
operations and are no longer exposed as legacy HTTP endpoints.

## Supported bundle artifacts

Bundle files remain implementation artifacts used by the canonical worker
runtime and deployment tooling. Their on-disk manifest (`manifest_v2.json`)
is not itself a public route. Any new download or inspection surface must be
introduced under an explicitly versioned canonical API and covered by a route
contract test before registration.
