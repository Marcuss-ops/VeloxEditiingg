# Workers API

## Connection status model

`GET /api/v1/workers` and `GET /api/v1/workers/:worker_id` enumerate the operational worker fleet with a canonical **connection status** derived from BOTH `worker_sessions` (auth-side) AND heartbeat freshness. This replaces the legacy "is heartbeat <30s" boolean with a four-state enum that distinguishes session-level liveness from heartbeat-aware staleness.

### State derivation

The canonical states are `CONNECTED`, `STALE`, `DISCONNECTED`, `DRAINING`. They are computed by `workers.ConnectionStatus()` (in `DataServer/internal/workers/registry_query.go`) on every read so revocations on the `worker_sessions` table are reflected immediately without a registry restart.

Rules (in evaluation order):

| # | Condition | State |
|---|-----------|-------|
| 1 | `drain == true` (overrides freshness — see note below) | `DRAINING`     |

> **Note on `DRAINING` precedence.** `DRAINING` wins over freshness-based
> rules (rules 2–6). A grinder with `drain=true` whose heartbeat is hours
> old still reads `DRAINING` so the operator dashboard sees it as
> gracefully-winding-down rather than silently-disappeared. If a
> dispatcher wants to know "is this worker still active for placement",
> it MUST explicitly exclude `DRAINING` from the eligible set in addition
> to filtering out `STALE` and `DISCONNECTED`. See PR 7 / dispatcher
> contract for the canonical prev-scheduler flow.
| 2 | `last_heartbeat` empty OR unparseable               | `DISCONNECTED` |
| 3 | `session_active == false`                           | `DISCONNECTED` |
| 4 | `session_active == true` AND `now - last_heartbeat >= 5 min` | `DISCONNECTED` |
| 5 | `session_active == true` AND `now - last_heartbeat >= 150 s` | `STALE`        |
| 6 | otherwise                                                       | `CONNECTED`    |

### Boundary thresholds

- `ConnectionStaleThreshold` = **150 s** — heartbeat older than this demotes a session-active worker from `CONNECTED` to `STALE`. This is 2.5 idle heartbeat intervals and tolerates normal scheduling/network jitter.
- `ConnectionDisconnectedThreshold` = **5 min** — heartbeat older than this bumps a worker to `DISCONNECTED` regardless of session state. Matches the `CleanupStaleWorkers` window so the read model and the eviction loop agree on what "abandoned" means.
- `WorkerSessionFreshnessWindow` = **5 min** — a session is only considered `session_active` if its `last_seen` is within this window, in addition to `revoked=0` AND `expires_at > now()`. Without this gate, an idle worker whose session token expires days from now would falsely read as `CONNECTED` for days.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `status`               | string | canonical connection state (`CONNECTED` \| `STALE` \| `DISCONNECTED` \| `DRAINING`) |
| `reason`               | string | canonical reason code for non-CONNECTED states (RW-PROD-005 A2). One of `drain`, `detached_session`, `heartbeat_stale`, or `""` when CONNECTED. Omitted when empty. |
| `session_active`       | bool   | raw boolean that drove the derivation. `true` when the worker has at least one valid auth session whose `last_seen` is inside `WorkerSessionFreshnessWindow`. Always present in JSON (deliberately NOT `omitempty`) so dashboards can distinguish `false` (offline) from "field missing" (legacy client). |
| `worker_class`         | string | operator-assigned fleet class (`cpu-xlarge`, `gpu-a100`, `mixed`, `io`, ...). Omitted when empty. Filterable via `?class=` query param (RW-PROD-005 A9). |
| `rollout_group`        | string | operator-assigned rollout cohort (`v3.4`, `canary`, `holdout`, ...). Omitted when empty. Filterable via `?rollout_group=` query param (RW-PROD-005 A9). |
| `last_heartbeat_at`    | string | RFC3339 timestamp of the last heartbeat received from the worker. Empty when no heartbeat has ever been recorded. |
| `heartbeat_age_seconds`| int64  | `now - last_heartbeat` rounded down. 0 when heartbeat is missing or unparseable. |

## GET /api/v1/workers

List all registered workers with canonical status.

### Response

```json
{
  "workers": [
    {
      "worker_id": "velox-worker-01",
      "worker_name": "render-01",
      "status": "CONNECTED",
      "reason": "",
      "session_active": true,
      "hostname": "render-01",
      "worker_class": "cpu-xlarge",
      "rollout_group": "canary-2026q3",
      "protocol_version": "v3",
      "bundle_version": "v1.0.6",
      "connected_at": "2026-06-23T11:00:00Z",
      "last_heartbeat_at": "2026-06-23T12:00:00Z",
      "heartbeat_age_seconds": 0,
      "current_task_id": "",
      "active_tasks": 0,
      "task_slots": 1,
      "cpu_utilization_ratio": 0.42,
      "memory_used_bytes": 4123411200,
      "disk_free_bytes": 94371840000
    },
    {
      "worker_id": "velox-worker-02",
      "worker_name": "render-02",
      "status": "DISCONNECTED",
      "reason": "detached_session",
      "session_active": false,
      "last_heartbeat_at": "2026-06-23T11:55:01Z",
      "heartbeat_age_seconds": 299,
      "active_tasks": 0,
      "task_slots": 1
    }
  ]
}
```

The list is sorted by `worker_id` (stable order so dashboards don't flicker).
The response deliberately EXCLUDES sensitive fields: credential hash, TLS file paths, worker secret, internal readiness blob, raw IP addresses. The full operator-facing schema lives in `WorkerResponse` (`DataServer/internal/handlers/server/api/workers_handler.go`).

## GET /api/v1/workers/:worker_id

Same shape as a single element of the list. Returns `404` if the worker is not registered.

```json
{
  "worker_id": "velox-worker-02",
  "status": "STALE",
  "reason": "heartbeat_stale",
  "session_active": true,
  "worker_class": "gpu-a100",
  "rollout_group": "holdout-2026q3",
  "last_heartbeat_at": "2026-06-23T12:00:42Z",
  "heartbeat_age_seconds": 18,
  "active_tasks": 1,
  "task_slots": 1
}
```

## API-surface namespaces (Phase 6)

The worker control-plane API is split into three canonical namespaces.
Remaining legacy paths are counted by
`velox_master_http_route_requests_total{surface="legacy"}` — a legacy
surface with sustained usage must stay; a quiet one can be retired.

| Namespace | Auth | Surface | Routes |
| --- | --- | --- | --- |
| `/api/v1/agent/*` | worker credential / session token | `agent` | `POST /register`, `GET /assets/:asset_id`, `GET /cache/protected-assets` |
| `/api/v1/admin/workers/*` | `VELOX_ADMIN_TOKEN` | `admin` | list, inspect, mutations (`drain`/`resume`/`quarantine`/`update`), control (`revoke`/`unrevoke`/`restart`), `revoked`, `health`, `smoke`, per-worker `metrics`/`alerts` |
| `/api/v1/fleet/*` | `VELOX_ADMIN_TOKEN` | `fleet` | `GET /metrics`, `GET /alerts/active`, `GET /alerts/recent` |

Removed legacy paths (zero sustained usage): `/api/v1/workers/register`,
`/api/v1/worker-assets/:asset_id`, `/api/v1/workers/cache/protected-assets`,
`/api/v1/admin/workers/metrics` (fleet-wide snapshot — now
`/api/v1/fleet/metrics`), `/api/v1/admin/alerts/*` (now
`/api/v1/fleet/alerts/*`) and the `/worker/*` admin group (control actions
migrated under `/api/v1/admin/workers/:worker_id/*`).

The `/api/v1/workers` diagnostic surface (`GET` list/get + per-worker
`/metrics`/`/sessions`/`/events`) remains mounted (surface `legacy`) while
`scripts/cert/master_state.sh` and the operator runbook still consume it.

## Canonical worker operations

Worker updates are submitted through the authenticated fleet API and tracked
through the persisted operation ledger:

### POST /api/v1/admin/workers/:worker_id/update

Requires an immutable GHCR image digest in `target_digest` and returns an
`operation_id` for asynchronous tracking.

### GET /api/v1/admin/operations/:operation_id

Poll the operation until it reaches a terminal state.

Worker registration, command delivery, heartbeats, and acknowledgements use
the current worker control protocol. The former bundle/update routes are
retired and are deliberately not documented as active API surfaces; see
`docs/api/bundle.md` for the complete 404 contract.
