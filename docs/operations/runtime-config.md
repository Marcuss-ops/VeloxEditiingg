# Runtime configuration

The server owns process configuration in `DataServer/internal/config`.
Application packages do not read `os.Getenv` directly.

At startup the composition root performs:

```text
load env/.env → parse → validate → freeze → redacted snapshot
```

The snapshot includes:

- `schema_version` (`velox.runtime.v1`)
- a deterministic SHA-256 `fingerprint`
- per-field `source` (`default`, `env`, or `file`)
- effective non-secret values

Secret values are never emitted. The snapshot reports only `[REDACTED]` or
`<unset>` for secret fields.

## Operational values

Periodic runtime values use Go duration syntax:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `VELOX_TASKGRAPH_TICK` | `2s` | Readiness/task graph tick |
| `VELOX_ARTIFACT_RECONCILE_INTERVAL` | `15m` | Artifact reconciler |
| `VELOX_METRICS_TICK` | `15s` | Attempt metrics supervisor |
| `VELOX_METRICS_SNAPSHOT_INTERVAL` | `5m` | Fleet metrics snapshot |
| `VELOX_RESTARTABLE_MAX_RETRIES` | `5` | Restartable runner retry budget |
| `VELOX_CACHE_LOOKAHEAD_JOBS` | `10` | Protected asset lookahead |
| `VELOX_CACHE_SNAPSHOT_INTERVAL` | `30s` | Protected asset refresh |
| `VELOX_ALERT_EVALUATION_INTERVAL` | `30s` | Alert rule evaluation |
| `VELOX_ALERT_COOLDOWN` | `5m` | Alert deduplication cooldown |

Malformed values are rejected during validation rather than silently replaced
with defaults. This prevents a typo such as `VELOX_TASKGRAPH_TICK=2` from
changing production behavior unexpectedly.

## Compatibility

`Config.FromEnv` remains available for package tests and compatibility. Server
bootstrap uses `config.LoadFromEnv`, which validates and freezes the resulting
configuration before constructing long-lived services.
