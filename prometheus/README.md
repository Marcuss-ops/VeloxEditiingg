# Prometheus — scrape config + label discipline for the Velox master

The Velox master exposes `/metrics` (text/plain; version=0.0.4)
on the configured `MetricsPort`. This directory documents:

1. The canonical scrape configuration.
2. The label discipline that pipelines MUST respect.
3. The metric family shapes that Prometheus rooms should expect.

## Scrape config

The Velox master exposes Prometheus text-format metrics at:

  http://<master-host>:<metrics-port>/metrics

The exact port is configured via `VELOX_METRICS_PORT` (default
disabled; set to a non-zero port to enable). The cert trust chain
mirrors the gRPC TLS triple when `VELOX_GRPC_TLS_*` is set.

A canonical scrape stanza:

```yaml
- job_name: velox-master
  metrics_path: /metrics
  scrape_interval: 15s
  static_configs:
    - targets: ['master-1.internal:9100', 'master-2.internal:9100']
```

## Label discipline (load-bearing)

The metric surface enforces a SAFE-LABEL allowlist at registration
time (see `DataServer/internal/metrics/metrics.go::unsafeLabelKeys`):

  SAFE: executor_id, executor_version, worker_class, phase,
        codec, preset, resolution_bucket, cache_source, worker_id

  UNSAFE: job_id, task_id, attempt_id, artifact_id, sha256,
          hash, video_title, channel_id, project_id

Any rule in `alerts/` or panel query in `dashboards/` referencing
UNSAFE labels is a regression at the cardinality discipline — flag
it as part of the same PR review.

## Compute outcome families (spec §14)

The 4 legacy split-out families have been COLLAPSED into a single
counter family classified by `outcome=`:

  `velox_compute_seconds_total{outcome=useful|failed|cancelled|stale|speculative_lost}`

A sibling counter family tracks failure-reason attribution:

  `velox_compute_failure_reasons_total{reason=...}`

(`reason` is a CLOSED enum: the worker emits `worker.Code*`
constants; free-form reason strings MUST NOT land in dashboards.)

## Migration window

The 4 retired split-out family names
(`velox_compute_seconds_total_{failed,cancelled,stale,useful}`)
are FORBIDDEN in any new dashboard, alert, or rule. Any matches
fail `scripts/ci/check-compute-outcome-labels.sh` at PR time.
