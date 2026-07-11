# Velox Enterprise Metric Catalog

> **Owner:** video-engine  
> **Last updated:** 2026-07-06  
> **Version:** Scorecard v2 (Steps 1–18)  
> **Cardinality discipline:** NEVER put `job_id`, `task_id`, `attempt_id`, `hash`, or `video_title` in a Prometheus label. Use SQL for those dimensions.

## Legend

| Column     | Meaning                                                      |
| ---------- | ------------------------------------------------------------ |
| **Type**   | `C` = Counter, `G` = Gauge, `H` = Histogram                 |
| **Ret.**   | Default Prometheus retention (TSDB). `90d` unless noted.     |
| **HC**     | High-cardinality? `Y` = unsafe for Prometheus, SQL only.     |
| **Sleep**  | `poll` = supervisor tick, `push` = worker report, `hb` = heartbeat |

---

## A. Job-Level Metrics

| #  | Metric Name                                | Type | Description                                           | Unit      | Labels                          | DB Source                                 | Ret. | HC |
| -- | ------------------------------------------ | ---- | ----------------------------------------------------- | --------- | ------------------------------- | ----------------------------------------- | ---- | -- |
| 1  | `velox_project_render_speed_ratio`         | G    | Media duration / wall clock (>1 = faster than realtime) | ratio   | `executor_id`, `worker_class`  | `task_attempt_metrics.media_duration_seconds / wall_clock_seconds` | 90d  | N  |
| 2  | `velox_compute_seconds_total`              | C    | CPU seconds classified by terminal outcome           | seconds   | `outcome`                       | `task_attempt_metrics.cpu_time_ms / 1000` | 90d  | N  |
| 3  | `velox_compute_failure_reasons_total`      | C    | Failed attempts by reason code                       | count     | `reason`                        | `task_attempts.error_code`                | 90d  | N  |
| 4  | `velox_error_classification_total`         | C    | Errors by canonical code × component × phase         | count     | `error_code`, `component`, `phase` | `task_attempt_metrics.error_component, error_phase, error_retryable` (populated by supervisor tick) | 90d | N |

---

## B. Attempt-Level Timing & Phase Metrics

| #  | Metric Name                                | Type | Description                                           | Unit      | Labels                              | DB Source                                           | Ret. | HC |
| -- | ------------------------------------------ | ---- | ----------------------------------------------------- | --------- | ----------------------------------- | --------------------------------------------------- | ---- | -- |
| 5  | `velox_task_phase_duration_seconds`        | H    | Per-phase duration for canonical rendering phases     | seconds   | `executor_id`, `executor_version`, `worker_class`, `phase`, `status` | `task_phase_timings` (legacy) / `task_phase_timings.duration_ms/1000` (detailed) | 90d | N |
| 6  | `velox_engine_phase_duration_seconds`      | H    | C++ engine + Go pipeline per-phase duration           | seconds   | `executor_id`, `worker_id`, `phase`, `status` | `task_phase_timings` detailed rows, fallback `task_attempt_metrics.pipeline_*_ms, engine_*_ms` | 90d | N |
| 7  | `velox_engine_segment_duration_seconds`    | H    | Per-segment encode/download duration (sidecar)        | seconds   | `executor_id`, `worker_id`, `source_type`, `status` | `task_attempt_segment_timings`                     | 90d | N |

### Engine Phase Labels (as emitted by `RecordEngineAggregate`)

| Phase                    | Source Column                        |
| ------------------------ | ------------------------------------ |
| `pipeline.resolve`       | `pipeline_resolve_ms`                |
| `pipeline.validate`      | `pipeline_validate_ms`               |
| `pipeline.compile`       | `pipeline_compile_ms`                |
| `pipeline.render`        | `pipeline_render_ms`                 |
| `pipeline.total`         | `pipeline_total_ms`                  |
| `native.total`           | `native_total_ms`                    |
| `native.process_wait`    | `native_process_wait_ms`             |
| `engine.asset_download`  | `engine_asset_download_ms`           |
| `engine.segment_build`   | `engine_segment_build_ms`            |
| `engine.concat`          | `engine_concat_ms`                   |
| `engine.audio_download`  | `engine_audio_download_ms`           |
| `engine.mux_audio`       | `engine_mux_audio_ms`                |
| `engine.copy_final`      | `engine_copy_final_ms`               |

---

## C. FFmpeg Metrics

| #  | Metric Name                                | Type | Description                                           | Unit      | Labels                    | DB Source                                             | Ret. | HC |
| -- | ------------------------------------------ | ---- | ----------------------------------------------------- | --------- | ------------------------- | ----------------------------------------------------- | ---- | -- |
| 8  | `velox_ffmpeg_frames_processed_total`      | C    | Total frames processed (from `-progress`)             | frames    | `executor_id`             | `task_attempt_metrics.frames_decoded + frames_encoded` | 90d  | N  |
| 9  | `velox_ffmpeg_fps`                         | G    | Last-observed FFmpeg fps                              | fps       | `executor_id`             | Worker heartbeat (in-band)                            | 90d  | N  |
| 10 | `velox_ffmpeg_speed_ratio`                 | G    | Last-observed FFmpeg speed vs realtime                | ratio     | `executor_id`             | `task_attempt_metrics.ffmpeg_speed_ratio`             | 90d  | N  |
| 11 | `velox_ffmpeg_encode_duration_seconds`     | H    | FFmpeg encode duration distribution                   | seconds   | `executor_id`             | `task_attempt_segment_timings.ffmpeg_encode_ms`       | 90d  | N  |
| 12 | `velox_ffmpeg_decode_duration_seconds`     | H    | FFmpeg decode duration distribution                   | seconds   | `executor_id`             | Worker heartbeat (in-band)                            | 90d  | N  |
| 13 | `velox_ffmpeg_dropped_frames_total`        | C    | Dropped frames observed                               | frames    | `executor_id`             | Worker heartbeat (in-band)                            | 90d  | N  |
| 14 | `velox_ffmpeg_duplicated_frames_total`     | C    | Duplicated frames observed                            | frames    | `executor_id`             | Worker heartbeat (in-band)                            | 90d  | N  |
| 15 | `velox_ffmpeg_exit_total`                  | C    | FFmpeg process exits by exit code                     | count     | `executor_id`, `exit_code` | Worker heartbeat (in-band)                            | 90d  | N  |
| 16 | `velox_ffmpeg_restarts_total`              | C    | FFmpeg process restarts                               | count     | `executor_id`             | Worker heartbeat (in-band)                            | 90d  | N  |
| 17 | `velox_ffmpeg_processes_active`            | G    | Currently-running FFmpeg processes per worker         | count     | `executor_id`             | Worker heartbeat (in-band)                            | 90d  | N  |

---

## D. Video Encode Amplification

| #  | Metric Name                                | Type | Description                                           | Unit      | Labels         | DB Source                                             | Ret. | HC |
| -- | ------------------------------------------ | ---- | ----------------------------------------------------- | --------- | -------------- | ----------------------------------------------------- | ---- | -- |
| 18 | `velox_video_encode_passes_total`          | C    | Total encode passes performed                         | count     | `executor_id`  | `task_attempt_metrics.encode_passes`                  | 90d  | N  |
| 19 | `velox_video_frames_encoded_total`         | C    | Frames encoded (sum across passes)                    | frames    | `executor_id`  | `task_attempt_metrics.frames_encoded`                 | 90d  | N  |
| 20 | `velox_video_output_frames_total`          | C    | Output frames published (dedup lower-bound)            | frames    | `executor_id`  | `task_attempt_metrics.frames_encoded` (proxy)         | 90d  | N  |
| 21 | `velox_video_stream_copy_operations_total` | C    | Stream-copy concat operations (cheap path)             | count     | _(none)_       | `task_attempt_metrics.final_concat_stream_copy`       | 90d  | N  |
| 22 | `velox_video_reencode_operations_total`    | C    | Re-encode concat operations (expensive path)           | count     | `reason`       | `task_attempt_metrics.concat_mode = 'reencode'`       | 90d  | N  |

---

## E. Cache Metrics

| #  | Metric Name                                | Type | Description                                           | Unit      | Labels        | DB Source                                                     | Ret. | HC |
| -- | ------------------------------------------ | ---- | ----------------------------------------------------- | --------- | ------------- | ------------------------------------------------------------- | ---- | -- |
| 23 | `velox_cache_requests_total`               | C    | Cache request count by result (hit\|miss\|corrupt)     | count     | `result`      | `task_attempt_cache_stats.cache_hits, cache_misses, cache_corruptions` | 90d | N |
| 24 | `velox_cache_bytes_total`                  | C    | Cache bytes transferred by result (hit\|miss)         | bytes     | `result`      | `task_attempt_metrics.bytes_from_local_cache` (→ hit), `bytes_from_drive + bytes_from_blobstore` (→ miss) | 90d | N |
| 25 | `velox_cache_entries`                      | G    | Current cache entry count per worker                  | count     | `worker_id`   | Worker heartbeat (in-band)                                    | 90d  | N  |
| 26 | `velox_cache_size_bytes`                   | G    | Current cache size per worker                         | bytes     | `worker_id`   | Worker heartbeat (in-band)                                    | 90d  | N  |
| 27 | `velox_cache_evictions_total`              | C    | Cache eviction events per worker                      | count     | `worker_id`   | Worker heartbeat (in-band, delta tracking)                    | 90d  | N  |
| 28 | `velox_cache_evicted_bytes_total`          | C    | Bytes evicted per worker (reserved)                   | bytes     | `worker_id`   | Reserved for future heartbeat extension                       | 90d  | N  |
| 29 | `velox_cache_corruption_total`             | C    | Cache corruption events per worker                    | count     | `worker_id`   | Worker heartbeat (in-band, delta tracking)                    | 90d  | N  |

---

## F. Worker Resource Metrics (from Heartbeat)

| #  | Metric Name                                   | Type | Description                                     | Unit      | Labels      | DB Source                        | Ret. | HC |
| -- | --------------------------------------------- | ---- | ----------------------------------------------- | --------- | ----------- | -------------------------------- | ---- | -- |
| 30 | `velox_worker_cpu_utilization_ratio`           | G    | Worker CPU utilization (0–1)                     | ratio     | `worker_id` | Heartbeat `resources.cpu_percent` | 90d  | N  |
| 31 | `velox_worker_cpu_iowait_ratio`                | G    | Worker iowait ratio (0–1)                        | ratio     | `worker_id` | Heartbeat `resources.iowait`      | 90d  | N  |
| 32 | `velox_worker_cpu_steal_ratio`                 | G    | Worker CPU steal ratio (0–1)                     | ratio     | `worker_id` | Heartbeat `resources.steal`       | 90d  | N  |
| 33 | `velox_worker_process_rss_bytes`               | G    | Worker process RSS                               | bytes     | `worker_id` | Heartbeat `resources.rss`         | 90d  | N  |
| 34 | `velox_worker_process_rss_peak_bytes`          | G    | Worker peak RSS (since start)                    | bytes     | `worker_id` | Heartbeat `resources.rss_peak`    | 90d  | N  |
| 35 | `velox_worker_memory_used_bytes`               | G    | Worker system memory used                        | bytes     | `worker_id` | Heartbeat `resources.memory_used` | 90d  | N  |
| 36 | `velox_worker_disk_free_bytes`                 | G    | Worker disk free bytes                           | bytes     | `worker_id` | Heartbeat `resources.disk_free`   | 90d  | N  |
| 37 | `velox_worker_temp_bytes`                      | G    | Worker temp bytes snapshot                       | bytes     | `worker_id` | Heartbeat `resources.temp_bytes`  | 90d  | N  |
| 38 | `velox_worker_active_tasks`                    | G    | Active tasks on this worker                      | count     | `worker_id` | Heartbeat `resources.active_tasks` | 90d | N |
| 39 | `velox_worker_task_slots`                      | G    | Total task slots on this worker                  | count     | `worker_id` | Heartbeat `resources.task_slots`  | 90d  | N  |
| 40 | `velox_worker_load1`                           | G    | Worker 1-min loadavg (×1000 encoded)             | load      | `worker_id` | Heartbeat `resources.load1`       | 90d  | N  |
| 41 | `velox_worker_run_queue`                       | G    | Worker OS run queue depth                        | count     | `worker_id` | Heartbeat `resources.run_queue`   | 90d  | N  |
| 42 | `velox_worker_network_receive_bytes_total`     | C    | Worker network receive bytes (cumulative delta)   | bytes     | `worker_id` | Heartbeat `resources.net_rx`      | 90d  | N  |
| 43 | `velox_worker_network_transmit_bytes_total`    | C    | Worker network transmit bytes (cumulative delta)  | bytes     | `worker_id` | Heartbeat `resources.net_tx`      | 90d  | N  |

---

## G. Master Health Metrics

| #  | Metric Name                                   | Type | Description                                     | Unit      | Labels      | DB Source                        | Ret. | HC |
| -- | --------------------------------------------- | ---- | ----------------------------------------------- | --------- | ----------- | -------------------------------- | ---- | -- |
| 44 | `velox_master_memory_rss_bytes`               | G    | Master process RSS (/proc/self/status)           | bytes     | _(none)_    | `/proc/self/status VmRSS`        | 90d  | N  |
| 45 | `velox_master_goroutines`                     | G    | Active goroutines on master                      | count     | _(none)_    | `runtime.NumGoroutine()`         | 90d  | N  |
| 46 | `velox_master_outbox_pending`                 | G    | Pending outbox events (not yet dispatched)       | count     | _(none)_    | `orchestrator_outbox` COUNT      | 90d  | N  |
| 47 | `velox_master_worker_heartbeat_age_seconds`   | G    | Seconds since last heartbeat per worker          | seconds   | `worker_id` | `collector.lastSeen` map diff    | 90d  | N  |

---

## H. Cost Metrics

| #  | Metric Name                                        | Type | Description                                           | Unit          | Labels         | DB Source                                                     | Ret. | HC |
| -- | -------------------------------------------------- | ---- | ----------------------------------------------------- | ------------- | -------------- | ------------------------------------------------------------- | ---- | -- |
| 48 | `velox_cost_cpu_core_seconds_per_output_minute`     | G    | CPU cost per output minute (€ × 1e6)                 | micro-EUR     | `worker_class` | `task_attempt_cost_basis.*` aggregated per tick              | 90d  | N  |
| 49 | `velox_cost_network_gb_per_output_minute`           | G    | Network egress cost per output minute (€ × 1e6)       | micro-EUR     | `worker_class` | `task_attempt_cost_basis.*` aggregated per tick              | 90d  | N  |
| 50 | `velox_cost_storage_gb_written_per_output_minute`   | G    | Storage cost per output minute (€ × 1e6)              | micro-EUR     | `worker_class` | `task_attempt_cost_basis.*` aggregated per tick              | 90d  | N  |
| 51 | `velox_cost_total_per_output_minute`                | G    | Total cost per output minute (€ × 1e6)                | micro-EUR     | `worker_class` | `task_attempt_cost_basis.*` aggregated per tick              | 90d  | N  |
| 52 | `velox_waste_total`                          | C    | Waste/cost totals by type (retry_count\|wasted_cpu_ms\|wasted_download_bytes\|wasted_cost_estimate) | mixed | `waste_type` | `task_attempt_metrics.retry_count, wasted_cpu_ms, wasted_download_bytes, wasted_cost_estimate` | 90d | N |

---

## I. Completion & Reconciliation

| #  | Metric Name                                   | Type | Description                                           | Unit  | Labels          | DB Source                                                     | Ret. | HC |
| -- | --------------------------------------------- | ---- | ----------------------------------------------------- | ----- | --------------- | ------------------------------------------------------------- | ---- | -- |
| 53 | `velox_completion_reconcile_total`            | C    | Reconcile supervisor dispatch count by case × action  | count | `case`, `action` | `task_attempts` terminal-state scan + commit deadline check  | 90d  | N  |
| 54 | `velox_commit_deadline_exceeded_total`        | C    | Attempts whose commit_deadline_at crossed              | count | _(none)_        | `task_attempts.commit_deadline_at < now`                     | 90d  | N  |

---

## J. Placement & Scheduling

| #  | Metric Name                                   | Type | Description                                           | Unit  | Labels    | DB Source                              | Ret. | HC |
| -- | --------------------------------------------- | ---- | ----------------------------------------------------- | ----- | --------- | -------------------------------------- | ---- | -- |
| 55 | `velox_placement_rejections_total`            | C    | Placement matcher rejections by reason code            | count | `reason`  | `tasks` / placement pipeline          | 90d  | N  |

### Placement Rejection Codes

`capacity_full`, `unsupported_executor`, `missing_capability`, `executor_mismatch`, `version_mismatch`, `lease_expired`

---

## K. ConflictBudget (CAS Contention)

| #  | Metric Name                                      | Type | Description                                           | Unit  | Labels | DB Source                                        | Ret. | HC |
| -- | ------------------------------------------------ | ---- | ----------------------------------------------------- | ----- | ------ | ------------------------------------------------ | ---- | -- |
| 56 | `velox_conflict_streak_reset_total`               | C    | ConflictBudget streak resets (Record(nil) on non-zero)  | count | _(none)_ | `attempt_commits` CAS path                      | 90d  | N  |
| 57 | `velox_conflict_escalations_total`                | C    | ConflictBudget escalations to ErrConflictBudgetExhausted | count | _(none)_ | `attempt_commits` CAS path                      | 90d  | N  |
| 58 | `velox_conflict_stayed_under_threshold_total`     | C    | Conflict observations that incremented streak under threshold | count | _(none)_ | `attempt_commits` CAS path                      | 90d  | N  |
| 59 | `velox_conflict_streak_length`                    | H    | Distribution of consecutive-conflict streak lengths    | count  | _(none)_ | `attempt_commits` CAS path (buckets: 1,2,3,5,10) | 90d  | N  |

---

## L. DB-Only Metrics (SQL Analytics — NOT in Prometheus)

These dimensions exist in SQLite / Postgres (`task_attempt_metrics`, `task_attempts`, `task_attempt_cache_stats`, `task_attempt_cost_basis`) but are **not** exposed as Prometheus labels because they carry high-cardinality values (`job_id`, `attempt_id`, `worker_id` in path context, free-form strings). Operators query them directly via SQL.

| #  | Column(s)                                      | Table                    | Description                                          | Unit       | Migration |
| -- | ---------------------------------------------- | ------------------------ | ---------------------------------------------------- | ---------- | --------- |
| L1 | `input_bytes`, `output_bytes`                  | `task_attempt_metrics`   | I/O byte counters per attempt                         | bytes      | 043       |
| L2 | `gpu_time_ms`, `peak_vram_bytes`               | `task_attempt_metrics`   | GPU counters (reserved)                               | ms / bytes | 054       |
| L3 | `temp_bytes_written`, `duplicate_download_bytes` | `task_attempt_metrics` | Storage amplification + duplicate waste               | bytes      | 054       |
| L4 | `media_duration_seconds`, `wall_clock_seconds`   | `task_attempt_metrics` | Per-attempt render speed base                         | seconds    | 054       |
| L5 | `git_sha`, `worker_version`, `engine_version`, `ffmpeg_version`, `config_hash`, `docker_image_digest` | `task_attempts` | Versioning context per attempt | _(string)_  | 071 |
| L6 | `ffprobe_valid`, `duration_diff_sec`, `has_video_stream`, `has_audio_stream`, `output_file_size`, `black_frame_ratio`, `audio_sync_offset_ms` | `task_attempt_metrics` | Output quality validation | mixed | 072 |
| L7 | `cpu_percent_peak`, `rss_peak_bytes`, `disk_read_bytes`, `disk_write_bytes`, `network_rx_bytes`, `network_tx_bytes`, `iowait_ms`, `open_fds_peak` | `task_attempt_metrics` | Per-attempt resource snapshot | mixed | 073 |
| L8 | `queue_ms`, `lease_wait_ms`, `time_to_first_worker_ms`, `pending_tasks_at_start`, `active_workers_at_start` | `task_attempt_metrics` | Queue / wait-time metrics | ms / count | 074 |
| L9 | `scene_count`, `segment_count`, `total_input_duration_sec`, `resolution_width`, `resolution_height`, `fps`, `audio_track_count`, `subtitle_count`, `template_id` | `task_attempt_metrics` | Input context for normalization | mixed | 075 |
| L10 | `error_component`, `error_phase`, `error_retryable`, `error_message_hash` | `task_attempt_metrics` | Structured error metadata | mixed | 076 |
| L11 | `asset_cache_hit_count`, `asset_cache_miss_count`, `blob_cache_hit_count`, `blob_cache_miss_count`, `render_cache_hit_count` | `task_attempt_metrics` | Granular per-tier cache hit/miss counters | count | 077 |
| L12 | `retry_count`, `wasted_cpu_ms`, `wasted_download_bytes`, `wasted_cost_estimate` | `task_attempt_metrics` | Waste/cost attribution per attempt | mixed | 078 |
| L13 | `pipeline_resolve_ms` … `engine_copy_final_ms`  | `task_attempt_metrics`   | Engine-aggregate phase columns (13 total)              | ms         | 070       |
| L14 | `cache_hits`, `cache_misses`, `cache_evictions`, `cache_corruptions`, `cache_bytes_used`, `cache_entries` | `task_attempt_cache_stats` | Per-attempt cache snapshot | mixed | 054 |
| L15 | `cpu_price_per_second`, `storage_price_per_gb`, `network_price_per_gb`, `cpu_time_seconds_total`, `storage_gb_written`, `network_gb_egressed`, `output_minutes_total` | `task_attempt_cost_basis` | Cost envelope per attempt | mixed | 054 |

---

## M. Cardinality Quick Reference

### Safe for Prometheus Labels (closed enums, < 1000 series each)

| Label              | Values                                              | Approx. Cardinality |
| ------------------ | --------------------------------------------------- | ------------------- |
| `executor_id`      | `pipeline`, `scene.composite`, `transcode`, …        | ~10                 |
| `executor_version` | `1`, `2`, …                                         | ~10                 |
| `worker_class`     | `cpu`, `gpu`, `mixed`, `io`, `default`, `all`       | 6                   |
| `worker_id`        | _(per-deployment, capped by fleet size)_            | ~50–200             |
| `phase`            | `cache_lookup`, `download`, `compile`, `render`, …  | ~14                 |
| `status`           | `ok`, `failed`, `skipped`, `timeout`                | ~4                  |
| `outcome`          | `useful`, `failed`, `cancelled`, `stale`, `speculative_lost` | 5          |
| `reason`           | Canonical error codes (closed enum)                 | ~25                 |
| `error_code`       | Same as `reason`                                    | ~25                 |
| `component`        | `asset_download`, `ffmpeg`, `pipeline`, `upload`, … | 7                   |
| `exit_code`        | Numeric FFmpeg exit codes (0–255)                   | ~256                |
| `source_type`      | `clip`, `image`, `audio`, `color`, `unknown`        | 5                   |
| `result`           | `hit`, `miss`, `corrupt`                            | 3                   |
| `case`             | Reconcile anomaly cases (closed enum)               | 11                  |
| `action`           | `noop`, `transition`, `escalate`                    | 3                   |

### NEVER in Prometheus (use SQL)

- `job_id`, `task_id`, `attempt_id`, `artifact_id`
- `hash`, `sha256`, `video_title`, `error_message`
- `template_id`, `pipeline_id`, `user_id`, `api_key_id`

---

## N. Sleep Schedule

| Source                 | Trigger              | Interval | Families Updated                                 |
| ---------------------- | -------------------- | -------- | ------------------------------------------------ |
| Worker heartbeat       | gRPC push            | ~15s     | `worker_*`, `ffmpeg_*` (in-band), `cache_*`      |
| Worker TaskResult      | gRPC push            | per-job  | `compute_*`, `render_speed`, `video_*`, `cache_*` |
| Supervisor tick        | Poll (DB scan)       | 15s      | `cost_*`, `error_classification`, `heartbeat_age`, `master_*`, `engine_*` |
| Reconcile supervisor   | Poll + CAS dispatch  | per-tick | `reconcile_total`, `commit_deadline_exceeded`     |
| Placement pipeline     | gRPC claim path      | per-claim| `placement_rejections`                           |
| ConflictBudget CAS     | CAS collision path   | per-call | `conflict_streak_*`, `conflict_escalations`      |

---

## O. Owner & Alerting Policy

| Owner             | Responsibility                                           | Alert Channel        |
| ----------------- | -------------------------------------------------------- | -------------------- |
| **video-engine**  | All `velox_*` metric families (this catalog)             | `#velox-alerts`      |
| **infra**         | `velox_master_*`, `velox_worker_*` (host-level)          | `#velox-alerts`      |
| **sre**           | Cost gauges (`velox_cost_*`), heartbeat age, outbox      | `#velox-alerts`      |

### Pre-configured Alert Thresholds (PrometheusRules)

| Alert                          | Condition                                         | Severity |
| ------------------------------ | ------------------------------------------------- | -------- |
| `VeloxWorkerHeartbeatStale`    | `velox_master_worker_heartbeat_age_seconds > 120` | warning  |
| `VeloxWorkerHeartbeatLost`     | `velox_master_worker_heartbeat_age_seconds > 300` | critical |
| `VeloxHighFailureRate`         | `rate(velox_compute_failure_reasons_total[5m]) > 0.1` | warning |
| `VeloxConflictBudgetExhausted` | `rate(velox_conflict_escalations_total[5m]) > 0`  | critical |
| `VeloxQueueDepthGrowing`       | `avg(pending_tasks_at_start) increasing 30m`      | warning  |
| `VeloxDiskNearFull`            | `velox_worker_disk_free_bytes < 10GB`             | critical |
| `VeloxOOMDetected`             | `velox_error_classification_total{error_code="OUT_OF_MEMORY"} > 0` | critical |
| `VeloxCostAnomaly`             | `velox_cost_total_per_output_minute > baseline × 3` | warning  |

---

## P. Distributed Tracing (OpenTelemetry)

| Setting                       | Description                                           | Default     |
| ----------------------------- | ----------------------------------------------------- | ----------- |
| `VELOX_OTEL_EXPORTER`         | Tracer backend: `""` (no-op), `"stdout"`, or `"otlp"` | `""`       |
| `VELOX_OTEL_ENDPOINT`         | OTLP gRPC collector address (`host:port`)              | _(required when `EXPORTER=otlp`)_ |
| `VELOX_VERSION`               | Service version tag on all spans                      | `""`       |

### Architecture

```
Worker (otelgrpc client handler)                    Master (otelgrpc server handler)
  ┌─────────────────────────┐                       ┌──────────────────────────────┐
  │ cache_lookup span       │                       │ claim_task span              │
  │ validate span           │── W3C traceparent ──▶│ ingest_result span           │
  │ compile span            │   (gRPC metadata)    │ schedule_task span           │
  │ render span             │                       │                              │
  │ upload span             │                       │                              │
  └─────────────────────────┘                       └──────────────────────────────┘
           │                                                 │
           └──────────── OTLP gRPC ─────────────────────────┘
                              │
                         ┌────▼────┐
                         │  OTLP   │
                         │Collector│  (e.g. otel-collector:4317)
                         └────┬────┘
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
                Jaeger    Grafana    Honeycomb
```

### Spans (master + worker)

| Span Name        | Side   | Trigger                              | Attributes                              |
| ---------------- | ------ | ------------------------------------ | --------------------------------------- |
| `schedule_task`  | Master | `PrepareJobAndTask` in enqueue       | `velox.job_id`                          |
| `claim_task`     | Master | `handleTaskAccepted` via gRPC stream | `velox.task_id`, `velox.worker_id`, `velox.attempt_id` |
| `ingest_result`  | Master | `handleTaskResult` via gRPC stream   | `velox.task_id`, `velox.worker_id`, `velox.attempt_id` |
| `cache_lookup`   | Worker | `cache.Get`                          | `velox.worker_id`                       |
| `validate`       | Worker | `spec.Validate` in task runner       | `velox.job_id`                          |
| `compile`        | Worker | `hybrid.Compile`                     | `velox.job_id`                          |
| `render`         | Worker | `exec.Execute` in task runner        | —                                       |
| `upload`         | Worker | `uploadTaskOutputs`                  | `velox.job_id`, `velox.task_id`         |

---

## Q. Change Log

| Date       | Change                                              | Migration |
| ---------- | --------------------------------------------------- | --------- |
| 2026-07-04 | Engine phase + segment histograms (Step 7)          | 070       |
| 2026-07-05 | Versioning columns (Step 8)                         | 071       |
| 2026-07-05 | Output quality validation (Step 9)                  | 072       |
| 2026-07-05 | Per-attempt resource snapshot (Step 10)             | 073       |
| 2026-07-05 | Queue / wait metrics (Step 11)                      | 074       |
| 2026-07-05 | Input context metrics (Step 12)                     | 075       |
| 2026-07-06 | Error classification + canonical codes (Step 13)    | 076       |
| 2026-07-06 | Tracing correlation — trace_id/span_id + OpenTelemetry SDK (Step 15) | 080 |
| 2026-07-06 | Cache metrics refinement — per-tier hit/miss counts (Step 16) | 077 |
| 2026-07-06 | Waste cost metrics (Step 17a)                      | 078       |
| 2026-07-06 | Error classification wired to supervisor tick (Step 16) | —     |
| 2026-07-06 | OTLP gRPC exporter support (Step 17b)              | —         |
| 2026-07-06 | Worker otelgrpc client interceptor (Step 18)       | —         |
| 2026-07-06 | **Metric catalog updated** (this revision)         | —         |
