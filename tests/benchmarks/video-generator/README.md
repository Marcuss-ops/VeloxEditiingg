# Video generator certification workloads

These are the frozen certification workloads for the video generator. They
are deliberately separate from correctness fixtures and from worker upgrade
tests.

| Case | Purpose | Canonical payload | Submit script |
|---|---|---|---|
| `minimal` | Fixed scheduling/download/FFmpeg overhead: one short scene. | `ops/jobs/benchmark-minimal.generate.json` | `ops/jobs/submit_benchmark_minimal.sh` |
| `five-legendary-boxers` | Representative normal workload: intro, five portraits, clips, stock and voiceover. | `ops/jobs/five_legendary_boxers_it.generate.json` | `ops/jobs/submit_benchmark_five_boxers.sh` |
| `heavy` | 24 scenes with repeated media layers, subtitles, music and overlays. | `ops/jobs/benchmark-heavy.generate.json` | `ops/jobs/submit_benchmark_heavy.sh` |
| `pathological` | Intake-valid payload whose scenes fail at asset resolution/render (fails well). | `ops/jobs/benchmark-pathological.generate.json` | `ops/jobs/submit_benchmark_pathological.sh` |

The case registry is [cases/registry.json](cases/registry.json). **The
canonical payloads live under `ops/jobs/`** — the registry points at them
so there is a single frozen source of truth per benchmark. The minimal /
heavy payloads carry frozen placeholder asset URLs that the submit scripts
replace from `VELOX_BENCHMARK_*_URL` env vars (see each script's header);
the pathological payload is submitted as-is.

### Delivery destination requirement

The M2M benchmarks submit via `POST /api/v1/jobs`, which **requires** an
explicit `delivery_plan` whose `destination_id` exists as an enabled row in
the deployment's `delivery_destinations` table. The frozen payloads use
`destination_id: "drive"`; override with
`VELOX_BENCHMARK_DELIVERY_DESTINATION=<your-destination-id>` if needed.

Every run must record:

```text
commit, worker_version, cpu, ram, disk, job_type, template_id,
cache_mode, concurrency, job_id, attempt_id, status, output_sha256
```

Per-attempt phase/resource data already exists in `task_phase_timings`,
`task_attempt_metrics` and `task_attempt_cache_stats`. The certification
runner must read those records rather than inventing a second metric model
(`collect_report.py --db <master.sqlite> --job-id <id> --out <json>`).

The stock rule is part of the workload contract: multiple stock assets are
deterministically shuffled per job and scene, then repeated until the exact
voiceover duration is covered. The final stock segment is trimmed to the
remaining duration.
