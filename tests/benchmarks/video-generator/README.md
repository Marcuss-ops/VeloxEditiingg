# Video generator certification workloads

These are the frozen certification workloads for the video generator. They
are deliberately separate from correctness fixtures and from worker upgrade
tests.

| Case | Purpose |
|---|---|
| `minimal` | Fixed scheduling/download/FFmpeg overhead: one short scene. |
| `five-legendary-boxers` | Representative normal workload: intro, five portraits, clips, stock and voiceover. |
| `heavy` | 24 scenes with repeated media layers, subtitles, music and overlays. |
| `pathological` | Expected rejection/failure: corrupt URL, invalid duration and missing audio. |

The case registry is [cases/registry.json](cases/registry.json). The boxer
payload is the operational fixture in
[`ops/jobs/five_legendary_boxers_it.generate.json`](../../../ops/jobs/five_legendary_boxers_it.generate.json).

Every run must record:

```text
commit, worker_version, cpu, ram, disk, job_type, template_id,
cache_mode, concurrency, job_id, attempt_id, status, output_sha256
```

Per-attempt phase/resource data already exists in `task_phase_timings`,
`task_attempt_metrics` and `task_attempt_cache_stats`. The certification
runner must read those records rather than inventing a second metric model.

The stock rule is part of the workload contract: multiple stock assets are
deterministically shuffled per job and scene, then repeated until the exact
voiceover duration is covered. The final stock segment is trimmed to the
remaining duration.
