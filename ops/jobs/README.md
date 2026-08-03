# Fixed Direct Jobs

## Official generator benchmarks

Frozen certification workloads (registry: `tests/benchmarks/video-generator/cases/registry.json`):

| Benchmark | Payload | Submit |
|---|---|---|
| minimal | `benchmark-minimal.generate.json` | `submit_benchmark_minimal.sh` (M2M `/api/v1/jobs`, needs `VELOX_BENCHMARK_CLIP_URL` + `VELOX_BENCHMARK_VOICEOVER_URL`) |
| five-boxers | `five_legendary_boxers_it.generate.json` | `submit_benchmark_five_boxers.sh` (`/api/v1/script/generate`, admin token) |
| heavy | `benchmark-heavy.generate.json` | `submit_benchmark_heavy.sh` (M2M `/api/v1/jobs`, needs the 7 `VELOX_BENCHMARK_*_URL` asset vars) |
| pathological | `benchmark-pathological.generate.json` | `submit_benchmark_pathological.sh` (M2M `/api/v1/jobs`, expects clean terminal FAILED) |

The minimal/heavy payloads carry frozen placeholder asset URLs; the submit
scripts replace them from `VELOX_BENCHMARK_*_URL` env vars. Override the
idempotency key with `VELOX_BENCHMARK_IDEM_KEY` for cold/warm cache runs.
M2M scripts mint an ephemeral M2M client via the admin surface
(`VELOX_ADMIN_TOKEN`) and delete it on exit, mirroring `scripts/api/jobs_smoke.sh`.

### Delivery destination requirement

`delivery_plan` is **required** at enqueue, and every `destination_id` must
exist as an enabled row in the deployment's `delivery_destinations` table
(otherwise the job is rejected with `DESTINATION_NOT_FOUND`). The frozen
payloads reference `destination_id: "drive"`; if your deployment uses a
different id, override it with
`VELOX_BENCHMARK_DELIVERY_DESTINATION=<your-destination-id>`. The scripts
deliberately never strip the plan.


`roman_aqueducts_city_engineering.fixed-job.json`
- Direct `script/generate-with-images` payload.
- Includes `skip_creator: true`, so the master enqueues it locally without calling the creator.
- Requires a valid full-length `voiceover_path` before submission.

`submit_roman_aqueducts_city_engineering.sh`
- Helper to submit the fixed job directly to the master.
- Usage:

```bash
./ops/jobs/submit_roman_aqueducts_city_engineering.sh "https://drive.google.com/file/d/<FULL_VOICEOVER_ID>/view"
```

Current reference assets bundled in the JSON:
- Scene image 1: `1QoPBq8z2DB9OUXyjIT3HwgKOYzihF8Mh`
- Scene image 2: `1S6NiFUeLEAQwtGZISX96nRsv6sv_p7f_`
- Reference per-scene voiceovers from creator output:
  - `11F0I60YScJN7tuVkpNhDeHzavHR7An9y`
  - `1z5_Tm7dSbu4tFKIEYpyVrdI7oWqy1dIR`

Important:
- The direct worker path still needs one valid full voiceover URL for the whole script.
- The two `reference_voiceovers` are saved as provenance, not as a guaranteed final mixed track.
