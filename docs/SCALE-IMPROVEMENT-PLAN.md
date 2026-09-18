# Scale Improvement Plan — 1k → 10k → 100k videos/hour ("Masterminds Edition")

Status: **PROPOSAL** — open items only, nothing here is merged yet. Per the
repo completion policy (`docs/100-percent-plan/00-TARGET-AND-DEFINITION-OF-DONE.md`),
a checkbox may be marked complete only with merged code + reproducible evidence.
This document is not an active implementation contract until items graduate
into the 100-percent plan.

Owner: Velox platform
Created: 2026-09-15
Baseline evidence: jobs v14–v16 (2026-09-15), worker v1.4.31, commits
`c46493c2`, `720788e9`, `1553dd23`, `d7f82020`, `c5092caf`, `d41af771`,
`c7dcfa6b`, `584c1798`, `55860f7c`, `578e2197`.

---

## 1. Measured baseline (job v16, 266.224s output, 51 segments)

| Metric | Value |
|---|---|
| Render path | `mixed_packet`, `encode_passes=0`, final concat stream-copy |
| Native render | 7.499 s |
| Plan compile | ~4 ms real (telemetry boundary fixed in `720788e9`) |
| Job wall cold cache | 19.825 s (12 miss / 13 hit) |
| Job wall warm cache | 12.029 s (25 hit / 0 miss) |
| Download cold | ~7.1 s (25 requests for 12 unique assets — duplicate fetch) |
| Drive upload | ~10 s; full delivery 13.174 s |
| Output size | 178.142.188 B |
| CPU per job | 8.8 s CPU over 19.8 s wall, peak ~123% of one core |
| Known open bug | Persistence RESOLVED (`fb4cc081`: migration `173_packet_copy_metrics.sql` + canonical columns + read/write + pin test); typed-path aggregates RESOLVED (`fa79aa7f`: proto tags 93–97, regenerated pb, worker/Master mapping, reflect contract test); runtime guard RESOLVED (`9e8989df`: `PacketCopyContractViolated`). Remaining item is v17 fleet evidence: inspect JSON 51/51 and 100%. |

Per-video budget today: ~36% cold download, ~66% upload+delivery, ~0% encode.
CPU is no longer a constraint; every remaining wall is I/O or policy.

## 2. The physics ladder — what each target costs

Assumptions: monolithic MP4 178 MB vs. bitrate-cut ~60 MB vs.
manifest-first (audio-only unique bytes ~4.3 MB @128 kbps × 266 s).
Slots = concurrent jobs; warm 12.0 s, cold 19.8 s.

| Target | Jobs/s | Slots (warm) | Egress monolithic | Egress @60MB | Egress manifest-first | Storage churn monolithic | Verdict |
|---|---|---|---|---|---|---|---|
| 1.000/h | 0.28 | ~4 | 0.4 Gbit/s | 0.13 Gbit/s | ~0.03 Gbit/s | 178 GB/h | **This week** (Drive quota dies in ~4h) |
| 10.000/h | 2.78 | ~34 (55 cold) | 4.0 Gbit/s | 1.3 Gbit/s | ~0.1 Gbit/s | 1.78 TB/h | **The plan below** — needs W1–W4 |
| 30.000/h | 8.33 | ~100 | 11.9 Gbit/s | 4.0 Gbit/s | ~0.3 Gbit/s | 5.3 TB/h | Monolithic ceiling: 10Gbps NICs + direct multipart; painful |
| 100.000/h | 27.8 | ~334 (42 hosts @8) | **39.6 Gbit/s** | 13.3 Gbit/s | **~1 Gbit/s** | **17.8 TB/h** | Only with W5 (manifest-first) + own delivery substrate |

Aggregate CPU at 100k/h: 27.8 × 8.8 s = ~245 core-seconds/s → ~336 cores across
42 hosts. Fits. Compute never becomes the wall again; packet-copy settled that.

## 3. Walls, in the order they saturate

1. **W1 — Drive quota (today).** 750 GB/day/account. At 1k/h the account dies
   in ~4 h; at 10k/h in ~25 min. Not tunable. Drive exits the hot path.
2. **W2 — Per-host egress (~10k/h).** 8 slots × 178 MB / 12 s ≈ 1 Gbit/s per
   host sustained. Fine on 2.5–10 Gbps NICs; impossible on 1 Gbps VPS.
   Bootstrap bandwidth benchmark decides placement (see §6).
3. **W3 — Monolithic storage churn (~30k/h).** 1.78–5.3 TB/h of unique output
   must be written, uploaded, and deleted. Origin egress + multipart fan-in
   dominate. Zero-disk streaming helps; manifest-first deletes the problem.
4. **W4 — Destination ingest (≥30k/h).** YouTube Data API: `video.insert`
   costs 1600 quota units; default project = 10k units/day ≈ **6 uploads/day**.
   Even audited/extended quotas plus multi-project/channel sharding land at
   hundreds-to-low-thousands of uploads/hour — and content-policy risk grows
   faster than quota. **If the destination is YouTube, 100k/h is gated by
   YouTube, not by Velox.** No engineering changes this.

## 4. Work items (who would do what)

### W1 — Delete Drive from the hot path *(Musk: "the best part is no part"; Vogels: everything fails)*
- New delivery destination: object storage (S3/R2-compatible) with multipart
  direct-from-worker upload; Drive becomes an on-demand mirror only.
- Workers upload their own output directly (master stays on the control path,
  not the data path). R2-class zero-egress storage preferred for origin.
- Capability contract per AGENTS.md §6: `DISABLED`/`READY`/`MISCONFIGURED`,
  fail-closed readiness pairing, `check-capability-contract.sh` coverage.
- **Evidence:** delivery p95 < 3 s at 178 MB, zero Drive API calls on hot path.

### W2 — Interrogate the bitrate requirement *(Musk: every requirement has a name)*
- Name the owner of "output = 178 MB". Target viewing surface likely accepts a
  1080p ladder at ~60–83 MB. A 30–50% cut deletes 1.2+ Gbit/s at 10k/h with
  zero code — pure requirement hygiene, one encode profile change.
- UPDATED: prepared/trimmed segments now target 2.25 Mbps video with a 4.5 Mbps
  VBV buffer and pinned 128 kbps AAC (`video_trimmer.go`). Sources that report a
  higher video bitrate are forced through normalization before stream-copy, so
  the final concat inherits the lower cap. Still open: visual approval and
  ffprobe verification of one delivered output, then measure the egress/delivery
  delta on the fleet.
- **Evidence:** ladder spec + ffprobe verification + egress/video delta.

### W3 — Hot-set pinning + out-of-band cache fill *(Cockcroft/OpenConnect; Gray's five-minute rule)*
- Policy is now pinned in production defaults: Master plans 3 jobs ahead,
  protects 10 jobs ahead, and expires plans after 2 minutes; Worker reserves a
  20 GiB prefetch byte budget. The existing FutureAssetPlan + singleflight
  resolver makes the next job a cache hit when the plan completes in time.
- Remaining evidence only: run the v17 canary with the hot set, then require
  `cache_miss_bytes=0`, `downloaded_during_attempt=0`, and
  `duplicate_download_bytes=0` in inspect JSON. The repository certification
  tests already cover cold/warm/prefetch origin classification.
- Dedupe the 25-requests-for-12-assets waste (singleflight per cache key;
  `duplicate_download_bytes` column already exists to prove it).
- **Evidence:** 0 miss on fresh worker after warm; duplicate_download_bytes → 0.

### W4 — Zero-disk streaming concat → multipart upload *(Carmack)*
- LANDED (current tranche): the worker sends an authenticated upload intent on
  the first safe renderer write, receives a fenced `master-stream.v1` session,
  and uploads immutable parts while the mux is still writing. The final
  declaration reuses that upload session, so the upload wait is overlapped with
  render/finalization instead of starting from zero after render.
- The strict zero-disk variant is still open: the renderer continues to write
  its durable local output and the progressive reader consumes that growing
  file. Removing that write requires a fragmented-container pipe plus a
  durable retry/spool policy; it is not claimed by this tranche.
- **Evidence:** one v17 canary with `progressive_overlap_ms > 0`,
  `progressive_overlap_bytes_before_render > 0`, final SHA/size equality, and
  `final_concat_stream_copy=true`; fleet delivery wall versus render wall.

### W5 — Manifest-first delivery *(Kay: change the representation)* — the 100k unlock
- The job is a template: `{intro, stock pool, voiceover, shuffle seed}`.
  Ship the manifest (~KB) + unique audio; edge/CDN assembles CMAF chunks from
  the shared pool. 95% of bytes are shared across "different" videos.
- 100k/h becomes "sign 10k playlists + upload 10k audios" ≈ 1 Gbit/s.
- Monolithic MP4 survives as an on-demand remux for legacy destinations
  (YouTube included) — the 10% you add back, per Musk's rule.
- Requires chunk factory (keyframe-aligned 1s chunks, content-addressed,
  cached like W3) and a player/edge assembly path or per-destination remux.
- **LANDED (first consumer, 2026-09-18):** `worker-agent-go/internal/chunkfactory`
  now has the packet-copy call site: when `chunk_store_enabled` is explicitly
  enabled, the executor plans each certified prepared fragment, installs the
  content-addressed payload, and writes an atomic worker-local manifest.
  Chunk identity = SHA-256 over {asset_key, source window, profile,
  chunk_index, identity_version}; chunks start only on keyframes. The
  capability remains default-off and follows DISABLED/READY/MISCONFIGURED per
  AGENTS.md §6. Missing for W5 completion: Master manifest delivery and the
  edge/player assembly path.
- **Evidence:** two "different" videos sharing >90% byte-identical chunks;
  origin egress per video < 5 MB.

### W6 — Measure the ceilings before buying hardware *(Feynman: decisive experiment; Patterson: roofline)*
- Bootstrap micro-benchmarks per worker: NVMe r/w MB/s, uplink/downlink
  measured (not assumed) → heartbeat → scheduling input.
- cgroup `nr_throttled` + PSI (`cpu.pressure`, `io.pressure`) per attempt —
  the only proof of silent CFS throttling at N=8–10.
- Concurrency ladder 2→4→6→8→10 per host; record `job_wall_ms(N)` curve.
- **Evidence:** ceiling numbers in heartbeat; scaling curve doc in
  `docs/benchmarks/`.

### W7 — Telemetry contract hardening *(Pike: measure, don't guess; Amdahl: no unknown serial fraction)*
- LANDED (`fb4cc081`, migration `173_packet_copy_metrics.sql`): the six columns
  are in `attemptMetricsColumns` + read/write + pin test; master derives the
  breakdown from `segment_timings` (`applySegmentMetrics`) with the worker
  aggregate as legacy fallback.
- LANDED (`fa79aa7f`): add the missing proto fields on `TaskExecutionMetrics` (`segments_total`,
  `segments_packet_copy`, `segments_reencoded`, `packet_copy_bytes`,
  `reencoded_bytes`; next free tags after 92), regenerate pb, and map them in
  `typed_metrics_proto.go` + `executionMetricsToAttemptMetrics` so worker
  aggregates survive when a renderer emits no per-segment timing rows.
- LANDED (`fa79aa7f`): reflect-based transport contract test covers every
  proto-backed `RawExecutionMetrics` field across `ToProto`/`FromProto`; the
  existing canonical column pin covers persistence order. The v1.4.28/v1.4.29
  class of silent metric loss is now structurally caught.
- Close the waterfall: `unaccounted_ms = wall − Σ(phases)` must → ~0.
- **Evidence:** inspect JSON shows 51/51, 100%; contract test in CI.

### W8 — Chaos staging before fleet growth *(Cockcroft: Simian Army; Lamport: spec it)*
- Kill a worker mid-render at N=10 (lease requeue, no starvation).
- Inject Drive/storage 429s and latency; fill a cache disk to the pressure
  watermark; verify admission hysteresis recovers.
- TLA+/invariant pass on the lease scheduler before >8 slots per host.
- **LANDED (DAG slice, 2026-09-18):** scenario
  `tests/e2e/recovery-matrix/scenarios/20-dag-kill-worker-propagation.sh` —
  worker kill mid-DAG, FAILED-root propagation to dependent PENDING tasks,
  formal invariants DAG-P1 (no PENDING behind terminal-FAILED dependency) and
  DAG-P2 (no dangling edges) in `invariants.sh`, run.sh enumerates 01–20.
  Remaining slices: N=10 concurrency kill, 429 injection, cache-disk fill.
- **Evidence:** scenario 20 runs 12/12 PASS (fixture DB; live-server mode via
  `API_URL`).

### W9 — Process fixes from the 2026-09-15 session *(Stroustrup: designated initializers; the 8-release day)*
- C++ aggregate initializers → designated initializers (the v8
  `transform_required` misalignment class).
- LANDED: mixed golden-path gate runs **before** Docker build/sign/certify in
  `worker-image.yml` (`Mixed packet-copy golden gate` step, pre-Buildx;
  `scripts/ci/check-mixed-render-gate.sh`).
- Engine binary split from worker image: engine fix = minutes, not a fleet
  rollout.
- LANDED (`9e8989df`, Master side): the 6th alertengine rule fires
  `PacketCopyContractViolated` (critical) when an attempt reports
  `concat_mode=mixed_packet` but `encode_passes>0` or `packet_copy_ratio<100`
  (AGENTS.md §6 pattern), so the original 51-encode anomaly cannot silently
  return at runtime. CI pin and runtime alert are both present; only v17 fleet
  evidence remains.

## 5. Economics at 100k/h (why W5 is not optional)

| Model | Egress volume/h | CDN-class cost @ $0.005–0.01/GB | Notes |
|---|---|---|---|
| Monolithic 178 MB | 17.8 TB | **$89–178/h ≈ $2.1–4.3k/day** | plus 17.8 TB/h storage churn |
| Bitrate-cut 60 MB | 6 TB | $30–60/h | still linear in output |
| Manifest-first | ~0.43 TB (audio) | **$2–4/h** | 30–60× cheaper; chunks cached at edge |

Zero-egress object storage (R2-class) shifts origin cost to ~0 either way,
but the churn/limit walls remain — only the representation change removes them.

## 6. Missing metrics for the capacity model

Already collected: `cpu_time_ms` (cgroup v2), `peak_rss_bytes`, RSS admission
pressure, `disk_read/write_bytes`, `iowait_ms`, `network_rx/tx_bytes`,
`job_scratch_peak_bytes`, cache hit/miss, `queue_ms`/`lease_wait_ms`,
p25/p50/p95/p99 rollups.

Missing (needed to compute the absolute per-worker limit
`capacity = slots × 3600 / max(T_cpu, T_disk, T_net_in, T_net_out)`):

1. Sustained disk ceiling (fio at bootstrap) — heartbeat field.
2. Measured uplink/downlink — heartbeat field.
3. cgroup `nr_throttled` + PSI pressure — per attempt.
4. `jobs_concurrent_at_start` — the N of the scaling curve.
5. `unaccounted_ms` per attempt — waterfall closure to 100%.
6. Delivery egress per destination + Drive/storage 429 rate.
7. Per-asset-id cache hit-rate (cold-set → prefetch input).
8. Slot utilization time series (`used/total` per worker).

## 7. Sequencing

### Track A — no new substrate (no object storage required)

Applicable now on Drive-only delivery: **W2** (bitrate cut), **W3** (hot-set
pinning + dedupe), **W6** (ceiling benchmarks), **W7** (telemetry migration +
contract tests), **W8** (chaos staging), **W9** (process/CI fixes), plus the
chunk factory *precursor* of W5. W4 shrinks to streaming the concat into the
Drive resumable-upload session (removes the 231 MB local write+read-back).

Projected effect: warm job ~12.0 → ~9-10 s (upload scales with bitrate), cold
cache eliminated as a runtime path (W3). Fleet 4×2 slots at ~9.5 s/job ≈
**~3,000/h burst**; raising to 4 slots/host after the W6/W8 validation ≈
**~6,000/h burst**. Sustained 24/7 remains capped by the Drive quota:
750 GB/day ÷ 60 MB ≈ **~520 videos/hour per account** — bursts spend the
window faster (e.g. ~3,000/h for ~4 h, then the account is exhausted until
midnight PT). Multi-account sharding is fragile and ToS-sensitive; it is not
the durable answer, W1 is.

| Week | Items | Unlocks |
|---|---|---|
| 1 | W7 (migration + contract tests), W3 (pin + warm), W6 (benchmarks) | trustworthy numbers; cold path gone |
| 2 | W1 (object-storage destination), W2 (bitrate ladder), W9 (CI/process) | 1k→5k/h sustainable |
| 3–4 | W4 (zero-disk streaming), W8 (chaos), concurrency ladder | 10k/h with 34–56 slots |
| Later | W5 (chunk factory + manifest delivery) | 30k/h monolithic ceiling broken; 100k/h becomes procurement |

### Track A progress (2026-09-18)

- **Batch plan dedupe — LANDED:** identical items inside one
  `POST /api/v1/jobs/batch` collapse onto one render (fingerprint over
  render-relevant fields after normalization; publications/delivery excluded
  so differing destinations still dedupe). Response carries `summary`
  {total, accepted, deduped, rejected, conflict, failed} and per-item
  `status:"dedup"` + `deduped_of`. Makes the batch surface a
  template+variants contract: pay one render, publish N times.
- **Multi-Task DAG — LANDED (substrate):** `depends_on` persisted (migration
  176), fail-closed `ValidateTaskGraph` (cycles, unknown deps), anti-zombie
  failure propagation in `TickReadiness`, `ExpandService` fan-out enqueue,
  deterministic multi-task `GetByJobID`. Late composition (asset-prep ∥
  render) can now be expressed at enqueue; the enqueue-time decomposition of
  the monolithic recipe into the DAG is the remaining step.
- **Chunk factory (W5 precursor) — LANDED** (see W5 above).
- **Chaos DAG slice (W8) — LANDED** (see W8 above).

**Bottom line:** with W1–W4 the farm is compute-unbounded and I/O-limited at
roughly **10k/h (this fleet ×4) to ~30k/h (monolithic ceiling)**. 100k/h is
reachable only through W5 — and only if the destination's ingest policy
allows it; YouTube's API quota is the first wall that no amount of Velox
engineering can move.
