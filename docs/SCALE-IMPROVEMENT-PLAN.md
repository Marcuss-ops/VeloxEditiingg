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
| Known open bug | Persistence RESOLVED (`fb4cc081`: migration `173_packet_copy_metrics.sql` + canonical columns + read/write + pin test); master derives the breakdown from `segment_timings` (`applySegmentMetrics`). Residual: worker aggregates `segments_*`/`packet_copy_bytes`/`reencoded_bytes` have NO proto fields on `TaskExecutionMetrics` (only `packet_copy_ratio=64`) and are dropped on the typed path — plus reflect round-trip contract test and Master-side capability alert (W7/W9 below) |

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
- LANDED (`0c6e1242`): prepared/trimmed segments capped at 4 Mbps
  (`-b:v 4M -maxrate 4M -bufsize 8M` in `video_trimmer.go`) → ~137 MB for the
  266 s output at ratio 100 (stream-copy inherits the cap). Still open: confirm
  the ladder owner accepts ~4.1 Mbps / 137 MB vs the 60–83 MB target
  (~2–2.5 Mbps); pin audio `-b:a`; ffprobe-verify one delivered output;
  measure the egress/delivery delta on the fleet.
- **Evidence:** ladder spec + ffprobe verification + egress/video delta.

### W3 — Hot-set pinning + out-of-band cache fill *(Cockcroft/OpenConnect; Gray's five-minute rule)*
- The 12 unique assets (~60 MB) are re-fetched by every cold job: pin them on
  every worker (they pass the five-minute rule by orders of magnitude).
- Master triggers cache warm at folder expansion (FutureAssetPlan already
  exists) → "cold cache" stops being a runtime path.
- Dedupe the 25-requests-for-12-assets waste (singleflight per cache key;
  `duplicate_download_bytes` column already exists to prove it).
- **Evidence:** 0 miss on fresh worker after warm; duplicate_download_bytes → 0.

### W4 — Zero-disk streaming concat → multipart upload *(Carmack)*
- Stream the packet-copy concat output directly into the multipart upload —
  the 178 MB never touches local disk (kills the write + read-back + the
  `job_scratch_peak_bytes` pressure at N=10).
- Pairs with migration 161 progressive-overlap: first bytes upload while the
  tail still muxes.
- **Evidence:** output write bytes ≈ 0 on attempt metrics; delivery wall < mux wall + tail.

### W5 — Manifest-first delivery *(Kay: change the representation)* — the 100k unlock
- The job is a template: `{intro, stock pool, voiceover, shuffle seed}`.
  Ship the manifest (~KB) + unique audio; edge/CDN assembles CMAF chunks from
  the shared pool. 95% of bytes are shared across "different" videos.
- 100k/h becomes "sign 10k playlists + upload 10k audios" ≈ 1 Gbit/s.
- Monolithic MP4 survives as an on-demand remux for legacy destinations
  (YouTube included) — the 10% you add back, per Musk's rule.
- Requires chunk factory (keyframe-aligned 1s chunks, content-addressed,
  cached like W3) and a player/edge assembly path or per-destination remux.
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
- Add the missing proto fields on `TaskExecutionMetrics` (`segments_total`,
  `segments_packet_copy`, `segments_reencoded`, `packet_copy_bytes`,
  `reencoded_bytes`; next free tags after 92), regenerate pb, and map them in
  `typed_metrics_proto.go` + `executionMetricsToAttemptMetrics` so worker
  aggregates survive when a renderer emits no per-segment timing rows.
- Reflect-based test (still open): every `RawExecutionMetrics` field round-trips `ToProto`
  **and** appears in the canonical column list — the v1.4.28/v1.4.29 class
  of silent metric loss becomes structurally impossible.
- Close the waterfall: `unaccounted_ms = wall − Σ(phases)` must → ~0.
- **Evidence:** inspect JSON shows 51/51, 100%; contract test in CI.

### W8 — Chaos staging before fleet growth *(Cockcroft: Simian Army; Lamport: spec it)*
- Kill a worker mid-render at N=10 (lease requeue, no starvation).
- Inject Drive/storage 429s and latency; fill a cache disk to the pressure
  watermark; verify admission hysteresis recovers.
- TLA+/invariant pass on the lease scheduler before >8 slots per host.
- **Evidence:** failure-injection report per the completion policy.

### W9 — Process fixes from the 2026-09-15 session *(Stroustrup: designated initializers; the 8-release day)*
- C++ aggregate initializers → designated initializers (the v8
  `transform_required` misalignment class).
- LANDED: mixed golden-path gate runs **before** Docker build/sign/certify in
  `worker-image.yml` (`Mixed packet-copy golden gate` step, pre-Buildx;
  `scripts/ci/check-mixed-render-gate.sh`).
- Engine binary split from worker image: engine fix = minutes, not a fleet
  rollout.
- Pin the capability contract (STILL OPEN, Master side): add a 6th alertengine
  rule firing `PacketCopyContractViolated` (critical) when an attempt reports
  `concat_mode=mixed_packet` but `encode_passes>0` or `packet_copy_ratio<100`
  (AGENTS.md §6 pattern), so the original 51-encode anomaly can never silently
  return at runtime. CI pin exists (`worker-mixed-canary.sh`); the runtime
  alert does not.

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

**Bottom line:** with W1–W4 the farm is compute-unbounded and I/O-limited at
roughly **10k/h (this fleet ×4) to ~30k/h (monolithic ceiling)**. 100k/h is
reachable only through W5 — and only if the destination's ingest policy
allows it; YouTube's API quota is the first wall that no amount of Velox
engineering can move.
