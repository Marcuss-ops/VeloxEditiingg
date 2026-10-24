# Issue filed 2026-10-24 — separate Master, asset storage, and render capacity

GitHub ticket: [#37 — Separate Master, stock storage, and render capacity](https://github.com/Marcuss-ops/VeloxEditiingg/issues/37)

## Current context

- Two OVH VPS instances, each reported as 8 vCPU / 200 GB disk and about
  $20/month plus VAT; one is the Velox Master.
- Stock/source assets currently move between machines manually.
- The fixed library is about 50 GB today and is expected to grow to hundreds
  of GB.
- The scale target is multiple videos per minute.
- The stock library should not live on the Master VPS disk.

## Question to resolve

Choose a scalable asset and render topology: keep the Master on OVH or move
it; select canonical object storage and worker cache behavior; and determine
whether OVH workers or burst workers such as RunPod provide the best cost and
throughput for actual Velox jobs.

## Initial direction

Keep the Master as the persistent control plane unless measurements identify
a reason to move it. Put canonical stock assets in S3-compatible object
storage, preferably with a low-cost network path from the workers. Workers
should use verified local caches and fetch only missing assets. Evaluate
RunPod as an optional render-worker pool, not as the default Master host.

Do not assume GPU workers accelerate the existing copy-only path: the
repository has measured 333 ms p50 for a 300-second copy-only assembly with
zero decoded or encoded video frames. Benchmark compositing/encoding separately.

## Acceptance criteria

- Routine jobs and library updates require no manual VPS-to-VPS copying.
- The Master does not store the complete source library on its local disk.
- Object storage cost is estimated at 50 GB, 500 GB, and 1 TB, including
  expected monthly reads and transfer conditions.
- Asset identities are hash-bound and verified; warm caches are reused.
- Placement can favor workers with the required assets cached, with a safe
  cold-cache path for new workers.
- OVH-only and OVH-plus-RunPod are compared by measured throughput, startup,
  transfer, reliability, and total cost per completed video.

## References

- [Rendering speed and workflow proposal](../architecture/RENDERING-SPEED-AND-WORKFLOW-PROPOSAL.md)
- [Phase 1 packet-copy benchmark](../benchmarks/phase1-zero-spawn-vs-ffmpeg-2026-08-12.md)
- [Current architecture](../architecture/current-architecture.md)
