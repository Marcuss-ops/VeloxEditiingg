# Track 4 — Distributed Rendering, Performance and Scale

Status: canonical TODO  
Priority: after runtime and CI P0 gates are stable

## Outcome

Velox must render a project as a persistent graph of deterministic reusable work rather than a monolithic whole-video operation. Parallelism must improve real wall-clock time without weakening correctness, observability or artifact integrity.

## 1. Immutable master-owned render contract

- [ ] Define one canonical versioned RenderPlan schema.
- [ ] Compile project input through a canonical compiler registry.
- [ ] Reject unknown compiler and contract versions.
- [ ] Normalize semantically equivalent input to the same canonical form.
- [ ] Compute a deterministic plan identity.
- [ ] Persist the immutable plan before Task publication.
- [ ] Prevent workers from rewriting or interpreting project structure.
- [ ] Store explicit inputs, outputs, executor ID, executor version and requirements for every Task.
- [ ] Validate the complete graph before any Task becomes READY.
- [ ] Support forward-compatible read adapters without dual-write.

## 2. Persistent multi-Task DAG

- [ ] Expand one Job into multiple persistent Tasks.
- [ ] Persist dependency edges.
- [ ] Detect missing dependencies and graph cycles.
- [ ] Transition only dependency-satisfied Tasks to READY.
- [ ] Reconstruct the complete graph after master restart.
- [ ] Keep ready queues reconstructible and non-authoritative.
- [ ] Support bounded fan-out and fan-in.
- [ ] Define failure propagation from Task to dependent Tasks and Job.
- [ ] Define cancellation propagation.
- [ ] Make graph publication atomic with the parent Job.
- [ ] Add invariant tests preventing executable Jobs without canonical Tasks.

Initial executor families to validate:

- [ ] asset preparation;
- [ ] text compilation and rendering;
- [ ] reusable precomposition rendering;
- [ ] scene composition;
- [ ] audio mixing;
- [ ] video concatenation;
- [ ] final H.264 encoding.

Only real implementations may be advertised.

## 3. Late composition

- [ ] Separate asset preparation from render work that does not depend on the asset.
- [ ] Allow overlays, text and static elements to render while stock ingestion is pending.
- [ ] Produce intermediate artifacts with explicit compatibility metadata.
- [ ] Compose intermediate outputs only after required dependencies are READY.
- [ ] Avoid rerendering completed independent layers when a late input arrives.
- [ ] Preserve frame timing, color space, alpha semantics and audio synchronization.
- [ ] Define deterministic ordering for overlapping layers.
- [ ] Validate composition output against a reference fixture.

## 4. Content-addressed cache and reusable precompositions

- [ ] Define cache keys from semantic inputs, executor version, engine version and output contract.
- [ ] Exclude machine-specific paths and timestamps from cache identity.
- [ ] Verify artifact hash before accepting a cache hit.
- [ ] Persist local worker cache metadata across restart.
- [ ] Keep the canonical blob artifact path behind the existing BlobArtifacts owner.
- [ ] Prevent executors from performing hand-rolled cache writes.
- [ ] Track cache provenance and compatibility.
- [ ] Evict by bounded policy without deleting referenced authoritative artifacts.
- [ ] Detect and quarantine corrupt cache entries.
- [ ] Measure byte and request hit ratios.
- [ ] Prove cold-cache and warm-cache output hashes are equivalent.

## 5. Executor registry and worker capabilities

- [ ] Keep one worker-side executor registry.
- [ ] Derive Hello capabilities only from registered descriptors.
- [ ] Include executor ID, version, resource class, temporal mode, deterministic flag and cacheable flag.
- [ ] Reject duplicate descriptor keys.
- [ ] Reject Task contracts unsupported by the selected worker.
- [ ] Keep resource sampling behind one sampler.
- [ ] Prevent job-type switch statements from duplicating registry dispatch.
- [ ] Add contract tests between master placement requirements and worker descriptors.

## 6. Cost-aware and locality-aware scheduling

- [ ] Define one master-side estimator registry.
- [ ] Persist estimated cost with the Task or scheduling decision.
- [ ] Filter workers by executor capability.
- [ ] Filter by resource class and temporal mode.
- [ ] Filter deterministic and cacheability requirements.
- [ ] Filter by current admission and pressure state.
- [ ] Score queue age and priority.
- [ ] Score estimated completion time using measured worker profiles.
- [ ] Score data locality and cache availability.
- [ ] Score transfer cost and measured bandwidth.
- [ ] Apply fairness so large projects cannot starve smaller work indefinitely.
- [ ] Produce a structured explanation for selected and rejected workers.
- [ ] Persist enough scheduling evidence for post-incident analysis.
- [ ] Measure estimation error and update profiles without moving scoring ownership to workers.

## 7. Temporal sharding

- [ ] Classify executors as global, frame-local or windowed.
- [ ] Define safe shard boundaries for frame-local work.
- [ ] Define overlap windows for temporally dependent effects.
- [ ] Include frame rate, time base, codec and color metadata in shard contracts.
- [ ] Produce deterministic shard identities.
- [ ] Run shards on different eligible workers.
- [ ] Reject incompatible shard outputs before concat or mux.
- [ ] Assemble compatible shards without a full-project rerender.
- [ ] Preserve exact duration and frame count.
- [ ] Preserve audio/video synchronization.
- [ ] Handle a failed shard without rerunning successful shards.
- [ ] Verify sharded and non-sharded reference output within the approved deterministic contract.

## 8. Observability and performance model

Per TaskAttempt, record typed measurements:

- [ ] queue time;
- [ ] asset wait;
- [ ] cache lookup;
- [ ] download;
- [ ] decode;
- [ ] compile;
- [ ] simulate;
- [ ] render;
- [ ] composite;
- [ ] encode;
- [ ] upload;
- [ ] finalize;
- [ ] total wall time;
- [ ] input and output bytes;
- [ ] bytes from source, BlobStore and local cache;
- [ ] CPU time and peak RSS;
- [ ] GPU time and peak VRAM when applicable;
- [ ] estimated time and estimation error.

Per project, record:

- [ ] wall-clock time;
- [ ] planned and actual critical path;
- [ ] total worker busy time;
- [ ] parallel efficiency;
- [ ] allocated and peak-active workers;
- [ ] cache byte hit ratio;
- [ ] retry and straggler counts;
- [ ] transfer and compute cost per output minute.

- [ ] Validate phase names through a canonical registry or enum.
- [ ] Prevent opaque JSON from being the only metric source.
- [ ] Publish dashboards that distinguish compute, waiting, transfer and reconciliation time.
- [ ] Define alerts for extreme estimation error and persistent stragglers.

## 9. CPU-first performance gates

- [ ] Keep a deterministic CPU-only fixture suite.
- [ ] Benchmark representative small, medium and heavy projects.
- [ ] Record cold-cache and warm-cache baselines.
- [ ] Record single-worker whole-job baseline.
- [ ] Record multi-worker DAG result.
- [ ] Prove output correctness before accepting speed improvements.
- [ ] Define maximum acceptable orchestration overhead.
- [ ] Define minimum useful parallel efficiency by workload class.
- [ ] Prevent excessive small Tasks whose scheduling overhead exceeds compute value.
- [ ] Bound memory, disk and temporary storage per Task.
- [ ] Detect thermal throttling during long CPU renders.
- [ ] Keep GPU-specific optimization optional and capability-gated.

## 10. Scale and soak validation

- [ ] Test multiple concurrent projects.
- [ ] Test a fleet with heterogeneous CPU classes.
- [ ] Test mixed worker versions during rollout.
- [ ] Test cache locality under worker churn.
- [ ] Test scheduler fairness under sustained queue pressure.
- [ ] Test database and outbox growth over long runs.
- [ ] Test artifact reconciliation under high concurrency.
- [ ] Test master restart with a large persistent DAG.
- [ ] Test one slow straggler among otherwise fast workers.
- [ ] Run a minimum 24-hour distributed-rendering soak.
- [ ] Report throughput, p50, p95, p99, retries, failures and resource peaks.

## Definition of Done

- [ ] One Job reliably expands into a validated persistent multi-Task DAG.
- [ ] Independent work executes in parallel and reduces representative wall-clock time.
- [ ] Verified precompositions are reused by deterministic content hash.
- [ ] Placement uses capability, live resources, cost and locality with an explainable decision.
- [ ] Safe work is temporally sharded and recomposed without a full rerender.
- [ ] Master restart reconstructs scheduling state without manual intervention.
- [ ] Critical path and parallel efficiency are available per project.
- [ ] CPU-only output correctness and resource limits pass the fixture suite.
- [ ] The 24-hour distributed soak meets the approved correctness and performance SLOs.
