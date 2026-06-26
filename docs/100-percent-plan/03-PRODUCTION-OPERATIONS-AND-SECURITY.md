# Track 3 — Production Operations and Security

Status: canonical TODO  
Priority: P0 before production fleet admission

## Outcome

Every remote worker must be uniquely identified, securely connected, correctly sized, observable and recoverable. Admission to production is evidence-based per worker or homogeneous hardware class.

## 1. Production doctor

Provide one deterministic command that returns `READY` or `NOT_READY` and machine-readable details.

- [ ] Support a documented production validation command.
- [ ] Validate configuration structure and environment.
- [ ] Validate stable worker identity.
- [ ] Validate master DNS and reachability.
- [ ] Validate certificate, key, CA and key-pair compatibility.
- [ ] Validate minimum certificate lifetime.
- [ ] Validate private-key permissions.
- [ ] Validate work, cache, blob, temp and output directories with write/read/delete probes.
- [ ] Validate minimum disk space.
- [ ] Validate health and metrics port availability.
- [ ] Validate native engine existence and executability.
- [ ] Validate FFmpeg and ffprobe.
- [ ] Validate executor registry is non-empty.
- [ ] Validate required executor `scene.composite.v1@1` is present.
- [ ] Validate protocol, engine and bundle compatibility.
- [ ] Optionally perform mTLS handshake, Hello/HelloAck and canary workload.
- [ ] Check the worker is visible through the canonical master API.
- [ ] Return stable versioned JSON with check ID, status, detail and remedy.
- [ ] Exit zero only when every mandatory production check passes.
- [ ] Guarantee no secret, key material or credential path appears in output.

## 2. Worker identity and mTLS

- [ ] Assign a unique stable non-reused `worker_id`.
- [ ] Issue one dedicated client certificate per worker.
- [ ] Bind certificate identity to the expected worker identity contract.
- [ ] Reject shared certificates across workers.
- [ ] Reject self-signed certificates outside the approved Velox PKI.
- [ ] Reject expired and soon-to-expire certificates according to policy.
- [ ] Reject wrong CA and identity mismatch.
- [ ] Reject plaintext in staging and production.
- [ ] Reject partial TLS configuration.
- [ ] Remove every automatic insecure fallback.
- [ ] Store certificate serial and fingerprint in inventory evidence.
- [ ] Redact certificates, secrets, raw topology and credential hashes from APIs and logs.

## 3. Liveness and readiness

### Worker

- [ ] `/health/live` reports only process and main-loop liveness.
- [ ] `/health/ready` reports whether the worker can safely accept a Task.
- [ ] Readiness requires active authenticated session and accepted registration.
- [ ] Readiness requires a valid executor registry.
- [ ] Readiness requires usable cache and BlobStore.
- [ ] Readiness requires sufficient disk.
- [ ] Readiness is false during drain and shutdown.
- [ ] Readiness becomes false immediately when the control stream is lost.
- [ ] Non-ready responses contain typed reasons.
- [ ] Docker and systemd probes use the canonical readiness endpoint.

### Master

- [ ] `/health` remains liveness only.
- [ ] `/ready` verifies bootstrap, database, BlobStore, outbox and mandatory services.
- [ ] The master container health contract uses readiness where a single Docker healthcheck is required.
- [ ] Readiness becomes false before graceful shutdown.
- [ ] Optional fleet-live readiness is explicit and documented.
- [ ] Readiness checks have bounded latency and do not leak secrets.

## 4. Canonical worker state

- [ ] Master API derives state from active session plus heartbeat.
- [ ] Expose only canonical states: CONNECTED, STALE, DISCONNECTED and DRAINING.
- [ ] Expose a typed reason for every non-CONNECTED state.
- [ ] Expose `session_active` and heartbeat age.
- [ ] Expose protocol, engine and bundle versions.
- [ ] Expose registered executor descriptors and Task slots.
- [ ] Expose active Tasks and current Task identity without secrets.
- [ ] Support class and rollout-group filters.
- [ ] Ensure persisted raw worker data does not contain derived or sensitive fields.
- [ ] Alert when state and session evidence disagree.

## 5. Secrets and access control

- [ ] Keep production secrets only in approved vaults or excluded host files.
- [ ] Keep committed configuration values as explicit placeholders.
- [ ] Scan commits and generated deployment files for secrets.
- [ ] Test the secret scanner against positive and negative fixtures.
- [ ] Restrict health, metrics and management ports by firewall, VPN or security group.
- [ ] Apply least-privilege filesystem ownership to runtime paths.
- [ ] Run master and worker as non-root identities.
- [ ] Rotate worker secrets and certificates without rebuilding images.
- [ ] Log security decisions through structured typed events.

## 6. Resource sizing and admission control

For every supported hardware class, measure small, medium and heavy workloads.

- [ ] Record CPU time and wall time.
- [ ] Record peak RSS and available memory.
- [ ] Record temp bytes, output bytes and disk free.
- [ ] Record I/O wait and network upload time.
- [ ] Record temperature and sustained throttling where available.
- [ ] Define a safe `task_slots` or `max_active_jobs` policy by class.
- [ ] Reserve an explicit memory margin for the operating system.
- [ ] Define warning and critical disk thresholds.
- [ ] Block new Tasks under memory or disk pressure.
- [ ] Return typed rejection reasons such as capacity, disk or memory pressure.
- [ ] Keep thresholds in canonical configuration, not executor hardcoding.
- [ ] Verify active Tasks never exceed advertised slots.

## 7. Metrics, alerts and logs

- [ ] Correlate logs with worker, session, Job, Task, attempt and lease IDs.
- [ ] Export session active and heartbeat age.
- [ ] Export active Tasks and available slots.
- [ ] Export CPU, RSS, disk and temp usage.
- [ ] Export network and upload metrics.
- [ ] Export Job and Task outcomes by typed reason.
- [ ] Export reconnect and lease-renewal failure counts.
- [ ] Export artifact upload and finalization failure counts.
- [ ] Export fallback and emergency-path counts.
- [ ] Export certificate residual lifetime.
- [ ] Verify units, especially milliseconds versus seconds and bytes versus MiB.
- [ ] Alert on disconnected or stale workers.
- [ ] Alert on memory or disk pressure.
- [ ] Alert on reconciliation backlog.
- [ ] Alert on fallback greater than zero in production.
- [ ] Alert on certificate warning and critical thresholds.
- [ ] Test alert rules with failure injection.

## 8. PKI lifecycle

- [ ] Fail closed when the certificate directory is absent or unreadable.
- [ ] Fail closed when zero valid worker certificates exist where certificates are required.
- [ ] Detect corrupt, expired, warning and critical certificates.
- [ ] Define and test certificate lifetime thresholds.
- [ ] Support overlap of old and new certificates during rotation.
- [ ] Rotate one worker without dropping active work.
- [ ] Revoke a worker certificate immediately.
- [ ] Archive serial, fingerprint, issued time and expiry.
- [ ] Assign an operational owner for every PKI alert.
- [ ] Exercise live rotation and revocation in staging.

## 9. Canary, soak, rollout and rollback

- [ ] Run one deterministic CPU-only canary on each worker before admission.
- [ ] Prove the canary TaskAttempt belongs to the selected worker.
- [ ] Require Job SUCCEEDED, TaskAttempt SUCCEEDED and Artifact READY.
- [ ] Require ffprobe and SHA-256 verification.
- [ ] Run the canary over production-like mTLS.
- [ ] Run at least 24 hours of soak per supported hardware class.
- [ ] Include small, medium and heavy workloads.
- [ ] Include cold and warm cache cases.
- [ ] Include master restart, worker restart, network partition and drain.
- [ ] Record success rate, failure reasons and p50, p95 and p99 latency.
- [ ] Promote one canary worker by immutable image digest.
- [ ] Observe a defined health window before expanding rollout.
- [ ] Expand by hardware class or percentage.
- [ ] Drain before update.
- [ ] Roll back to the previous digest without rebuilding.

## Per-worker certification record

- [ ] Worker ID and hostname.
- [ ] Hardware class and rollout group.
- [ ] Worker, engine, bundle and protocol versions.
- [ ] Image digest and source commit.
- [ ] Certificate serial, fingerprint and expiry.
- [ ] Doctor report.
- [ ] Canary Job, Task, attempt and artifact IDs.
- [ ] Artifact SHA-256 and ffprobe result.
- [ ] Recovery suite result.
- [ ] Soak interval and workload count.
- [ ] Success rate and failure count.
- [ ] Fallback and emergency-path counts.
- [ ] Final verdict and approver.

## Definition of Done

- [ ] Doctor = READY.
- [ ] mTLS canary = PASS.
- [ ] Artifact integrity = PASS.
- [ ] Recovery suite = PASS.
- [ ] Soak test = PASS.
- [ ] Production fallback count = 0.
- [ ] Critical alerts are tested and routed.
- [ ] Rollout and rollback by digest are demonstrated.
- [ ] No uncertified worker is present in the production allowlist.
