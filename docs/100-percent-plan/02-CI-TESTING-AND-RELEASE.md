# Track 2 — CI, Testing and Release Integrity

Status: canonical TODO  
Priority: P0 before production

## Outcome

A green required check must prove that the repository is formatted, architecturally valid, unit-tested, integration-tested, natively tested and capable of completing a real workload. A release must identify and promote immutable signed artifacts.

## 1. One canonical verification entrypoint

- [ ] Keep `make verify` as the only full repository verification entrypoint.
- [ ] Keep workflow files as thin dispatchers rather than duplicated build logic.
- [ ] Run from a clean checkout with full history required by diff-scoped checks.
- [ ] Fail when the working tree is dirty unless an explicit local-only override is used.
- [ ] Run all architectural invariant scripts from the canonical entrypoint.
- [ ] Run formatting, vet and race-enabled tests for every Go module.
- [ ] Run native configure, build and tests.
- [ ] Build master and worker images from the same source checkout.
- [ ] Print a concise final summary of every gate and its duration.

## 2. Formatting and toolchain consistency

- [ ] Ensure `gofmt -w` produces no diff on committed code.
- [ ] Add a non-mutating formatting check for pull requests.
- [ ] Align the Go version in `go.work`, every `go.mod`, Docker builders and GitHub Actions.
- [ ] Add a CI check that rejects version drift.
- [ ] Pin or document the minimum CMake, compiler and FFmpeg versions.
- [ ] Print Go, CMake, compiler, Docker, FFmpeg and ffprobe versions in CI logs.
- [ ] Keep `VERSION.txt` as the only product version source.
- [ ] Reject image tags that disagree with `VERSION.txt`.

## 3. Execute native tests

- [ ] Run `ctest --test-dir /tmp/velox-engine --output-on-failure` after the C++ build.
- [ ] Fail CI when zero native tests are discovered.
- [ ] Add unit coverage for render-plan parsing.
- [ ] Add unit coverage for FFmpeg progress parsing and sidecar generation.
- [ ] Add invalid-input and corrupt-media fixtures.
- [ ] Run a minimal deterministic native render smoke.
- [ ] Archive native test logs on failure.
- [ ] Run AddressSanitizer and UndefinedBehaviorSanitizer in a scheduled or dedicated job.

## 4. Critical dependency policy

- [ ] Install FFmpeg and ffprobe explicitly in CI.
- [ ] Fail critical bootstrap and workload tests when FFmpeg or ffprobe is missing.
- [ ] Permit dependency-based skips only for local developer runs.
- [ ] Make CI skip decisions visible as failures for mandatory suites.
- [ ] Verify Docker daemon availability before heavy gates begin.
- [ ] Verify the native engine binary is executable before running worker tests.
- [ ] Add a test that the worker image contains every required runtime dependency.

## 5. Required test layers

### Unit and invariant

- [ ] Go unit tests with race detector.
- [ ] C++ unit tests with CTest.
- [ ] State transition tables for Job, Task, TaskAttempt, upload and artifact.
- [ ] Single-writer and no-legacy full-tree checks.
- [ ] Migration ordering and forward-only schema tests.
- [ ] Registry uniqueness and descriptor validation tests.
- [ ] Secret and private-path redaction tests.

### Integration

- [ ] SQLite repository contract tests.
- [ ] gRPC authentication and protocol contract tests.
- [ ] BlobStore upload, promotion and reconciliation tests.
- [ ] Outbox dispatch and replay tests.
- [ ] Delivery idempotency tests.
- [ ] Worker bootstrap, doctor and readiness tests.

### Real workload E2E

- [ ] Start a real master with a temporary database and BlobStore.
- [ ] Start a real worker agent with the real executor registry.
- [ ] Use the real native engine and FFmpeg.
- [ ] Complete Hello, HelloAck, TaskOffer, TaskAccepted and TaskLeaseGranted.
- [ ] Render a deterministic CPU-only fixture.
- [ ] Submit TaskResult through the real control plane.
- [ ] Upload and finalize the real artifact.
- [ ] Assert TaskAttempt `SUCCEEDED`.
- [ ] Assert Task `SUCCEEDED`.
- [ ] Assert Artifact `READY`.
- [ ] Assert Job `SUCCEEDED`.
- [ ] Inspect output with ffprobe.
- [ ] Verify SHA-256 and file size.
- [ ] Assert master and worker metrics are non-zero.
- [ ] Archive logs, database snapshot and output metadata on failure.

### Production-like mTLS E2E

- [ ] Generate an ephemeral test CA, master certificate and unique worker certificate.
- [ ] Run the complete real workload over mTLS.
- [ ] Reject plaintext-to-TLS.
- [ ] Reject wrong CA.
- [ ] Reject expired certificate.
- [ ] Reject certificate belonging to another worker.
- [ ] Reject partial TLS configuration.
- [ ] Assert there is no insecure fallback.

## 6. Required-check policy

- [ ] Make the fast architecture and Go suite required on every PR.
- [ ] Make native CTest required on every relevant PR.
- [ ] Make real workload E2E required for runtime, protocol, artifact, worker and renderer changes.
- [ ] Make mTLS E2E required before release and for security-sensitive changes.
- [ ] Add path filters only when they cannot hide a runtime dependency.
- [ ] Prevent direct push to `main`.
- [ ] Require an up-to-date branch before merge.
- [ ] Require review for ownership, schema or protocol changes.
- [ ] Cancel superseded runs without cancelling the newest required result.
- [ ] Block merge when a required workflow is absent, skipped or neutral.

## 7. Master and worker image integrity

- [ ] Build both images from a fresh checkout without prebuilt local binaries.
- [ ] Run as a deterministic non-root UID/GID.
- [ ] Keep compilers and build tools out of runtime layers.
- [ ] Use explicit health/readiness probes.
- [ ] Generate SBOM and provenance for both images.
- [ ] Sign both image digests through keyless OIDC or the approved signing policy.
- [ ] Scan images for known critical vulnerabilities.
- [ ] Record commit SHA, version and build time in each binary.
- [ ] Verify the running binary reports metadata matching the image label.
- [ ] Never publish `latest` as the deployment contract.

## 8. Promote without rebuild

- [ ] Build a release candidate once.
- [ ] Deploy staging by immutable digest.
- [ ] Run doctor, mTLS workload, recovery suite and soak against that digest.
- [ ] Promote the identical digest to production.
- [ ] Record the previously approved digest for rollback.
- [ ] Test rollback without rebuilding.
- [ ] Preserve mixed-version protocol compatibility during staged rollout.
- [ ] Fail rollout if runtime version, engine version or bundle version diverges.

## 9. Release evidence

Every release must publish or retain:

- [ ] source commit SHA;
- [ ] `VERSION.txt` value;
- [ ] master image digest;
- [ ] worker image digest;
- [ ] SBOM;
- [ ] provenance attestation;
- [ ] signature verification output;
- [ ] required-check result links;
- [ ] workload and mTLS E2E report;
- [ ] recovery suite report;
- [ ] soak report;
- [ ] approved rollback digest.

## Definition of Done

- [ ] A green required check cannot occur when formatting, CTest, workload E2E or required dependencies are broken.
- [ ] The complete release candidate is reproducible from a clean checkout.
- [ ] Staging and production run the same signed digest.
- [ ] Rollback is demonstrated and documented with evidence.
- [ ] No release-critical test is silently skipped.
