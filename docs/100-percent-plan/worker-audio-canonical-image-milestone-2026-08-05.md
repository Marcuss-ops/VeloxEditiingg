# Canonical worker audio image — milestone 2

## Verdict

**PARTIAL / NOT PROMOTABLE.**

The canonical worker Dockerfile built successfully locally and the image-local
engine verification passed. The image was not published, signed, or deployed.
A repeat build with the same source epoch produced the same engine bytes but a
different image ID, so byte-for-byte image reproducibility is **not certified**.
The published digest-pinned canary was not run because no registry RepoDigest or
cosign installation was available.

No worker service, production container, registry repository, or live canary was
modified.

## Build inputs and local image evidence

```text
Dockerfile: RemoteCodex/native/worker-agent-go/Dockerfile
Build context: repository root
SOURCE_DATE_EPOCH: git commit timestamp of ee1a26d1
First tag: velox-worker:canonical-audio-verify
First image ID: sha256:6c0705b132466640292e2d5fe27c39bde3f2716819ed68ee4cf18263c14246ff
```

The legacy Docker builder was used because `docker buildx` is unavailable in
the environment. The canonical build command omitted the unsupported
`--progress` option after the first probe showed Docker's `unknown flag:
--progress`; no build had run during that first probe.

The successful build returned `BUILD_RC=0` and ran the C++ builder's CTest:

```text
100% tests passed, 0 tests failed out of 3
```

The image-local verifier returned `VERIFY_RC=0` and verified:

```text
engine path: /usr/local/bin/velox_video_engine
engine SHA-256: 6df2c0442d4bbdf4a4592414a25e306607f1ac0136b02d5bb26b3cc7152e9599
```

The image reports the intended non-root `velox` user and canonical engine
labels, including `org.veloxproject.engine.build=canonical-cpp-builder` and
`org.veloxproject.engine.runtime=debian:bookworm-slim`. The Dockerfile uses a
pinned Debian Bookworm digest for C++ builder and runtime; the successful build
therefore exercised the intended same-base configuration, but no independent
registry provenance or signature was available.

## Reproducibility check

A second build used the same Dockerfile and the same `SOURCE_DATE_EPOCH`:

```text
Second tag: velox-worker:canonical-audio-verify-repeat
Second image ID: sha256:2f88a0ef7d3ba17b87cf7be5aa724df2967281370640b2f774d8208b32898d7d
First engine SHA: 6df2c0442d4bbdf4a4592414a25e306607f1ac0136b02d5bb26b3cc7152e9599
Second engine SHA: 6df2c0442d4bbdf4a4592414a25e306607f1ac0136b02d5bb26b3cc7152e9599
```

Result:

- engine bytes: **identical**;
- full image IDs: **different**;
- image reproducibility: **NOT CERTIFIED**.

The difference must be investigated before claiming a reproducible release
image. No tag was pushed and no image was deleted as part of this verification.

## Signing and immutable canary status

The environment inspection found:

- Docker daemon: available, version 29.1.3;
- `ffmpeg`, `ffprobe`, CMake and C++ compiler: available;
- `docker buildx`: unavailable;
- `cosign`: unavailable;
- local images: local IDs only, with empty `RepoDigests`.

The canonical script `scripts/ci/worker-audio-canary.sh` requires a published
reference in the form:

```text
registry/repository@sha256:<64-hex>
```

It also pulls and verifies that exact RepoDigest. Because the locally built
image has no RepoDigest and no registry/signing target was supplied, the
published canary is **BLOCKED**, not skipped silently and not replaced by a
mutable tag.

An attempted inline local smoke wrapper did not emit reliable result evidence
(the generated JSON was subject to shell-quoting issues), so it is not counted
as a canary PASS. The native C++ loop test and Docker builder CTest remain the
reliable loop evidence for this milestone.

## Required follow-up

Before promotion, an operator must:

1. make BuildKit/buildx available or document the approved legacy-builder
   reproducibility controls;
2. identify and eliminate the source of differing full image IDs despite the
   identical engine bytes and source epoch;
3. rebuild and compare full image IDs again;
4. publish the verified image to the approved registry;
5. install/use the approved cosign workflow and verify the exact
   `registry@sha256` reference;
6. run `scripts/ci/worker-audio-canary.sh` against that immutable signed digest;
7. record the canary duration, engine SHA, image digest and zero residual
   ffmpeg processes before promotion.

Until then, the image is locally built and engine-verified but **not a
reproducible, signed, digest-pinned release candidate**.
