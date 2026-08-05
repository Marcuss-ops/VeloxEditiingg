# Canonical worker audio engine — milestone 1

## Status

**PASS — local C++ engine build and loop regression test.**

This milestone verifies the engine source and its deterministic local test
before attempting the canonical worker image build. No host-built binary was
copied into a worker image, no registry push was performed, and no live worker
was changed.

## Repository/runtime context

```text
Branch: main
Preflight HEAD == origin/main: 4e6a216ee4eb5194d58e5e63eb193238e0e8e8b6
```

The working tree contained unrelated pre-existing modifications and untracked
files. They were excluded from this report and remain untouched.

## Local build evidence

Command:

```bash
BUILD=/tmp/velox-engine-verify-1785934012-998617
cmake -S RemoteCodex/native/video-engine-cpp -B "$BUILD" -DCMAKE_BUILD_TYPE=Release
cmake --build "$BUILD" --parallel 2
ctest --test-dir "$BUILD" --output-on-failure
```

Result:

```text
100% tests passed, 0 tests failed out of 3
```

Passed targets:

- `ffmpeg_progress_parser_tests`
- `phase_recorder_tests`
- `looped_music_tests`

The temporary build directory was outside the repository; no generated build
files were added to the working tree.

## Loop contract verified

`looped_music_tests` passed the native-engine regression scenario:

- generated source music: 30 seconds;
- rendered timeline: 95 seconds;
- the native plan omitted an explicit audio duration (zero/default field);
- output duration constrained to approximately 95 seconds;
- synchronous render completion;
- no residual ffmpeg process associated with the test output.

The payload/compiler JSON path was not executed by this local C++ test. Source
inspection shows the intended two-defense design, but that claim remains
unverified until the canonical image build and payload-level canary run:

1. the payload/plan path should supply the final timeline duration when loop
audio has no explicit duration;
2. the C++ filter graph trims looped audio and applies `-t` to the final audio
mix, preventing an unbounded `-stream_loop -1` graph.

## Canonical image prerequisites observed

Inspection shows that the repository Dockerfile is structured as a
self-sufficient multistage build:

- a pinned Debian Bookworm digest is referenced by both C++ builder and runtime;
- the Dockerfile intends to build and test the C++ engine inside `cpp-builder`;
- the Go worker is built inside `go-builder`;
- runtime copies only builder outputs and runtime dependencies;
- runtime stores `video-engine.sha256` and derives `BUNDLE_HASH.txt`;
- image labels identify canonical C++ builder/runtime provenance.

These are Dockerfile properties only at this milestone; no Docker build was
completed here, so same-base runtime, in-image C++ tests, and final image
reproducibility remain unverified.

The local environment reported:

- Docker daemon available: Docker 29.1.3;
- `ffmpeg`, `ffprobe`, CMake and C++ compiler available;
- Docker Buildx command unavailable;
- `cosign` unavailable;
- approximately 9.1 GB free on the root filesystem;
- existing local worker images have local image IDs but no registry
  `RepoDigests`.

Consequently, image signing, registry digest, and immutable published-image
canary are not yet verified by this milestone.
