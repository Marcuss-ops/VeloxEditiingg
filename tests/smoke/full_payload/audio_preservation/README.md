# tests/smoke/full_payload/audio_preservation/

Smoke matrix that proves the worker retains **both audio streams** in the
narrated-clip path: a `voiceover` lane (the original VO asset) AND a
`scene_clip_audio` lane (the clip's original audio preserved after the
voiceover bed).

Source-of-truth for the dual-track contract:

- `DataServer/internal/jobs/enqueue/narrated_clip_timeline.go` —
  emits `audio_tracks[]` with `role:voiceover` + `role:scene_clip_audio`
  lanes for each scene that has BOTH a voiceover binding and an explicit
  (or probe-derived) voiceover/clip duration.
- `RemoteCodex/native/video-engine-cpp/src/services/media_utils.cpp::muxAudio`
  — muxes both feeds into the final MP4 with aligned PTS baseline.
- `tests/worker-cert/verify_artifact.sh` — canonical ffprobe call pattern
  for the worker-cert smokes.

## Files

- `fixtures/dual_audio_scene.json` — single-scene SubmitJobRequest payload.
  Uses `asset-recording-001` (voiceover) + `opening-clip-01` (clip) from
  `tests/worker-cert/fixtures/assets.json` (DO NOT introduce new asset_ids
  here). Explicit `voiceover_duration_seconds` + `final_clip_duration_seconds`
  so the worker doesn't have to probe audio files at submit time.
- `check_audio_streams.sh <path-to-rendered.mp4>` — pure ffprobe verifier.
  Asserts:
  1. exactly 2 audio streams (`codec_type=audio` count=2)
  2. both start within ±100ms of each other (`sync_drift ≤ 0.10s`)
  3. `format.duration > 0` (the MP4 has muxed content)
  Prints `OK: dual-audio preserved -> audio_streams=2 codec_1=... codec_2=...`
  on PASS.

## Usage — offline verification

```sh
# After a render is in hand (e.g. downloaded from the artifact_url in
# tests/smoke/full_payload/evidence/run-*.json), verify the dual-track:
bash tests/smoke/full_payload/audio_preservation/check_audio_streams.sh \
  /path/to/rendered.mp4
```

Exit codes:

- `0`  PASS — both audio streams present + sync drift ≤ 0.10s + non-empty MP4.
- `1`  FAIL — count / sync / duration assertion failed. See stderr.
- `2`  usage / env — missing arg, missing input, missing ffprobe.

## Triggering a fresh render

A three-step operator loop (assumes a running master + admin token):

```sh
# 1. Submit:
bash tests/smoke/full_payload/run.sh   # runs the 4-scene scenario.json
                                       # (delegate to a 1-scene submit when needed)
# 2. Download the artifact from the artifact_url in evidence/run-*.json.
# 3. Verify:
bash tests/smoke/full_payload/audio_preservation/check_audio_streams.sh /path/to.mp4
```

## Cross-references

- The dual-track contract is asserted at the unit level by
  `DataServer/internal/jobs/enqueue/narrated_clip_timeline_test.go` —
  `TestBuildNarratedClipPayload_NestedVoiceoverUsesClipPlusVoiceoverDuration`
  confirms a scene with nested VO + clip emits two audio tracks: the
  voiceover at offset=0 and the scene_clip_audio at offset=voiceoverDuration.
- The same contract is asserted at the integration level by
  `DataServer/internal/jobs/enqueue/clip_input_normalizer_test.go` line 130
  ("audio_tracks = 2; want 2 (voiceover + scene_clip_audio for 1 scene)").
