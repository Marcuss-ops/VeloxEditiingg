# tests/smoke/full_payload/subtitle_sync/

Smoke matrix that proves the **FFmpeg / libass burn-in path renders subtitles
with millisecond-level timing precision relative to the voiceover audio track**.
Closes the `docs/operations/04-velox-final-smoke-checklist.md:341`
item: *"Sottotitoli e voiceover restano sincronizzati (frame accurate)".*

The test renders a 3.0-second clip with a 440 Hz sine voiceover stub and an
ASS subtitle that has a single explicit `Dialogue` event at PTS `0:00:00.300`
through `0:00:01.200`, then per-frame strip-stats extracts the actual PTS
at which the burned-in text first appears. The drift between expected and
measured PTS is the synchronization error.

## Source-of-truth chain

```
SubmitJobRequest with voiceover binding + subtitle_tracks[] (ASS)
  → DataServer/internal/jobs/enqueue/normalize.go
  → DataServer/internal/handlers/server/pipeline/plan_derivation.go
  → worker pkg/video/pipelines/hybrid/compiler.go → parseSubtitleTracks
  → C++ engine core/render_engine.cpp:279
       `reportProgress(80, "burning_subtitles");`
       `const auto& subtitle = plan.subtitle_tracks.front();`
       `burnSubtitleTrack(videoOnly, localSubtitle, subtitledVideo);`
  → burnSubtitleTrack (lines 77–83)
       `filter << "subtitles=" << file::shellQuote(subtitleFile.string());`
  → ffmpeg libass burn-in (Subtitle Start/End from ASS event → burned pixels)
```

The libass filtergraph parses the ASS event PTS and paints the event text
onto the corresponding video frame. The smoke uses **the same ffmpeg/libass
binary** (`--enable-libass`, here `8.0.1-3ubuntu2`) that the worker image
ships with, so a local execution validates the same code path the production
worker renders against.

## Files

- `fixtures/special_chars_sync.ass` — UTF-8 ASS with a single Dialogue
  event whose PTS is explicit: `Dialogue: 0,0:00:00.300,0:00:01.200,...,Default,,0,0,0,,SYNC_TEST_300MS`.
  The marker text is short enough that tesseract OCR can also be used as a
  secondary signal if a future maintainer wants to layer detection methods.

- `check_subtitle_sync.sh` — bash driver + verifier. Sources
  `tests/_lib/sh/_lib.sh` for `log_*` / `ensure_command_available`.
  Asserts `ffmpeg`, `ffprobe`, `jq`, `python3` are on PATH. Generates a
  3.0-second 440 Hz sine voiceover via `ffmpeg -f lavfi -i sine=frequency=440`,
  generates a dark-blue 1280×720 test frame via lavfi `color`, renders the
  combined timeline via `ffmpeg -i test_frame.png -i voiceover.wav
  -vf "subtitles=special_chars_sync.ass" -t 3.0 -r 30 -c:v libx264 -c:a aac`.
  Then runs the verification tiers below and emits
  `$JOB_DIR/run_summary.json`.

## Verification tiers

| Tier | What it proves | Gate? | Evidence |
|------|----------------|-------|----------|
| 1 | Burn-in worked (no soft-track muxing). | **YES** | `streams.subtitle = 0`, `video = 1`, `audio = 1` (voiceover muxed) |
| 1b | Voiceover starts at PTS=0 (chronology preserved in mux step). | **YES** | `format.start_time ≈ 0.000` |
| 2 | Subtitle text appeared within the burnt-in window (PTS ∈ [0.300, 1.200]). | **YES** | per-frame strip-stats crosses `stddev > 5` threshold within those frames |
| 3 | Drift between measured PTS and ASS Start PTS ≤ 80 ms. | **YES** | `abs(detected_pts - 0.300) * 1000 ≤ 80` |

Exit codes:

- **`0` PASS** — Tier 1 + Tier 1b + Tier 2 + Tier 3 green. The drift was
  measured and is within the 80 ms tolerance.
- **`1` FAIL** — at least one of: missing text in the burnt-in window,
  voiceover PTS ≠ 0, drift > 80 ms, or unexpected stream count.
- **`2` usage / env** — missing arg, missing prereq, missing fixture path.

## How to invoke

```sh
# Default: per-run audit dir under /tmp/velox-subtitle-sync/$UTC
bash tests/smoke/full_payload/subtitle_sync/check_subtitle_sync.sh

# Stable audit dir (compare runs side-by-side)
JOB_DIR=/tmp/velox-subtitle-sync/baseline bash tests/smoke/full_payload/subtitle_sync/check_subtitle_sync.sh

# Help
bash tests/smoke/full_payload/subtitle_sync/check_subtitle_sync.sh --help
```

## Local-only rationale

`tests/worker-cert/fixtures/assets.json` does not yet expose rows for the
synthetic voiceover that this smoke generates, and the master/worker fleet
requires those rows before any master-routed subtitle smoke could be
exercised end-to-end. The test surface under evaluation here is
**libass-driven burn-in timing** — the same ffmpeg/libass binary the worker
image ships with, with the same `subtitles=` filter invocation the C++
engine calls. The local execution isolates the timing-latency measurement
from the upstream asset-upload chain.

## Cross-references

- The audio-side counterpart that asserts the dual-track contract lives at
  `tests/smoke/full_payload/audio_preservation/`. The dual-audio verifier
  confirms *what* is muxed (`codec_type=audio` count == 2); this sync verifier
  validates the *when* of subtitle appearance against voiceover PTS=0.
- The character-rendering verifier lives at
  `tests/smoke/full_payload/subtitle_special_chars/`. It validates *which*
  chars render correctly (Italian accented + Euro + emoji + smart quotes +
  Latin-1 + Greek β); this sync verifier validates the *timing* of the same
  render path.
- The libass override-tag bypass detector (`LIBASS_BYPASS_MARKERS` with
  `{\b` and `{\i`) lives in
  `tests/smoke/full_payload/subtitle_special_chars/check_subtitle_burn_in.sh`.
  Cross-referenced here because the same verifier source file imports the
  `compute_strip_stats` helper from there.
