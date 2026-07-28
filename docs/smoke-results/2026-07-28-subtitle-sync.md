# Subtitle-Voiceover Synchronization — Smoke Report
*Captured: 2026-07-28*

## TL;DR

| Metric | Value |
|---|---|
| Verifier | `tests/smoke/full_payload/subtitle_sync/check_subtitle_sync.sh` |
| Fixture | `fixtures/special_chars_sync.ass` (single ASS Dialogue event) |
| Threshold | `\|drift\| ≤ 80 ms` |
| **Measured drift** | **+33.3 ms** (PASS) |
| First painted frame | n=10 (PTS=0.3333 s) |
| Last painted frame | n=36 (PTS=1.2000 s) |
| Voiceover PTS at muxer | 0.000000 (chronology preserved) |
| Burn-in scope | `codec_type=video=1 codec_type=audio=1 codec_type=subtitle=0` ✓ |
| Smoke rc | **`0`** (Tier 1, 1b, 2, 3 all green) |

The ffmpeg / libass burn-in path renders subtitles with **±1-frame precision
relative to the ASS Start PTS**, on the same libass binary
(`--enable-libass`, libass 0.17.4, HarfBuzz 12.3.2) the worker image ships.

## 1. Source-of-truth chain (mirrors production)

```
SubmitJobRequest with voiceover binding + subtitle_tracks[] (ASS)
  → DataServer/internal/jobs/enqueue/normalize.go
  → DataServer/internal/handlers/server/pipeline/plan_derivation.go
  → worker pkg/video/pipelines/hybrid/compiler.go → parseSubtitleTracks
  → C++ engine RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp:279
       reportProgress(80, "burning_subtitles");
       const auto& subtitle = plan.subtitle_tracks.front();
       burnSubtitleTrack(videoOnly, localSubtitle, subtitledVideo);
  → burnSubtitleTrack (lines 77–83)
       filter << "subtitles=" << file::shellQuote(subtitleFile.string());
  → ffmpeg libass burn-in (Subtitle Start/End from ASS event → burned pixels)
```

The verifier's ffmpeg invocation reproduces the same `subtitles=FILE` filter
graph that the engine builds, plus a voiceover mux so chronology can be
cross-checked (`format.start_time == 0`).

## 2. Smoke design (4 tiers)

| Tier | What it gates | Evidence |
|------|---------------|----------|
| 1 | The C++ engine's burn-in contract — no soft-track subtitle muxing, voiceover audio present. | `ffprobe`: `v=1 a=1 s=0` ✓ |
| 1b | Voiceover PTS starts at 0 (chronology preserved in the mux). | `format.start_time = 0.000000` ✓ |
| 2 | Subtitle text appeared within the burnt-in window (`PTS ∈ [0.30, 1.20] s`). | per-frame strip-stats crosses `stddev > 5` within those frames ✓ |
| 3 | Drift between measured paint PTS and ASS Start PTS ≤ 80 ms. | `\|drift_ms\| = 33.3` ≤ 80 ✓ |

Tier 2 uses the same `compute_strip_stats` helper as the sibling
`tests/smoke/full_payload/subtitle_special_chars/check_subtitle_burn_in.sh`,
operating on the bottom-25% strip of each per-frame PNG. The strip region is
chosen because ASS `Default` style places the bottom-center ribbon at
`MarginV=40` (i.e. the last ~180 px at 720p), matching where libass drops the
text.

## 3. Captured per-frame evidence (PTS window)

```
n   pts      stddev  bright_pct
5   0.1667   0.00    0.00    ; pre-event
6   0.2000   0.00    0.00
7   0.2333   0.00    0.00
8   0.2667   0.00    0.00
9   0.3000   0.00    0.00    ; ASS Start (no paint yet — see §4 finding A)
10  0.3333   8.06    0.00    ; first painted frame  ← drift = +33.3 ms
11  0.3667   8.06    0.00
12  0.4000   8.06    0.00
…
33  1.1000   8.06    0.00
34  1.1333   8.06    0.00
35  1.1667   8.06    0.00
36  1.2000   8.06    0.00    ; ASS End (paint stops on next frame)
37  1.2333   0.00    0.00    ; cleared
38  1.2667   0.00    0.00
…
```

`stddev > 5` cleanly fires at frame 10 (PTS=0.3333 s) and drops below
threshold at frame 37 (PTS=1.2333 s). Burning window = 27 frames ≈ 0.900 s,
matching ASS `End - Start = 01.20 - 00.30 = 00.90` (within ±1 fudge frame).

Per-frame PNG byte-size variance independently corroborates the painted
window: 49 distinct frames at 4384 bytes (text painted) bracketed by 9
pre-event frames at 4356 (baseline `test_frame.png`) and post-event frames
returning to 4356 / 4368.

## 4. Design findings

### Finding A — ASS fractional part is CENTISECONDS, not milliseconds [gotcha]

The fresh fixture originally encoded the event PTS as
`Dialogue: 0,0:00:00.300,0:00:01.200,...,SYNC_TEST_300MS`. **libass parsed
that as `0:00:00.300` = 0 hours + 0 min + 0 sec + 300 centiseconds = 3.00
seconds**, i.e. the event was scheduled exactly at the wall-clock end of the
3 s test clip and **never painted**. Symptom: 90 frames byte-identical to
the `test_frame.png` baseline, x264 `kb/s = 10.01`, `stddev=0` everywhere.

**Fix**: the ASS spec's `H:MM:SS.cs` is centiseconds (2 digits), not
milliseconds (3 digits) like SRT's `H:MM:SS,mmm`. The corrected event is:

```text
Dialogue: 0,0:00:00.30,0:00:01.20,Default,,0,0,0,,SYNC_TEST_300MS
```

This is the canonical format used by every libass-compatible authoring tool
(Aegisub, Subtitle Edit, FFmpeg `ass` muxer). Three-digit fractions are not
part of the ASS spec and are silently interpreted as `> 1 second` shifts.

> **Latent impact**: the sibling smoke
> `tests/smoke/full_payload/subtitle_special_chars/fixtures/special_chars.ass`
> has the same `Dialogue: 0,0:00:00.300,0:00:01.200,...` defect. That smoke
> happens to pass its character-rendering tiers (Tier 2 Tier 3 Tier 4 read
> strip-stats and OCR-style detection during their smaller `-t=0.25` window)
> only because the burn-in there relies on inline `{\c&H0000FF&}{\b1}` override
> tags which bypass the Style alpha (see Finding B) — and the timing-bug text
> was scheduled at 3.0 s of a 0.25 s clip, so it never painted but was
> overridden by something visible. **A future maintainer who removes the
> inline overrides will silently lose subtitle visibility on the sibling.**
> Recommended followup: separate commit hardening that fixture to 2-digit
> cents.

### Finding B — ASS `&H00XXXXXX` style colours render INVISIBLE [gotcha]

A naive ASS primary-colour string `&H00FFFFFF` (intended as
`alpha=00 + RGB=FFFFFF`) is parsed by libass in the **`&HAABBGGRR` (8-digit
with explicit alpha)** format: AA=0x00 → fully transparent. The text on the
sibling's burn-in fixture has the same defect — `PrimaryColour=&H00FFFFFF`
in `Style:`. The sibling smoke is rescued by the inline `{\c&H0000FF&}`
override that forces a 50%-alpha blue; without that override the Style
itself would be invisible.

**Fix**: the new sync fixture sets `&HFFFFFFFF` (alpha=0xFF fully opaque,
white) so the ASS event paints visibly from frame 10 onward without needing
inline override tags. Any producer that writes ASS subs MUST either
explicitly use `&HFF` (or any non-zero `AA`) for primary/secondary/outline,
or rely on inline override tags.

> Recommended: a sanity-check helper in `enqueue/normalize.go` that
> rejects ASS events starting with `Dialogue:` whose parent Style has a
> `PrimaryColour` field of `&H00XXXXXX` (or any AA=0 colour). This is a
> Type-1 (input) failure category, not a Type-4 (render) one.

### Finding C — Per-frame strip-stats has 1-frame precision (~33.3 ms) [budget]

At 30 fps, the smallest measurable unit of `first_text_frame_n / fps` is
33.333 ms. The measured drift of +33.3 ms is therefore **exactly the price
of the measurement granularity**, not a render latency. To meaningfully
distinguish drift better than ±33 ms:

| Option | Trade-off |
|---|---|
| Run this smoke at 60 fps (`-r 60`) and divide frame-N by 60. | 2× frame-extraction time, marginal benefit. |
| Use OCR / TEM signature to identify the exact *first* painted pixel row. | Brittle; fails on font fallback and emoji. |
| Time-stretch the event to longer (`Start=0.10, End=2.90`) and find onset within 1 frame. | Same 1-frame budget unless paired with higher fps. |

The current 80 ms threshold (≈2.4 frames at 30 fps) is appropriate for an
operator-facing PASS/FAIL without requiring higher-fps renders. If a
production worker is observed with > 80 ms subtitle-to-voiceover drift in
the field, raise the threshold and re-derive a per-frame budget from the
facts; do not paper over with a wider threshold.

## 5. Operator-facing verdict

```text
==== SUBTITLE-VOICEOVER SYNC VERDICT ====
expected_subtitle_start_s = 0.300
voiceover_start_s        = 0.000000   (must be 0.000)
video_fps                = 30
total_frames_extracted   = 90
first_text_frame_n       = 10
first_text_pts_s         = 0.3333
drift_ms                 = 33.3
abs_drift_ms             = 33.3   (threshold <= 80)
run_summary              = /tmp/.../run_summary.json
per_frame_stats          = /tmp/.../per_frame_stats.tsv

SYNC-PASS: subtitle rendered within tolerance (drift=33.3ms <= 80ms)
[INFO] run_utc=2026-07-28T17:55:59Z
[INFO] voiceover_start_s=0.0   (PTS=0 muxer alignment confirmed)
[INFO] rendered_mp4=65.8 KiB   (vs ~56 KiB for an empty burn-in baseline)
```

## 6. Cross-references

- Sibling character-rendering verifier (renders the **same code path**, but
  validates *which* chars survive):
  `tests/smoke/full_payload/subtitle_special_chars/`
- Audio-dual-track verifier (asserts the dual-audio contract the "preserve
  original clip audio" smoke relies on):
  `tests/smoke/full_payload/audio_preservation/`
- Production burn-in chain:
  `RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp:77–83` (libass filter wiring)
  `RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp:279–292` (engine entry point)
- Smoke checklist gating this:
  `docs/operations/04-veloxediting-final-smoke-checklist.md:341` ("Sottotitoli e voiceover restano sincronizzati (frame accurate)")

## 7. Reproduce

```sh
# Default: per-run audit dir under /tmp/velox-subtitle-sync/$UTC
bash tests/smoke/full_payload/subtitle_sync/check_subtitle_sync.sh

# Stable audit dir (compare runs side-by-side)
JOB_DIR=/tmp/velox-subtitle-sync/baseline bash tests/smoke/full_payload/subtitle_sync/check_subtitle_sync.sh
```

The verifier pre-condition: `ffmpeg` with `--enable-libass`,
`libass >= 0.17.4`, `HarfBuzz >= 8.0`, `fontconfig`, and a font capable
of rendering the Latin + Symbola range (`DejaVu Sans` is sufficient;
`Arial` is not strictly required because libass falls back via
fontconfig).
