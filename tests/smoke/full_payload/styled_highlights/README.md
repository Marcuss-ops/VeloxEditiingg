# tests/smoke/full_payload/styled_highlights/

Smoke matrix that proves the **libass burn-in path preserves per-Dialogue
style attributes** (font size + colour + position + bold/italic) across
3 distinct ASS events rendered in non-overlapping PTS windows. Closes
`docs/operations/04-veloxediting-final-smoke-checklist.md` line items for:

* `frase importante` (important phrase) — large red bold at top
* `nome speciale` (special proper name) — medium cyan regular at middle
* `parola evidenziata` (highlighted word) — medium green italic at bottom

## Layout

```
tests/smoke/full_payload/styled_highlights/
├── README.md                            ; this file
├── check_styled_highlights.sh           ; bash verifier (sources tests/_lib/sh/_lib.sh)
└── fixtures/
    └── styled_highlights.ass            ; ASS source: 3 styled Dialogue events
```

## The fixture

Single ASS file. ScriptType: v4.00+. PlayResX=1280, PlayResY=720. Each
Dialogue uses `\pos(x,y)` to anchor the text in a different quadrant
(bottom-centre anchor ⇒ text extends UP from the y coordinate). Inline
override tags force colour/bold/italic at the Dialogue level so default
Style values become irrelevant to per-event appearance.

| Event | Text | Quadrant | y-extent | `\\fs` | Inline colour | Style |
|---|---|---|---|---|---|---|
| 1 | `frase importante` | TOP | y ≈ 24..120 | 96 | `\\c&H0000FF&` (red) | `\\b1` (bold) |
| 2 | `Marco Aurelio` | MIDDLE | y ≈ 296..360 | 64 | `\\c&HFFFF00&` (cyan) | (default — Style.Bold=-1 still applies) |
| 3 | `_HIGHLIGHTED_` | BOTTOM | y ≈ 520..600 | 80 | `\\c&H00FF00&` (green) | `\\i1` (italic) |

PT windows (2-digit centiseconds per Finding A in
`docs/smoke-results/2026-07-28-subtitle-sync.md`):

- event 1: `0:00:00.10` → `0:00:00.80`
- event 2: `0:00:00.90` → `0:00:01.60`
- event 3: `0:00:01.70` → `0:00:02.40`

Background voiceover is a 440 Hz sine 3.0s (lavfi), muxed alongside the
subtitle. format.start_time = 0.000 (chronology preserved).

## Verification tiers

| Tier | What it gates | Verdict |
|---|---|---|
| 1 | Burn-in contract: ffprobe `video=1 audio=1 subtitle=0`; `format.start_time ≈ 0`. | Always required; failure here short-circuits. |
| 2 | Per event, painted quadrant has `stddev > 5` and the **other two quadrants** have `stddev < 1`. | Proves POSITION correctness — no cross-quadrant bleed. |
| 3 | Per event, channel-dominance on bg-removed positive-deviation sums (`rdev`, `gdev`, `bdev` over painted pixels): red expects `rdev > 1.5·max(gdev, bdev)`; cyan expects `gdev > 1.5·rdev AND bdev > 1.5·rdev AND |gdev-bdev|/max < 0.30`; green expects `gdev > 1.5·max(rdev, bdev)`. | Proves COLOUR correctness. |
| 4 | Per event, painted-pixel footprint within the bracket for the expected font size at 1280×720 Arial: 96 px ≥ 1500; 64 px ≥ 700; 80 px ≥ 1100. | Proves SIZE correctness. |

Per-event verdict strings:

- `PASS` — T2 + T3 + T4 all green on that event.
- `FAIL_T2` / `FAIL_T3` / `FAIL_T4` — first failing tier (event skipped downstream).
- The global verdict is `PASS` iff all 3 events are individual-PASS.

### Why `Y > 128` is NOT used for "painted"

`compute_strip_stats` exposes two distinct counts per quadrant:

- `bright_pct` — fraction of pixels with luminance Y > 128 (used for sibling's
  Tier 4 "is there TEXT?" gate in `tests/smoke/full_payload/subtitle_special_chars/`).
- `painted_px` — fraction of pixels where `max(|r-bgr|, |g-bgg|, |b-bgb|) > 50`.
  Operates on raw RGB, NOT luminance, so it catches pure-red (Y=76) pixels
  that fail the Y > 128 threshold.

The colour-signature analysis (T3) further tracks bg-removed positive-deviation
sums `rdev / gdev / bdev` so that antialiasing fringe pixels leaking bg-blue
into the B channel do not pollute red/green ratio dominance.

### Why position uses quadrant-strip-stats × 3

Each event lives in EXACTLY ONE of the 3 vertical quadrants (1/3 of frame
height, = 240 px). The verifier samples the centre frame of each event
window and runs `compute_strip_stats` on each of the 3 quadrants. PASS
iff the painted quadrant has `stddev > 5` AND the inactive two quadrants
have `max stddev < 1`. Cross-quadrant bleed is the failure mode this
catches (e.g. dialog text spilling from y ≈ 360 into y ≈ 480 because of
font ascent above the y anchor point).

## Reference frames (F2) and reference signatures (F3)

For each run the verifier writes:

- `$JOB_DIR/stills/event{N}_first_painted.png` — first extracted frame whose
  PTS is `>=` event's PTS Start. Deterministic given the fixture + libass
  binary + fontconfig cache on the host. For operator eyeball + future
  `compare -metric AE /compare -metric PAE` regressions.
- `$JOB_DIR/stills/event{N}_centre.png` — extracted frame at the centre PTS
  of the event window. The strip-stats and bg-removed sums are computed
  from this frame.

For each run the verifier ALSO writes:

- `$JOB_DIR/event_results.json` — per-event array of measured
  `position.{painted_stddev, inactive_max_stddev, tier2_pass}`,
  `color.{rsum, gsum, bsum, rdev, gdev, bdev, tier3_pass}`,
  `size.{painted_px, min_footprint, tier4_pass}`,
  `verdict`, `stills.{first_painted, centre}`.
- `$JOB_DIR/run_summary.json` — schema `velox.smoke.styled-highlights@1`,
  contains `expected[]`, `measured[]`, `summary{events_total, events_pass,
  events_fail, verdict}`, `evidence{...}`.

Compare two runs to detect regression:

```sh
# diff reference signatures across runs
diff -u run1/run_summary.json run2/run_summary.json | head
# eyeball diff of painted frames
compare -metric AE run1/stills/event1_first_painted.png \
                 run2/stills/event1_first_painted.png /tmp/diff.png
```

## How to invoke

```sh
# Default: per-run UTC audit dir under /tmp/velox-styled-highlights/$UTC
bash tests/smoke/full_payload/styled_highlights/check_styled_highlights.sh

# Stable audit dir (compare runs side-by-side)
JOB_DIR=/tmp/velox-styled-highlights/baseline bash tests/smoke/full_payload/styled_highlights/check_styled_highlights.sh

# Help
bash tests/smoke/full_payload/styled_highlights/check_styled_highlights.sh --help
```

## Source-of-truth chain

```
SubmitJobRequest with voiceover binding + subtitle_tracks[] (ASS)
  → DataServer/internal/jobs/enqueue/normalize.go
  → RemoteCodex/native/worker-agent-go/pkg/video/pipelines/hybrid/compiler.go
  → RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp:279
       `burnSubtitleTrack` (lines 77–83)
       `filter << "subtitles=" << file::shellQuote(...);`
  → local libass burn-in (mirrors the engine's ffmpeg filtergraph)
```

The verifier reproduces the same `subtitles=FILE` filtergraph that the
engine builds, plus a voiceover mux so the dual-track chronology is
cross-checked (Tier 1b reserved).

## Cross-references

- The audio-side counterpart (`tests/smoke/full_payload/audio_preservation/`)
  asserts `codec_type=audio count == 2` for the dual-audio contract.
- The character-rendering verifier
  (`tests/smoke/full_payload/subtitle_special_chars/`) validates which
  characters render correctly; this smoke validates that the style
  attributes of any text render correctly relative to its inline `\c / \fs / \pos`
  directive overrides.
- The sync verifier (`tests/smoke/full_payload/subtitle_sync/`) measures PTS
  drift; this smoke measures style preservation on top of the same
  burn-in chain.
- Prior design findings (`docs/smoke-results/2026-07-28-subtitle-sync.md`):
  *Finding A (cents vs ms)* — fixture uses 2-digit centisecond format.
  *Finding B (alpha=00 transparent)* — Style.PrimaryColour is `&HFFFFFFFF`
  (alpha=FF opaque); inline `\c&HBBGGRR&` overrides the BGR portion keeping
  alpha=FF so the colour gate doesn't false-positive against alpha-0 trap.

## Local-only rationale

Same as subtitle_sync and subtitle_special_chars: the master-routed smoke
path requires `tests/worker-cert/fixtures/assets.json` rows for these
synthetic events. The local execution isolates the style-attribute
verification from the asset-upload chain and exercises the same
ffmpeg/libass binary the worker image ships.
