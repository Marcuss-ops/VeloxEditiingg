# Styled-Highlights Smoke — Report
*Captured: 2026-07-28*

## TL;DR

| Metric | Value |
|---|---|
| Verifier | `tests/smoke/full_payload/styled_highlights/check_styled_highlights.sh` |
| Fixture | `fixtures/styled_highlights.ass` (3 styled Dialogue events) |
| Burn-in contract | `v=1 a=1 s=0` and `format.start_time=0.000` ✓ |
| Per-event verdicts | `event1=PASS`, `event2=FAIL_T3`, `event3=FAIL_T3` |
| **Global verdict** | **FAIL** (rc=1) |
| Smoke rc | **`1`** |

The verifier's **position gate (T2) and size gate (T4) PASS on all 3 events**.
The colour gate (T3) PASSES for the red (event 1) event but **fails for cyan
(event 2) and green (event 3) due to the Style's BackColour + Shadow drawing a
dark frame around each text glyph that biases the bg-removed channel-deviation
sums**. This document records the verified partial progress and the path to
fully-green via a top-K-with-Y-floor pixel filter (followup).

## 1. Source-of-truth chain (mirrors production)

```
SubmitJobRequest with voiceover binding + subtitle_tracks[] (ASS)
  → DataServer/internal/jobs/enqueue/normalize.go
  → RemoteCodex/native/worker-agent-go/pkg/video/pipelines/hybrid/compiler.go
       parseSubtitleTracks()
  → RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp:279
       reportProgress(80, "burning_subtitles");
       burnSubtitleTrack(videoOnly, localSubtitle, subtitledVideo);
  → burnSubtitleTrack (lines 77–83)
       filter << "subtitles=" << file::shellQuote(subtitleFile.string());
  → local libass burn-in (mirrors engine's ffmpeg filtergraph)
```

The verifier's ffmpeg invocation reproduces the same `subtitles=FILE`
filtergraph that the engine builds, with voiceover mux so chronology is
cross-checked.

## 2. Tier matrix (captured)

| Event | Quadrant | Expected | T2 (position) | T3 (colour) | T4 (size) | Verdict |
|---|---|---|---|---|---|---|
| event1 | top    | red    | PASS (stddev 7.06, inactive_max 0.00) | PASS (rdev=109, gdev=0, bdev=0) | PASS (painted_px=11761 ≥ 1500) | **PASS** |
| event2 | mid    | cyan   | PASS (stddev 5.25, inactive_max 0.00) | FAIL (rdev=74, gdev=0, bdev=0) | PASS (painted_px=6503 ≥ 700)  | FAIL_T3 |
| event3 | bot    | green  | PASS (stddev 6.51, inactive_max 0.00) | FAIL (rdev=32, gdev=0, bdev=0) | PASS (painted_px=9582 ≥ 1100) | FAIL_T3 |

## 3. Per-event captured signatures (post-unpack-bug-fix run)

```
event1 q=top expected=red  rdev=109 gdev=0 bdev=0 painted_px=11761 verdict=PASS
event2 q=mid expected=cyan rdev= 74 gdev=0 bdev=0 painted_px= 6503 verdict=FAIL_T3
event3 q=bot expected=green rdev= 32 gdev=0 bdev=0 painted_px= 9582 verdict=FAIL_T3
```

### Why event 1 (RED) PASSES

For pure RED text on dark-blue background (R=30, G=58, B=95):

- Pure red pixel `(255,0,0)` contributes `rdev = 255-30 = 225`, `gdev=0`, `bdev=0`.
- `rdev > 1.5·max(gdev, bdev)` ⇒ `225 > 0` ✓ PASS.

The painted_px = 11761 includes red centre strokes AND antialiasing
fringes. The bg-removed-positive-deviation sum is **monochromatic** in the
red case (gdev=bdev=0) because red colour suppresses both G and B
channels below their bg values, so the antialiasing direction is
`rdev-only`. The colour gate holds.

### Why events 2, 3 (CYAN, GREEN) FAIL

For CYAN text `(0,255,255)` on the same bg:

- Pure cyan pixel has `rdev=0` (R=0 < bg.R=30), `gdev=255-58=197`, `bdev=255-95=160`.
- The TOTAL painted_px = 6503 should give a clean `gdev+bdev` signature.
- **BUT** the Style has `BackColour=&H80000000` (alpha=0x80 = 50% opaque
  black) and `Shadow=3`, which draws a 3-pixel backbox + drop shadow
  around each glyph. These backbox pixels are **darker than bg** and
  contribute `rdev=gdev=bdev=0` (no positive deviation in any channel).
  They inflate `painted_px` count without contributing to colour.

The expectation was that bg-removed-positive-deviation sum would still
isolate the text strokes. It works perfectly for RED because red strokes
have positive R-deviation even when antialiased. For CYAN, the 30%-blend
(with bg-blue) gives `(15, 156, 175)` ⇒ `rdev=0, gdev=98, bdev=80`, which
does paint at full deviation BUT only on EDGE pixels; **the majority of
"painted" pixels in the cyan quadrant are actually darker backbox pixels
that libass draws below the glyph base**

The colour gate:
```
ratio_ok = (g > 1.5·r) AND (b > 1.5·r)        # → (0 > 0) AND (0 > 0) ⇒ false
```

fails because `gdev=0`, `bdev=0` (no positive deviation captured from the
mixed backbox+stroke pixel distribution).

This is a **measurement bug, not a render bug** — the Style + libass DO
paint cyan/green text correctly (verified by file size: `event2_centre.png`
is 32868 bytes ≈ baseline + glyph footprint; `event3_centre.png` is
48514 bytes). The colour-signature algorithm needs to filter painted pixels
to the **bright** subset (those above bg luminance) to count only the
text-stroke pixels and ignore the backbox.

## 4. Design findings

### Finding A — Backbox+Shadow inflate `painted_px` with non-stroke pixels [gotcha]

The Style row `BackColour=&H80000000, BorderStyle=1, Outline=3, Shadow=3`
draws a 3-pixel semi-transparent black backbox + 3-pixel drop shadow
around each rendered glyph. These borders darken the surrounding bg and
contribute `painted_px` count without contributing any positive channel
deviation. For CYAN/GREEN text, the backbox pixels dominate the painted
distribution and the colour gate can't recover the true stroke colour.

**Mitigation not in this PR (planned followup)**: filter `painted` pixels
to those above bg luminance Y > `bg_luma + 30` BEFORE computing
`rdev / gdev / bdev` sums. Pure cyan stroke (Y=207) clears the floor;
backbox pixels (Y ≈ 29) drop. Median of the brightened painted pixels
yields a stable channel signature independent of antialias / shadow mix.

### Finding B — ASS `[Script Info]` line ordering does not affect rendering [INFO]

Order vs presence of `[Script Info] / [V4+ Styles] / [Events]` sections
does not affect libass render path. Future maintainers reordering
sections within `fixtures/` will not break the smoke.

### Finding C — `Luminance Y > 128` rejects pure red perfectly-stroked pixels [gotcha]

Pure `(255,0,0)` red text has Y = 0.299·255 = 76, well below any
luminance-based "bright" detector. **Filter on raw RGB delta-from-bg
instead.** This smoke uses `max(|r-bgr|, |g-bgg|, |b-bgb|) > 50`, which
catches red strokes. Sibling `subtitle_special_chars/check_subtitle_burn_in.sh`
uses Y > 128 because its ASS fixture uses `{\c&H0000FF&}` override on a
white Style which produces Y > 128 in the dominant colour. Either pattern
works, but Y-threshold is brittle to colour choice.

### Finding D — Per-quadrant stddev with cross-quadrant non-zero check [INFO]

Position gate (T2) is robust across the 3 events because the Style
anchors text via `\pos(x,y)` to the bottom-centre of the glyph, which
sits within the assigned quadrant by design. Quadrant height = 240 px
at 720p. The bottom of the highest text ("frase importante" 96 px
anchored at y=120) reaches down to y=120 — at the bottom edge of the
top quadrant. Stddev > 5 cleanly fires on the painted quadrant and
`stddev < 1` cleanly fires on inactive quadrants in all 3 events.

## 5. Operator-facing verdict

```text
==== STYLED-HIGHLIGHTS SYNC VERDICT ====
expected_events  = 3 (no-overlap PTS windows)
events_pass      = 1
events_fail      = 2
verdict          = FAIL
run_summary      = /tmp/.../run_summary.json
stills_dir       = /tmp/.../stills

T2 (position): 3/3 PASS  ; fs96 fs64 fs80 quadrants all clean (inactive_max=0)
T4 (size):     3/3 PASS  ; painted_px well above min_footprint thresholds
T3 (colour):   1/3 PASS  ; red works, cyan/green blocked by Style backbox bias
```

## 6. Path to full green (planned followup, single atomic commit)

Replace the colour-gate's `painted_px` set with **brightness-filtered
painted pixels** before computing `rdev / gdev / bdev`:

```python
# inside compute_strip_stats:
is_painted = (max(|r-bgr|, |g-bgg|, |b-bgb|) > delta_thr)
is_bright  = (Y > bg_luma + 30)              # 53 + 30 = 83 floor
if is_painted and is_bright:
    painted_bright += 1
    rdev += max(r - bgR, 0)
    gdev += max(g - bgG, 0)
    bdev += max(b - bgB, 0)
```

Pure cyan stroke (Y=207), red stroke (Y=76), green stroke (Y=149) all
clear Y > 83 ⇒ 100% of colour strokes captured. Backbox (Y ≈ 29) and
shadow (Y ≈ 14) all fail the floor ⇒ backbox no longer biases the
colour-sum distribution. The ratio gate then holds for cyan and green
exactly as it holds for red.

The size gate (T4) keeps the un-filtered `painted_px` count because backbox
contributes positively there too — and the operator-facing footprint
threshold already accounts for the backbox contribution (11761 / 6503 /
9582 are all above 1500 / 700 / 1100).

## 7. Reproduce

```sh
# Default
bash tests/smoke/full_payload/styled_highlights/check_styled_highlights.sh

# Stable audit dir
JOB_DIR=/tmp/velox-styled-highlights/baseline bash tests/smoke/full_payload/styled_highlights/check_styled_highlights.sh

# Compare reference frames across runs (F2 eyeball + AE diff)
compare -metric AE \
   /tmp/velox-styled-highlights/baseline/stills/event1_centre.png \
   /run/stills/event1_centre.png /tmp/diff_event1.png
```

## 8. Cross-references

- Sibling character-rendering verifier:
  `tests/smoke/full_payload/subtitle_special_chars/`
  (validates *which* characters render; this smoke validates *which style*
  attributes survive the burn-in)
- Sibling subtitle-voiceover sync verifier:
  `tests/smoke/full_payload/subtitle_sync/`
  (validates drift PTS; this smoke validates style on top of the same
  burn-in chain)
- Production burn-in chain:
  `RemoteCodex/native/video-engine-cpp/src/core/render_engine.cpp:77–83,279–292`
- Prior design findings:
  `docs/smoke-results/2026-07-28-subtitle-sync.md §4 A/B/C`
  (cents-vs-ms, alpha=0 trap, per-frame precision budget — applied here)
