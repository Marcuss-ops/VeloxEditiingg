# Phase 1 — Zero-Spawn vs FFmpeg baseline: delta reale sulla pista canonica (2026-08-12)

Riesecuzione di `COPY_ONLY_CANONICAL_5M_V1` con il **backend
zero-spawn LibAV Packet** (plan §23) e confronto con il **baseline
misurato del backend pre-change** (spawn ffmpeg/ffprobe per segmento),
stesso fixture, stessa pista, stesso host, stesso contratto
`FINAL_AUDIO_COPY`.

## Setup

| | |
|---|---|
| Fixture | `COPY_ONLY_CANONICAL_5M_V1` — 24 clip H264 1920×1080 30fps CFR (375 frame × 12.5s) + audio finale AAC 48kHz stereo, 300s, warm cache |
| Pista | generata con `velox-fixture-gen` (spec digest `8dce9a44…`, manifest verificato) |
| Host | benchmark host locale, engine ricompilato a `be1a56b4` |
| Candidate | `velox-benchmark -track-dir … -runs 8` → `evidence/phase1-2026-08-12/run-zero-spawn.json` |
| Baseline | replica misurata del backend pre-change (2× ffprobe + stream-copy per segmento → concat → mux `-c:a copy`), 3 run attraverso il sampler process-group `/proc` (stessa metodologia dell'engine sampler) → `evidence/phase1-2026-08-12/baseline-ffmpeg-replication.json` |

## Verifica target Phase 1 (plan §15) — tutti ✅

| Target | Evidenza (8/8 run) | Esito |
|---|---|---|
| `video_decode_frames == 0` | `media.frames_decoded = 0` ogni run | ✅ |
| `video_encode_frames == 0` | `media.frames = 0` ogni run | ✅ |
| `audio_encode_passes == 0` | `media.encode_passes = 0` ogni run | ✅ |
| `external_ffmpeg_exec == 0` | `process.ffmpeg_exec_count = 0` ogni run | ✅ |
| `external_ffprobe_exec == 0` | `process.ffprobe_exec_count = 0` ogni run | ✅ |
| `external execve == 0` | `process.external_process_count = 0`; un solo processo: l'engine (`engine_spawn_count = 1`) | ✅ |
| `temporary segment/video files == 0` | gate tier-1: 0 violazioni, `io.file_copy_count = 0`, `temp_bytes_written = 0` | ✅ |
| `mux_passes == 1` | esattamente **un** evento `engine.packet_mux` (273ms, `bytes_out = 5.838.907` = size finale) per run | ✅ |
| SHA artefatto deterministico | **1** SHA distinto su 8 run: `324a8ecd9280…` | ✅ |

Gate: `velox-fixture-gate -tier deterministic` 0 violazioni (per-receipt),
`-tier performance` **PASS** (`run-zero-spawn.json`, exit 0).

## Delta BASELINE → CANDIDATE (plan §22)

Report ufficiale `velox-benchmark-compare -base baseline-ffmpeg-replication.json -candidate run-zero-spawn.json`:

```
METRIC                    BASELINE    CANDIDATE    DELTA
  wall p50 (ms)                5320         333    -93.7%
  wall p95 (ms)                5470         359    -93.4%
  external execve                74           0   -100.0%
  read amplification           8.83        2.40    -72.8%
  write amplification          1.33        0.05    -96.5%
  audio encode passes             0           0 n/a (baseline 0)

VERDICT: no regression
```

### Note oneste sui numeri

- **wall**: 333ms p50 per 300s di contenuto = **~900× realtime**. Il
  baseline old-path (5.32s p50) era già stream-copy puro; il guadagno
  viene dall'eliminazione dei 74 processi figli (26 ffmpeg + 48 ffprobe)
  e del loro costo (il report phase-0 del workstream quantificava il
  ~52% di CPU in linker+kernel, puro churn di processo).
- **execve**: 74 → 0, `-100%` — l'invariante architetturale del piano è
  ora misurato, non solo dichiarato. Il baseline replica il flusso
  pre-change (2× ffprobe + stream-copy per segmento + concat + mux) e il
  conteggio è esatto (26 ffmpeg + 48 ffprobe, orchestratore escluso come
  fa l'engine sampler).
- **write amplification**: il valore `0.05` del candidate è il **limite
  noto del sampler `/proc`** su render sub-secondo (la finestra I/O del
  tree sfugge; 0 = "non misurato", mai un 1.0x finto). Il dato stabile e
  engine-declared è **`mux_bytes_written / final_bytes = 5.838.907 /
  5.838.907 = 1.00x`** — identico in tutti gli 8 run — contro 1.32–2.35x
  dell'old path. Il `write_amplification_max: 1.5` pinnato sul fixture
  è rispettato da entrambe le letture.
- **read amplification**: 2.40x stabile (rchar ≈ 14.0MB = doppia lettura
  input per probe+packet read) contro 8.8–9.7x dell'old path. Il piano
  §6 punta a ~1.0–1.4x con l'apertura singola; il gap residuo è il
  ri-aprimento (`input_reopen_count = 27`) — follow-up naturale.
- **SHA**: i due backend producono artefatti byte-diversi (muxer
  diversi: `1cfbfefe…` old vs `324a8ecd…` zero-spawn), ciascuno
  **deterministico al proprio interno**. Il pinning SHA del fixture
  avviene per-backend, dopo il baseline.

## Artefatti

- `evidence/phase1-2026-08-12/run-zero-spawn.json` — run machine-readable (8 osservazioni, receipt completi)
- `evidence/phase1-2026-08-12/baseline-ffmpeg-replication.json` — baseline pre-change misurato (3 osservazioni, provenance documentata nel JSON)

## Metodologia baseline (riproducibile)

Il wrapper misura la process group come fa l'engine sampler (`/proc`:
union dei pid visti, picchi per-pid di rchar/wchar/utime/stime/RSS,
tick 5ms). Script della replica old-path (3 run, 26 temp file/run
lasciati sul lavoro, poi rimossi):

```bash
for i in $(seq 1 24); do
  ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 "clip_$i.mp4"
  ffprobe -v error -select_streams a:0 -show_entries stream=codec_name -of csv=p=0 "clip_$i.mp4"
  ffmpeg -y -hide_banner -loglevel error -i "clip_$i.mp4" -map 0:v:0 -c:v copy -an "seg_$i.mp4"
done
ffmpeg -f concat -safe 0 -i list.txt -c copy video_only.mp4
ffmpeg -i video_only.mp4 -i final_audio.m4a -map 0:v:0 -map 1:a:0 -c:v copy -c:a copy -movflags +faststart out.mp4
```
