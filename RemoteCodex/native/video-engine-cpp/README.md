# Velox Video Engine C++

Motore C++ nativo per la composizione video Velox. Sotto-comandi CLI
(eseguibili singolarmente, come dispatchato da `src/main.cpp`):

1. **`--render --plan <path>`** — Percorso canonico: consuma un `RenderPlan`
   JSON (contratto canonico) e produce il video finale.
2. **`--render-frames --input <path> --output <path> [--width W --height H
   --fps N --codec c --preset p --pool N]`** — Renderizza frame con la
   pipeline nativa in-process (LibAV): decode → render → encode su tre
   thread con un pool limitato di AVFrame.
3. **`--help`** — Mostra la guida.

> Nota: il percorso legacy `--full-video` e i sotto-comandi CLI legacy
> (`--download-asset`, `--probe-media`, `--build-scene-segment`,
> `--build-clip-segment`, `--concat-segments`, `--mux-audio`) documentati in
> precedenza non esistono nel binario. `main.cpp` accetta solo
> `--render`, `--render-frames` e `--help`; ogni altro argomento produce un
> errore con exit code 1.

## Struttura

```
video-engine-cpp/
├── src/
│   ├── main.cpp                  # Dispatcher CLI (--render / --render-frames)
│   ├── app/
│   │   └── commands.cpp          # Comando --render-frames
│   ├── core/
│   │   ├── render_engine.cpp     # Motore di rendering (split in unità focali)
│   │   ├── render_engine_*.cpp   # Lifecycle, timeline, audio, packet, sidecar, ...
│   │   ├── execution_plan.cpp    # Piano di esecuzione segmenti
│   │   └── canonical_video_profile.cpp
│   ├── plan/
│   │   └── render_plan_parser*.cpp # Parser RenderPlan da JSON (V1/V2)
│   ├── render/
│   │   ├── frame_graph.cpp       # Compositing ops (single render hook)
│   │   ├── frame_backend.cpp     # Registry backend (CPU; GPU slot fail-closed)
│   │   ├── frame_overlay.cpp     # Kernel overlay scalar + AVX2
│   │   └── kernel_registry.cpp
│   ├── services/
│   │   ├── file_utils.cpp        # I/O, download, Drive
│   │   ├── media_probe.cpp       # Probe in-process (LibAV) + cache LRU sharded
│   │   ├── media_packet_*.cpp    # Pipeline packet copy-only
│   │   ├── frame_pipeline_*.cpp  # Pipeline frame nativa (decode/render/encode)
│   │   ├── ffmpeg_progress_parser.cpp # Parser -progress + spawn fork/exec
│   │   └── io_counters.cpp       # Contatori I/O di processo
│   └── telemetry/
│       └── emitter.cpp           # Emissione eventi telemetria
├── include/
│   ├── json_utils.hpp            # Scansione JSON + escape canonico
│   ├── video_contract.hpp        # Contratto legacy (struct Go<->C++)
│   └── velox/                    # Header pubblici (core, plan, render, services, telemetry)
├── schemas/
│   ├── render_plan_v1.json       # JSON Schema per RenderPlan V1
│   └── ...
├── tests/                        # Test CMake/CTest
├── cmake/                        # Moduli CMake (Options, Dependencies, Tests, ...)
├── CMakeLists.txt
└── README.md
```

## Build

The engine links `libavformat`, `libavcodec`, and `libavutil` for in-process
metadata probing. Install the FFmpeg development packages and `pkg-config`
before configuring CMake:

```bash
# Debian/Ubuntu
sudo apt-get install cmake pkg-config libavformat-dev libavcodec-dev libavutil-dev ffmpeg
```

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
ctest --output-on-failure
```

## RenderPlan V1 (`--render`)

Percorso canonico per tutti gli endpoint. Il piano JSON descrive cosa renderizzare:

```json
{
  "version": 1,
  "job_id": "abc123",
  "canvas": { "width": 1080, "height": 1920, "fps": 30 },
  "timeline": [
    {
      "source": { "type": "image", "url": "https://..." },
      "duration_seconds": 5.0,
      "transform": { "scale_mode": "cover", "ken_burns_effect": true }
    },
    {
      "source": { "type": "color", "color_hex": "#FF0000" },
      "duration_seconds": 2.0
    }
  ],
  "audio_tracks": [
    { "source_url": "https://...", "volume": 0.8, "start_time_offset": 1.0 }
  ],
  "output_path": "/tmp/output.mp4"
}
```

### Campi supportati

| Campo | Descrizione |
|---|---|
| `canvas.width/height/fps` | Dimensioni e frame-rate del video finale |
| `timeline[].source.type` | `image`, `video`, o `color` |
| `timeline[].source.url` | URL dell'asset (richiesto per image/video) |
| `timeline[].source.color_hex` | Colore esadecimale (richiesto per color) |
| `timeline[].duration_seconds` | Durata del segmento |
| `timeline[].transform.scale_mode` | `cover` (default), `contain`, `stretch` |
| `timeline[].transform.ken_burns_effect` | `true` per zoompan, `false` per fermo |
| `audio_tracks[].volume` | Volume (0-2, default 1.0) |
| `audio_tracks[].start_time_offset` | Ritardo in secondi prima che la traccia inizi |

### Fail-closed contract

Il motore non dichiara mai successo incompleto: un download asset, un mix
audio o una mux falliti producono `success=false` con `error` e un phase
telemetry `Abort` con error code (es. `audio_download_failed`,
`audio_mix_failed`, `asset_download_failed`). Non esistono fallback
silenziosi (es. sostituzione con segmento a tinta unita).

### Progress protocol

Entrambi i percorsi (`--render` e `--render-frames`) emettono progress JSON su
stderr:

```json
{"progress": 75, "percent": 75, "stage": "concatenating"}
```

Il campo `percent` è quello che il worker Go legge per il callback.
