# Velox Video Engine C++

Motore C++ nativo per la composizione video Velox. Supporta due percorsi:

1. **`--render --plan <path>`** — Percorso nuovo: consuma un `RenderPlan` JSON (contratto canonico).
2. **`--full-video --request <path>`** — Percorso legacy: consuma un `VideoEngineRequest` JSON.

## Struttura

```
video-engine-cpp/
├── src/
│   ├── main.cpp                  # Dispatcher CLI
│   ├── cmd_full_video.cpp        # Pipeline legacy (--full-video)
│   ├── video_builder.cpp/.hpp    # Parsing scene/clip da JSON legacy
│   ├── app/
│   │   └── commands.cpp          # Comando --render
│   ├── core/
│   │   └── render_engine.cpp     # Motore di rendering generico
│   ├── plan/
│   │   └── render_plan_parser.cpp# Parser RenderPlan da JSON
│   └── services/
│       ├── file_utils.cpp        # I/O, download, Drive
│       └── media_utils.cpp       # FFmpeg wrappers
├── include/
│   ├── json_utils.hpp            # Parsing JSON helper (regex-based)
│   ├── video_contract.hpp        # Contratto legacy (struct Go<->C++)
│   └── velox/
│       ├── core/
│       │   └── render_engine.hpp # RenderEngine + RenderResult
│       ├── plan/
│       │   ├── render_plan.hpp   # Modello RenderPlan V1
│       │   └── render_plan_parser.hpp
│       └── services/
│           ├── file_utils.hpp
│           └── media_utils.hpp   # SceneSegmentParams + FFmpeg API
├── schemas/
│   ├── render_plan_v1.json       # JSON Schema per RenderPlan V1
│   ├── colosseo_scene_video.json # Esempio legacy scene
│   └── smoke_video_to_video.json # Esempio legacy clip
├── CMakeLists.txt
└── README.md
```

## Build

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
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

### Progress protocol

Entrambi i percorsi (`--render` e `--full-video`) emettono progress JSON su stderr:

```json
{"progress": 75, "percent": 75, "stage": "concatenating"}
```

Il campo `percent` è quello che il worker Go legge per il callback.

## Sotto-comandi CLI legacy

### `--full-video` — Pipeline completa

```bash
./velox_video_engine --full-video --request /path/to/payload.json
```

### `--download-asset` — Scarica asset

```bash
./velox_video_engine --download-asset --url "https://..." --dest /tmp/asset.mp4
```

### `--probe-media` — Rileva durata

```bash
./velox_video_engine --probe-media /tmp/voiceover.mp3
```

### `--build-scene-segment` — Segmento da immagine

```bash
./velox_video_engine --build-scene-segment --image /tmp/scene.jpg --duration 5.0 --out /tmp/segment.mp4
```

### `--build-clip-segment` — Segmento da clip

```bash
./velox_video_engine --build-clip-segment --clip /tmp/intro.mp4 --duration 4.0 --out /tmp/segment.mp4
```

### `--concat-segments` — Concatena segmenti

```bash
./velox_video_engine --concat-segments --list /tmp/list.txt --out /tmp/merged.mp4
```

### `--mux-audio` — Muxa audio su video

```bash
./velox_video_engine --mux-audio --video /tmp/video_only.mp4 --audio /tmp/voiceover.mp3 --out /tmp/final.mp4
```
