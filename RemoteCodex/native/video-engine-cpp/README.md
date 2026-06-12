# Velox Video Engine C++

Motore C++ nativo per la composizione video Velox. Scomposto in **building block CLI indipendenti**,
ognuno eseguibile singolarmente per massima riutilizzabilità tra endpoint.

## Struttura

```
video-engine-cpp/
├── src/
│   ├── main.cpp              # Dispatcher CLI + implementazione sotto-comandi
│   └── video_builder.cpp/.hpp# Parsing scene/clip da JSON (namespace velox)
├── include/
│   ├── video_contract.hpp     # Contratto video (struct Go↔C++)
│   ├── json_utils.hpp         # Parsing JSON helper (namespace velox::json)
│   ├── file_utils.hpp         # I/O file, download, Drive (namespace velox::file)
│   └── media_utils.hpp        # Helpers FFmpeg/media (namespace velox::media)
├── schemas/                   # JSON schema dei contratti
│   ├── colosseo_scene_video.json
│   └── smoke_video_to_video.json
├── CMakeLists.txt
└── README.md
```

## Build

```bash
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
```

## Sotto-comandi CLI

Ogni sotto-comando stampa JSON su **stdout** e log/errori su **stderr**.
Exit code: `0` = successo, `1` = errore.

### `--full-video` — Pipeline completa

Combina tutti i blocchi: scarica asset, costruisce segmenti (scene o clip), concatena, muxa audio.

```bash
./velox_video_engine --full-video --request /path/to/payload.json
```

Il payload JSON (`video_contract.hpp` → `SceneVideoRequest`) deve contenere almeno `output_path`.

### `--download-asset` — Scarica asset

Scarica un file da URL (supporta Google Drive).

```bash
./velox_video_engine --download-asset --url "https://..." --dest /tmp/asset.mp4
# Output: {"success":true,"url":"https://...","dest":"/tmp/asset.mp4"}
```

### `--probe-media` — Rileva durata

Rileva la durata di un file multimediale tramite ffprobe.

```bash
./velox_video_engine --probe-media /tmp/voiceover.mp3
# Output: {"success":true,"path":"/tmp/voiceover.mp3","duration_seconds":127.5}
# Exit 1 se il file non esiste o ffprobe fallisce
```

### `--build-scene-segment` — Segmento da immagine

Genera un segmento video da un'immagine con effetto zoompan Ken Burns.

```bash
./velox_video_engine --build-scene-segment --image /tmp/scene.jpg --duration 5.0 --out /tmp/segment.mp4
# Output: {"success":true,"out":"/tmp/segment.mp4"}
```

### `--build-clip-segment` — Segmento da clip video

Genera un segmento video da un clip (scalato/croppato a 1920×1080).

```bash
./velox_video_engine --build-clip-segment --clip /tmp/intro.mp4 --duration 4.0 --out /tmp/segment.mp4
# Output: {"success":true,"out":"/tmp/segment.mp4"}
```

### `--concat-segments` — Concatena segmenti

Concatena segmenti video usando un file lista (una path per riga).

```bash
echo "/tmp/seg1.mp4" > /tmp/list.txt
echo "/tmp/seg2.mp4" >> /tmp/list.txt
./velox_video_engine --concat-segments --list /tmp/list.txt --out /tmp/merged.mp4
# Output: {"success":true,"out":"/tmp/merged.mp4","segments":2}
```

### `--mux-audio` — Muxa audio su video

Aggiunge una traccia audio a un video (codifica AAC, output mp4).

```bash
./velox_video_engine --mux-audio --video /tmp/video_only.mp4 --audio /tmp/voiceover.mp3 --out /tmp/final.mp4
# Output: {"success":true,"out":"/tmp/final.mp4"}
```

### `--help` — Guida

Mostra la lista completa dei sotto-comandi e opzioni.

```bash
./velox_video_engine --help
```

## Esempi di composizione

Nuovo endpoint "solo scene senza audio":

```bash
# 1. Scarica immagini
for img in https://...; do
    ./velox_video_engine --download-asset --url "$img" --dest "/tmp/scene_$i.jpg"
done
# 2. Genera segmenti
for i in 0 1 2; do
    ./velox_video_engine --build-scene-segment --image "/tmp/scene_$i.jpg" --duration 5 --out "/tmp/seg_$i.mp4"
done
# 3. Concatena
echo "/tmp/seg_0.mp4" > /tmp/list.txt
echo "/tmp/seg_1.mp4" >> /tmp/list.txt
echo "/tmp/seg_2.mp4" >> /tmp/list.txt
./velox_video_engine --concat-segments --list /tmp/list.txt --out /tmp/output.mp4
```

Nuovo endpoint "solo clip":

```bash
# Salta download scene, salta mux audio
./velox_video_engine --build-clip-segment --clip intro.mp4 --duration 4 --out seg0.mp4
./velox_video_engine --build-clip-segment --clip main.mp4  --duration 10 --out seg1.mp4
echo "... list ..." | ./velox_video_engine --concat-segments --list /dev/stdin --out output.mp4
```
