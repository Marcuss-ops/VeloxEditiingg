# Velox Video Engine C++

Motore C++ nativo per la composizione video Velox. Riceve un payload JSON con parametri di scena, scarica asset, compone segmenti video con FFmpeg e produce il file finale.

## Struttura

```
video-engine-cpp/
├── src/
│   └── main.cpp              # Entrypoint, parsing scene, main()
├── include/
│   ├── video_contract.hpp     # Contratto video (struct/const)
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
cmake ..
cmake --build . -j$(nproc)
```

## Utilizzo

```bash
./build/velox_video_engine --request /path/to/payload.json
```

Il payload JSON deve contenere almeno `output_path` e parametri di scena/clip.
