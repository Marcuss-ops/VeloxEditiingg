# velox-shared

Libreria condivisa di tipi e utility tra **DataServer** (Go) e **worker-agent-go** (Go).

Lo scopo è eliminare la duplicazione di codice tra i due moduli e sincronizzare i tipi contratto con il **C++ video engine**.

## Struttura

```
velox-shared/
├── payload/    → Estrazione/normalizzazione valori da mappe JSON-deserializzate
├── paths/      → Manipolazione path, slug, URL Google Drive
├── media/      → Rilevamento metadati multimediali (ffprobe)
└── contract/   → Tipi contratto Go↔C++ per job video
```

## Dipendenze

```
┌─────────────┐
│   payload   │  (standalone — nessuna dipendenza interna)
└──────┬──────┘
       │ importato da
       ▼
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│  contract   │   │    paths    │   │    media    │
│ Tipi Go↔C++ │   │ Slug, path  │   │ ffprobe     │
│ Parse/Unmarshal  │ Drive URL   │   │ durata audio│
└─────────────┘   └─────────────┘   └─────────────┘
                                           │
                                           │ (usa exec.Command)
                                           ▼
                                        ffprobe
```

L'unica dipendenza interna è `contract → payload`. `paths` e `media` non dipendono da nessun altro package interno.

## Package

### `payload`
Utility per lavorare con `map[string]interface{}` da JSON deserializzato. Ogni funzione gestisce i type-switch necessari (`float64`, `json.Number`, `int`, `string`, etc.) e normalizza i risultati in tipi Go standard.

Funzioni principali:

| Funzione | Input | Output | Descrizione |
|---|---|---|---|
| `FirstString` | `map, keys...` | `string` | Prima stringa non vuota tra più chiavi |
| `StringParam` | `map, key, fallback` | `string` | Parametro string con default |
| `FloatParam` | `map, fallback, keys...` | `float64` | Float con guardia `> 0` (per configurazioni) |
| `FloatValue` | `map, key` | `float64` | Float senza guardia (per dati analytics) |
| `IntParam` / `EnsureInt` | `map/interface{}` | `int` | Int con guardia `> 0` |
| `NormalizedDuration` | `interface{}` | `float64` | Duration normalizzata (0 se non valida) |
| `NormalizeStringList` | `map, keys...` | `[]string` | Lista deduplicata da più chiavi |
| `ToSliceString` | `interface{}` | `[]string` | Conversione in slice di stringhe |
| `NormalizeList` | `interface{}` | `string` | Lista → stringa con newline |
| `NormalizeListToArray` | `interface{}` | `[]string` | Lista → slice |
| `AsString` / `AsInt` / `AsFloat` | `interface{}` | `tipo` | Conversione generica |
| `MustJSON` | `interface{}` | `string` | JSON marshalling silenzioso |
| `DeepCopyMap` | `map` | `map` | Copia profonda via JSON |
| `IsLikelyMediaSource` | `string` | `bool` | Rilevamento URL/estensione media |
| `DedupeStrings` | `[]string` | `[]string` | Deduplicazione |
| `ParseInt*` / `ParseFloatParam` | `string, default` | `tipo, error` | Parsing con default |
| `EnsureRFC3339` | `string, fallback` | `string` | Validazione data RFC3339 |
| `MapParam` / `SliceParam` | `map, key` | `map/slice` | Estrazione tipizzata |

### `paths`
Utility per path filesystem e URL Google Drive.

| Funzione | Descrizione |
|---|---|
| `SanitizeVideoName` | Slug safe per filesystem (solo lowercase + numeri + underscore) |
| `SanitizeStrings` | Trim + rimozione vuoti da slice |
| `DefaultOutputPath` | Path output predefinito per video |
| `VideoNameFromPath` | Nome file senza estensione |
| `SanitizeDriveFolderName` | Nome cartella Google Drive safe |
| `NormalizeDriveURL` | URL Drive → download diretto (`/uc?export=download&id=...`) |
| `ExtractDriveID` | Estrazione file ID da URL Drive |

### `media`
Utility per rilevamento metadati multimediali tramite **ffprobe**.

| Funzione | Descrizione |
|---|---|
| `DetectAudioDurationSecs` | Durata audio (supporta URL Drive) |
| `ResolveAudioURL` | Normalizzazione URL Drive per ffprobe |

### `contract`
Tipi condivisi Go↔C++ per la serializzazione JSON dei job video.
Le strutture corrispondono esattamente a quelle in `RemoteCodex/native/video-engine-cpp/include/video_contract.hpp`.

| Struct Go | Equivalente C++ | Descrizione |
|---|---|---|
| `SceneRequest` | `video::SceneAsset` (alias `SceneRuntime`) | Singola scena con testo e immagini |
| `ClipRequest` | `video::ClipAsset` (alias `ClipRuntime`) | Singolo clip segment |
| `VideoEngineRequest` | `video::SceneVideoRequest` | Richiesta completa al C++ engine |
| `RenderJobParams` | — (solo Go) | Parametri interni del workflow worker |

Funzioni di parsing:

| Funzione | Input → Output | Note |
|---|---|---|
| `ParseScenes` | `string` → `[]SceneRequest` | Silenzia errori |
| `ParseClips` | `[]interface{}` → `[]ClipRequest` | Da JSON deserializzato |
| `ParseClipsJSON` | `string` → `[]ClipRequest` | Simmetrica a ParseScenes |
| `UnmarshalSceneRequest` | `[]byte` → `*SceneRequest, error` | Singolo oggetto, con errore |
| `UnmarshalClipRequest` | `[]byte` → `*ClipRequest, error` | Singolo oggetto, con errore |
| `UnmarshalScenes` | `[]byte` → `[]SceneRequest, error` | Array, con errore |
| `UnmarshalClips` | `[]byte` → `[]ClipRequest, error` | Array, con errore |
| `ExtractRenderJobParams` | `map` → `RenderJobParams` | Estrazione da param map |
| `NormalizeSceneEntry` | `map` → `map` | Normalizzazione scena (fallback campi) |
| `FirstSceneImageLink` | `map` → `string` | Prima immagine disponibile |

## Differenze note Go↔C++

I test in `contract_test.go` verificano la compatibilità JSON tra Go e C++.
Le differenze note sono:

| Campo Go | Equivalente C++ | Motivo |
|---|---|---|
| `clip_segments` (`[]ClipRequest`) | `clip_segments_json` (`std::string`) | Go serializza array tipizzato, C++ riceve JSON string |
| `scene_image_paths` (`[]string`) | N/A | Solo Go, usato internamente dal worker |

## Utilizzo

Tutti i package usano il module path `velox-shared`. Per importare in altri moduli Go:

```go
import (
    "velox-shared/contract"
    "velox-shared/payload"
    "velox-shared/paths"
    "velox-shared/media"
)
```

Nei `go.mod` dei consumer va aggiunto:
```
require velox-shared v0.0.0
replace velox-shared v0.0.0 => ../shared
```

(Adattare il path del replace alla struttura della propria repository.)
