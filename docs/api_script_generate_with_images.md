# POST /api/script/generate-with-images

> Endpoint principale per creare un job video da scene e asset gia pronti.

## Overview

Questo endpoint accetta un payload JSON con scene, immagini, voiceover e parametri di configurazione gia generati upstream, normalizza i dati (rilevando automaticamente la durata dell'audio se non specificata), e accoda un job `process_video` per il worker remoto.

## Endpoint

```
POST /api/script/generate-with-images
POST /api/v1/script/generate-with-images
```

Entrambi gli endpoint sono protetti da **admin token** (header `Authorization: Bearer <token>`).

## Flusso di elaborazione

```
Client → POST /api/script/generate-with-images
  │
  ▼
Server (scene_normalize.go)
  │  • Normalizza scene/immagini gia presenti nel payload
  │  • Rileva durata audio (ffprobe) SE non specificata
  │  • Distribuisce durata equamente tra le scene
  │  • Prepara payload → scenes_json con duration_seconds
  │
  ▼
FileQueue → Worker remoto disponibile
  │
  ▼
Worker (native_engine.go)
  │  • Parsa scenes_json → estrae duration_seconds ✅ (fixato)
  │  • Se nessuna scena ha durata → auto-detect ffprobe fallback
  │  • Prepara richiesta per C++ engine
  │
  ▼
C++ Video Engine (velox_video_engine)
  │  • Legge duration_seconds da ogni scena
  │  • Genera video con audio + immagini + durata corretta
  │
  ▼
Worker → Upload risultato su Google Drive
  │
  ▼
Job COMPLETED
```

## Payload Request

### Campi obbligatori

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| `voiceover_path` | `string` | URL del file audio gia pronto (Google Drive, HTTP, locale accessibile) |
| `scenes` | `array` | Array di oggetti scena (almeno 1) |

**Oppure** al posto di `scenes` si può usare:
| `images` | `array[string]` | Array di URL immagini gia generate upstream |
| `source_text` | `string` | Testo descrittivo usato come metadato o fallback per il testo scena |

### Campi opzionali

| Campo | Tipo | Default | Descrizione |
|-------|------|---------|-------------|
| `video_name` | `string` | auto-generato | Nome del video |
| `language` | `string` | `"it"` | Lingua per sottotitoli SRT |
| `youtube_group` | `string` | — | Nome gruppo YouTube per upload automatico |
| `drive_output_folder` | `string` | — | Cartella Drive di destinazione |
| `scene_duration_secs` | `number` | auto-detect | Durata di OGNI scena in secondi |
| `total_duration_secs` | `number` | auto-detect | Durata TOTALE del video in secondi |
| `scene_count` | `number` | `len(scenes)` | Numero di scene (se si usa `images`) |

### Oggetto Scena

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| `text` | `string` | Testo da mostrare nella scena |
| `image_link` | `string` | URL immagine (Google Drive) |
| `image_links` | `array[string]` | URL immagini multiple |
| `duration_seconds` | `number` | **Sovrascritto dal server** con durata calcolata |

### Auto-detect durata audio

**Nuovo!** Se non specifichi `scene_duration_secs` né `total_duration_secs`:

1. Il server rileva la durata dell'audio via `ffprobe` (supporta link Google Drive)
2. Divide equamente: `durata_audio / numero_scene`
3. Imposta `duration_seconds` su ogni scena

```
Esempio: audio 336s, 3 scene → 112s per scena
```

Nota: questo endpoint non genera immagini o voiceover in locale. Si aspetta che arrivino gia prodotti da un servizio upstream.

#### Esempio completo

```json
{
  "video_name": "Amish Stories",
  "source_text": "The Amish community lives a simple life...",
  "language": "en",
  "youtube_group": "amish",
  "voiceover_path": "https://drive.google.com/file/d/XXX/view",
  "drive_output_folder": "https://drive.google.com/drive/u/1/folders/YYY",
  "scenes": [
    {
      "text": "The Amish live a simple life.",
      "image_link": "https://drive.google.com/file/d/IMG1/view"
    },
    {
      "text": "Their community is based on faith.",
      "image_link": "https://drive.google.com/file/d/IMG2/view"
    }
  ]
}
```

## Response

```json
{
  "ok": true,
  "job_id": "scriptimg_uuid",
  "job_run_id": "run_uuid",
  "correlation_id": "corr_uuid",
  "status": "PENDING",
  "video_mode": "scene_image",
  "video_name": "Amish Stories",
  "output_path": ".../generated_videos/script_with_images/...mp4",
  "drive_output_folder": "...",
  "scene_count": 2,
  "voiceover_count": 1,
  "scene_image_paths": ["..."],
  "enqueue": { "...": "..." }
}
```

## Job status

Dopo l'invio, controlla lo stato:

```
GET /api/script/jobs/:job_id
GET /api/v1/script/jobs/:job_id
GET /api/script/jobs/:job_id/full   (dettaglio completo)
```

Stati possibili: `PENDING` → `PROCESSING` → `COMPLETED` | `FAILED`

## Casi d'uso

### 1. Video con durata automatica (raccomandato)
Basta inviare audio + scene gia pronte. Il server rileva la durata e distribuisce.

### 2. Durata esplicita
Specifica `total_duration_secs: 60` per forzare 60 secondi totali.
Specifica `scene_duration_secs: 30` per forzare 30 secondi per scena.

### 3. Solo immagini (senza scene strutturate)
```json
{
  "images": ["url1", "url2", "url3"],
  "scene_count": 3,
  "voiceover_path": "..."
}
```

## Workers

I worker sono gestiti dinamicamente dal server. Consulta `/api/v1/workers` per la lista dei worker attivi.

## Polling log (worker)

Il worker logga l'attività di polling con tag `[POLLING]`:

```
[INFO] [host_xxx] [POLLING] Status: alive — 12 polls sent, no jobs available
[INFO] [host_xxx] [POLLING] Job acquired on attempt 3 — executing
```
