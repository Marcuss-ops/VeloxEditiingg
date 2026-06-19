# Worker Job Template — Auto-Generate Video from Topic

> Endpoint sul **worker remoto** che genera automaticamente script, immagini AI, voiceover TTS, renderizza il video e lo uploada su YouTube + Drive.

## Endpoint

```
POST http://<WORKER_IP>:8081/api/script/generate-with-images
```

Il worker è raggiungibile via tunnel localhost.run:
```
https://<tunnel>.lhr.life/api/script/generate-with-images
```

## Payload Template (funzionante ✅)

```json
{
  "topic": "How Amish families save money and manage their budget",
  "channel_id": "amish",
  "language": "en",
  "languages": ["en", "pt", "pl", "fr", "de", "ru"],
  "scene_count": 5,
  "images_per_scene": 1,
  "render_video": true,
  "use_memory": false,
  "save_to_db": true
}
```

## Campi

| Campo | Tipo | Default | Descrizione |
|-------|------|---------|-------------|
| `topic` | `string` | **obbligatorio** | Argomento del video (il worker genera script, scene, immagini da questo) |
| `channel_id` | `string` | — | Nome del gruppo YouTube per upload automatico (es. `"amish"`) |
| `language` | `string` | `"en"` | Lingua principale del video |
| `languages` | `array[string]` | — | Array di tutte le lingue per i voiceover multilingua (upload su canali diversi nel gruppo) |
| `scene_count` | `number` | `5` | Numero di scene/generations da creare |
| `images_per_scene` | `number` | `1` | Numero di immagini AI per scena |
| `render_video` | `boolean` | `true` | Se `true`, esegue il rendering del video finale |
| `use_memory` | `boolean` | `false` | Usa memoria contestuale tra job |
| `save_to_db` | `boolean` | `false` | Salva il risultato nel database |

## Esempio di utilizzo (curl)

```bash
curl -X POST http://<WORKER_IP>:8081/api/script/generate-with-images \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "How Amish families save money and manage their budget",
    "channel_id": "amish",
    "language": "en",
    "languages": ["en", "pt", "pl", "fr", "de", "ru"],
    "scene_count": 5,
    "images_per_scene": 1,
    "render_video": true,
    "use_memory": false,
    "save_to_db": true
  }'
```

## Response

```json
{
  "ok": true,
  "status": "queued",
  "job_id": "job_1781375207918096601_8c4dab64"
}
```

## Monitoraggio

```bash
GET http://<WORKER_IP>:8081/api/script/jobs/:job_id
```

Stati: `queued` → `running` → `completed` | `failed`

Esempio:
```bash
curl http://<WORKER_IP>:8081/api/script/jobs/job_1781375207918096601_8c4dab64
```

## Output generati (dal test riuscito ✅)

- **Script**: Google Doc con testo completo
- **Voiceover**: 6 file audio (uno per lingua) su Google Drive
- **Immagini**: 5 scene generate AI per ogni lingua
- **Video**: Renderizzato e processato dal worker
- **YouTube**: Upload automatico sui canali del gruppo (uno per lingua)
- **Drive**: Cartella dedicata su Google Drive

---

## Template vuoto per nuovi job

```json
{
  "topic": "YOUR_TOPIC_HERE",
  "channel_id": "amish",
  "language": "en",
  "languages": ["en", "pt", "pl", "fr", "de", "ru"],
  "scene_count": 5,
  "images_per_scene": 1,
  "render_video": true,
  "use_memory": false,
  "save_to_db": true
}
```
