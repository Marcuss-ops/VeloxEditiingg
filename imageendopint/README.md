# Image Endpoint

Servizio HTTP per accodare richieste di generazione immagini, eseguirle in un worker separato e restituire artefatti scaricabili tramite API.

## Cosa fa

- Espone `POST /v1/generate`
- Accoda i job in Redis
- Esegue la generazione in un worker separato
- Riusa i cookie e le sessioni del profilo Chrome copiandoli in un profilo di lavoro headless
- Salva artefatti in `outputs/<job_id>/`

## Prerequisiti

- Python 3.11+
- Google Chrome installato nel percorso configurato
- Il profilo sorgente deve essere il root del profilo Chrome, di solito `~/.config/google-chrome`
- Redis 7+ raggiungibile dal servizio

## Installazione

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
playwright install chromium
```

Per avviare Redis in locale:

```bash
docker run -p 6379:6379 --name image-endpoint-redis -d redis:7-alpine
```

## Configurazione

Crea un `.env` oppure esporta le variabili.

```bash
export API_TOKEN="una_chiave_lunga"
export CHROME_EXECUTABLE="/opt/google/chrome/google-chrome"
export CHROME_CDP_URL="http://127.0.0.1:9222"
export PROFILE_SOURCE_DIR="/home/pierone/.config/google-chrome"
export PROFILE_WORK_DIR="/home/pierone/Pyt/imageendopint/.cache/google-chrome-headless"
export COOKIE_JAR_PATH="/home/pierone/Download/cookies.txt"
export STORAGE_STATE_PATH="/home/pierone/Pyt/imageendopint/outputs/flow-storage-state.json"
export HEADLESS=true
export HOST="0.0.0.0"
export PORT="8000"
export REDIS_URL="redis://127.0.0.1:6379/0"
export REDIS_QUEUE_NAME="image-endpoint:jobs"
export REDIS_JOB_KEY_PREFIX="image-endpoint:job"
```

Se il sito Flow usa selettori stabili diversi, puoi forzarli:

```bash
export PROMPT_SELECTOR="textarea"
export SUBMIT_SELECTOR="button[type='submit']"
```

## Modalità CDP

Se vuoi che il servizio si agganci al tuo Chrome già aperto e già loggato, avvia Chrome con remote debugging:

```bash
/opt/google/chrome/google-chrome \
  --remote-debugging-port=9222 \
  --user-data-dir=/home/pierone/.config/google-chrome
```

Poi avvia il server con `CHROME_CDP_URL=http://127.0.0.1:9222`.

In questa modalità il servizio usa la sessione già aperta del browser e non prova a ricreare il profilo headless.

Per esportare lo stato sessione da Chrome già loggato:

```bash
./scripts/export-storage-state.sh
```

Questo salva `outputs/flow-storage-state.json` per l'uso headless successivo.

## Modalità davvero headless

Una volta esportato `STORAGE_STATE_PATH` dalla sessione loggata, puoi chiudere Chrome e far girare tutto senza finestra.

Il flusso è:

1. apri Chrome loggato una volta sola
2. esporti lo stato sessione in `STORAGE_STATE_PATH`
3. chiudi Chrome
4. fai partire API e worker con Redis attivo

Da quel momento il backend:

- riceve il prompt via API
- accoda il job in Redis
- il worker apre Flow headless
- inserisce il prompt nella textbox
- preme `Enter`
- aspetta le immagini
- scarica i file finali in `outputs/<job_id>/generated-*.jpg`

## Script sessione Chrome

Per fare login nel profilo che poi useremo per l'automazione:

```bash
./scripts/chrome-session.sh start "https://labs.google/fx/tools/flow/project/6a001474-4561-4f81-9c0d-65af18805fec"
```

Poi usa quel Chrome per entrare su Flow e verificare che il progetto sia aperto.

Quando vuoi chiudere:

```bash
./scripts/chrome-session.sh close
```

Lo script salva il profilo in `.chrome-session/saved-sessions/<timestamp>/`.

## Avvio

API:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Worker:

```bash
image-endpoint-worker
```

In alternativa:

```bash
python -m app.worker
```

## Uso

Invio job:

```bash
curl -X POST http://server-remoto:8000/v1/generate \
  -H "Authorization: Bearer una_chiave_lunga" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "6a001474-4561-4f81-9c0d-65af18805fec",
    "prompt": "Crea un'immagine di un robot minimalista in stile poster"
  }'
```

Risposta:

```json
{"job_id":"...","status":"queued"}
```

Stato job:

```bash
curl -H "Authorization: Bearer una_chiave_lunga" \
  http://server-remoto:8000/v1/jobs/<job_id>
```

Download artefatto:

```bash
curl -H "Authorization: Bearer una_chiave_lunga" \
  http://server-remoto:8000/v1/jobs/<job_id>/artifact/generated-01.jpg \
  --output generated-01.jpg
```

Gli artefatti principali sono:

- `outputs/<job_id>/page.png`
- `outputs/<job_id>/page.html`
- `outputs/<job_id>/result.json`
- `outputs/<job_id>/generated-*.jpg`

## Nota importante

La parte di automazione del sito Flow è volutamente generica. Se l'interfaccia usa pulsanti o campi con selettori diversi, imposta `PROMPT_SELECTOR` e `SUBMIT_SELECTOR` o adatta `app/browser/` ai selettori reali.
