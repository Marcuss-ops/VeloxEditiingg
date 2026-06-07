# Image Endpoint - Documentazione Tecnica

Backend HTTP per accodare richieste di generazione immagini, eseguirle in un worker separato e consegnare gli artefatti via API.

## Architettura

Il sistema è diviso in tre pezzi:

- **API FastAPI**: riceve le richieste, valida l'autenticazione e crea il job.
- **Redis**: conserva stato dei job e funge da coda.
- **Worker Playwright**: consuma i job, apre Flow, invia il prompt, scarica le immagini e salva gli artefatti.

### Flusso

1. Un client remoto invia `POST /v1/generate`.
2. L'API crea un record job persistente in Redis.
3. L'API mette il job nella coda Redis e risponde subito con `job_id`.
4. Il worker legge la coda, prende il job e lo marca `running`.
5. Il worker esegue la generazione con Playwright headless.
6. Gli artefatti vengono salvati in `outputs/<job_id>/`.
7. Il client controlla lo stato con `GET /v1/jobs/{job_id}` e scarica i file con `GET /v1/jobs/{job_id}/artifact/{name}`.

## Componenti

- `app/main.py`: API FastAPI e endpoint pubblici.
- `app/worker.py`: loop di consumo della coda.
- `app/store.py`: persistenza job su Redis.
- `app/browser/`: automazione browser, estrazione immagini e supporti.
- `app/models.py`: modelli dati e serializzazione job.
- `app/config.py`: configurazione da variabili d'ambiente.

## Variabili d'Ambiente

```env
API_TOKEN=change-me
CHROME_EXECUTABLE=/opt/google/chrome/google-chrome
CHROME_CDP_URL=http://127.0.0.1:9222
PROFILE_SOURCE_DIR=/home/pierone/.config/google-chrome
PROFILE_WORK_DIR=/home/pierone/Pyt/imageendopint/.cache/google-chrome-headless
COOKIE_JAR_PATH=/home/pierone/Download/cookies.txt
STORAGE_STATE_PATH=/home/pierone/Pyt/imageendopint/outputs/flow-storage-state.json
HEADLESS=true
HOST=0.0.0.0
PORT=8000
REDIS_URL=redis://127.0.0.1:6379/0
REDIS_QUEUE_NAME=image-endpoint:jobs
REDIS_JOB_KEY_PREFIX=image-endpoint:job
PROJECT_URL_TEMPLATE=https://labs.google/fx/tools/flow/project/{project_id}
JOB_TIMEOUT_SECONDS=900
RESULT_POLL_SECONDS=5
MAX_RESULT_WAIT_SECONDS=300
```

## Installazione

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
playwright install chromium
```

Per Redis locale:

```bash
docker run -p 6379:6379 --name image-endpoint-redis -d redis:7-alpine
```

## Setup Sessione Google

Per usare Flow in headless serve una sessione valida.

### 1. Apri Chrome con il profilo autenticato

```bash
./scripts/chrome-session.sh start "https://labs.google/fx/tools/flow/project/<project_id>"
```

### 2. Fai login

Entra con l'account Google che ha accesso a Flow.

### 3. Esporta lo stato sessione

```bash
./scripts/export-storage-state.sh
```

Questo produce `outputs/flow-storage-state.json`.

### 4. Chiudi Chrome

```bash
./scripts/chrome-session.sh close
```

## Avvio

### API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Worker

```bash
image-endpoint-worker
```

In alternativa:

```bash
python -m app.worker
```

## API

### `POST /v1/generate`

Accoda un nuovo job.

Esempio:

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

### `GET /v1/jobs/{job_id}`

Restituisce stato e metadati del job.

Campi utili:

- `status`: `queued`, `running`, `succeeded`, `failed`
- `client_ip`: IP del chiamante
- `user_agent`: user agent del client
- `error`: dettaglio dell'errore in caso di fallimento
- `result`: metadati finali e artefatti

Esempio:

```bash
curl -H "Authorization: Bearer una_chiave_lunga" \
  http://server-remoto:8000/v1/jobs/<job_id>
```

### `GET /v1/jobs/{job_id}/log`

Restituisce il log per-job in formato testo.

```bash
curl -H "Authorization: Bearer una_chiave_lunga" \
  http://server-remoto:8000/v1/jobs/<job_id>/log
```

### `GET /v1/jobs/{job_id}/artifact/{name}`

Scarica un artefatto dal job.

```bash
curl -H "Authorization: Bearer una_chiave_lunga" \
  http://server-remoto:8000/v1/jobs/<job_id>/artifact/generated-01.jpg \
  --output generated-01.jpg
```

## Log e Stato

Per ogni job il worker crea:

- `outputs/<job_id>/job.log`
- `outputs/<job_id>/page.png`
- `outputs/<job_id>/page.html`
- `outputs/<job_id>/result.json`
- `outputs/<job_id>/generated-*.jpg`

Il log contiene:

- ricezione job
- IP e user agent del chiamante
- inizio esecuzione
- esito finale
- errori catturati

## Accesso Da Altri Computer

Per ricevere richieste da altri host:

- imposta `HOST=0.0.0.0`
- apri la porta `8000` sul firewall della macchina
- usa `API_TOKEN` su tutte le richieste
- esponi l'API idealmente dietro reverse proxy HTTPS

Esempio di test da un computer remoto:

```bash
curl -X POST http://IP_DEL_SERVER:8000/v1/generate \
  -H "Authorization: Bearer una_chiave_lunga" \
  -H "Content-Type: application/json" \
  -d '{"project_id":"...","prompt":"..."}'
```

Se il server è dietro Nginx o un bilanciatore, aggiungi anche `X-Forwarded-For` per ottenere nel job il client reale.

## Note Operative

- Redis mantiene coda e stato dei job anche se l'API viene riavviata.
- Il worker può essere avviato su una macchina separata.
- L'API risponde subito con `job_id`, quindi i client remoti devono fare polling sullo stato.
- Se Flow cambia selettori o UI, aggiorna i selettori in `app/browser/`.
