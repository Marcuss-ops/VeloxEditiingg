# Velox — Collegamento Master/Client per job M2M

## Stato verificato

Il Creator operativo è la macchina:

```
pierone@77.93.152.122
```

Il servizio PipelineGen/Creator è attivo come `pipelinegen.service` e ascolta su:

```
http://77.93.152.122:8000
```

Verifiche completate:

- `GET /health` risponde `200`;
- `GET /ready` risponde `200`;
- `POST /api/v1/jobs` senza credenziali risponde `401`;
- il Creator può inviare il payload completato al render master;
- il render master può assegnare il task a un subworker.

Job di prova verificato:

```
job_1788115346956711063_5794794c
```

Risultato:

```
status: SUCCEEDED
render attesi: 5
render riusciti: 5
render falliti: 0
output: UPLOADED
```

Il test è stato inviato dalla macchina remota stessa e seguito fino alla fine senza intervento manuale.

> Nota: il payload aveva `assemble_final: false`. Ha verificato submit, autenticazione, coda, rendering, documentazione e upload dei 5 clip singoli. Non ha prodotto un unico video finale assemblato.

Il successivo job da 40 clip (`job_1788116640302762695_f27a4b0d`) è terminato con successo, ma fu eseguito prima della correzione del routing: `RENDERINGGEN_QUEUE_URL` era vuoto e il render finale venne eseguito localmente dal Master. Quel job non è una certificazione del routing verso worker.

Il routing è stato corretto il 30 agosto 2026. Ora è configurato:

```text
RENDERINGGEN_QUEUE_URL=http://51.91.11.36:8000
```

Il `77` crea e trasferisce clip, audio e scene. Il `51.91.11.36` è il render master/orchestrator: riceve il Creator push, prepara il task e lo assegna ai subworker video registrati al `51`. Il `77` non deve assemblare né renderizzare il video finale.

## Architettura

```
Creator / PipelineGen — 77.93.152.122
          |
          | Creator push
          v
Render master/orchestrator: 51.91.11.36:8000
          |
          | gRPC task placement + asset prefetch
          v
Subworker video del render master
          |
          v
Google Drive / Social Editor
```

Il Creator deve conoscere soltanto:

```
VELOX_MASTER_URL
VELOX_CLIENT_ID
VELOX_M2M_SECRET
```

Non deve ricevere token amministrativi, credenziali Drive, certificati worker o dettagli interni del codec.

## Credenziali M2M

Sul Master è stato creato il client:

```
computer-editor-77-01
```

La configurazione privata è salvata sul Master in:

```
/home/pierone/computer-editor-77-01.env
```

Il file contiene il secret M2M e non deve essere committato, pubblicato o incluso nei log. Per trasferirlo a un altro computer usare un canale sicuro e poi impostare permessi `600`.

Contenuto atteso, con secret omesso:

```bash
VELOX_MASTER_URL=http://77.93.152.122:8000
VELOX_CLIENT_ID=computer-editor-77-01
VELOX_M2M_SECRET=<secret-privato>
```

Il client M2M di polling dispone dello scope:

```
jobs.submit
```

## Invio di un job

Il contratto attualmente attivo usa l'envelope `EnqueueRequest`. Il client deve inviare `type`, `project`, `video_name`, `payload` e `idempotency_key`.

Esempio:

```bash
set -a
. /path/to/computer-editor-77-01.env
set +a

IDEMPOTENCY_KEY="matt-damon-5-$(date -u +%Y%m%dT%H%M%SZ)"

jq -n \
  --arg type "script.generate" \
  --arg project "matt-damon-5-clips-docs-true" \
  --arg video_name "Matt Damon — 5 clip verification" \
  --arg idem "$IDEMPOTENCY_KEY" \
  --slurpfile payload /path/to/job-payload.json \
  '{
    type: $type,
    project: $project,
    video_name: $video_name,
    payload: $payload[0],
    priority: 1,
    max_retries: 3,
    correlation_id: $idem,
    idempotency_key: $idem
  }' > /tmp/velox-job.json

curl --fail-with-body -sS \
  -X POST "$VELOX_MASTER_URL/api/v1/jobs" \
  -H "Authorization: Bearer $VELOX_M2M_SECRET" \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: $VELOX_CLIENT_ID-$IDEMPOTENCY_KEY" \
  --data-binary @/tmp/velox-job.json
```

La risposta accettata è `HTTP 202` e contiene il `job_id`.

## Polling autonomo

Per leggere lo stato:

```bash
JOB_ID="job_xxx"

curl --fail-with-body -sS \
  "$VELOX_MASTER_URL/api/v1/jobs/$JOB_ID" \
  -H "Authorization: Bearer $VELOX_M2M_SECRET" \
  -H "Accept: application/json" | jq .
```

Il client termina il polling quando `status` è:

```
SUCCEEDED  -> completato
FAILED     -> errore
CANCELLED  -> annullato
```

Per `PENDING` e `RUNNING` deve attendere e riprovare. Un intervallo iniziale di 5 secondi è sufficiente; aumentarlo gradualmente fino a 30 secondi per job lunghi.

Esempio:

```bash
while true; do
  response=$(curl --fail-with-body -sS \
    "$VELOX_MASTER_URL/api/v1/jobs/$JOB_ID" \
    -H "Authorization: Bearer $VELOX_M2M_SECRET") || exit 1

  status=$(printf '%s' "$response" | jq -r '.status // .job.status // "UNKNOWN"')
  printf 'job=%s status=%s\n' "$JOB_ID" "$status"

  case "$status" in
    SUCCEEDED) printf '%s\n' "$response" | jq .; break ;;
    FAILED|CANCELLED) printf '%s\n' "$response" | jq . >&2; exit 1 ;;
    PENDING|RUNNING) sleep 5 ;;
    *) printf '%s\n' "$response" >&2; exit 1 ;;
  esac
done
```

## Idempotenza e retry

Una richiesta ripetuta deve usare la stessa `idempotency_key`. Questo consente al client di riprovare se la connessione cade dopo l'invio senza creare intenzionalmente una seconda lavorazione.

Regole:

1. Generare una chiave una sola volta per ogni job logico.
2. Salvare la chiave insieme al job locale del client.
3. Riutilizzare la stessa chiave per ogni retry della stessa richiesta.
4. Generare una nuova chiave soltanto per un nuovo job.

Non mettere secret o token dentro `idempotency_key`, `X-Request-ID`, titolo, payload o log.

## Payload video e profilo 24 fps

Per il flusso futuro di assemblaggio si raccomanda un profilo canonico unico:

```
video-copy-1080p24-v1
```

Il profilo deve definire almeno:

```
H.264 / AVC
1920x1080
yuv420p
24 fps CFR
progressive
AAC-LC
48 kHz stereo
MP4 faststart
```

Prima di usare `copy_only=true`, tutti i clip devono avere la stessa firma media, compresi codec, profilo, risoluzione, pixel format, frame rate, timebase, extradata, audio e layout canale. Se una firma non coincide, il Master deve normalizzare il clip e ripetere il preflight.

Il client non deve decidere manualmente quali clip transcodificare: deve inviare il job e lasciare preflight e normalizzazione al Master.

## Destinazione Drive

La cartella di destinazione indicata per il progetto è:

```
1TiCjzIgYaHA-0tZAs6H4zykHqUOYWs7q
```

La sottocartella del test è:

```
matt-damon-5-clips-docs-true
```

Per il video finale da 5 clip, il payload deve impostare esplicitamente `assemble_final: true` e indicare il nome della sottocartella finale. Il test precedente non lo ha fatto.

## Riavvio e manutenzione

La configurazione di rete persistente del servizio è nel drop-in systemd:

```
/etc/systemd/system/pipelinegen.service.d/network.conf
```

Il servizio è configurato per ascoltare su tutte le interfacce, con M2M abilitato e con la coda RenderingGen attiva. Dopo una modifica alla configurazione o al binario:

```bash
sudo systemctl daemon-reload
sudo systemctl restart pipelinegen.service
sudo systemctl is-active pipelinegen.service
curl -fsS http://127.0.0.1:8000/health
```

Per il normale invio dei job non serve riavviare il servizio. Non riavviare durante un rendering attivo senza prima verificare lo stato dei job.

## Verifica rapida dal client

```bash
set -a; . /path/to/computer-editor-77-01.env; set +a

curl --fail-with-body -sS "$VELOX_MASTER_URL/health" | jq .

curl -sS -o /dev/null -w '%{http_code}\n' \
  -X POST "$VELOX_MASTER_URL/api/v1/jobs" \
  -H 'Content-Type: application/json' \
  --data '{}'
```

Il primo comando deve rispondere con stato healthy. Il secondo deve rispondere `401`: conferma che l'endpoint è raggiungibile e protetto.

## Checklist di accettazione

- [x] Creator raggiungibile su `77.93.152.122:8000`.
- [x] Render master raggiungibile su `51.91.11.36:8000`.
- [x] Health e readiness verificati.
- [x] M2M abilitato sul servizio persistente.
- [x] Client `computer-editor-77-01` creato.
- [x] Secret non esposto nei log o nel repository.
- [x] Submit M2M verificato con risposta `202`.
- [x] Creator push dal `77` accettato dal render master `51`.
- [x] Task assegnato a un subworker del `51`.
- [x] Prefetch verificato sul subworker: 5 download, 10 cache hit.
- [ ] Job assemblato finale verificato con `pipeline_id: clips.v1` sull'immagine aggiornata.
- [ ] Receipt finale con artifact Drive verificato.
- [ ] Trasporto HTTPS o VPN/Tailscale configurato al posto dell'HTTP pubblico.

## Sicurezza

La password SSH/sudo usata durante il setup è stata condivisa nella conversazione. Deve essere cambiata appena possibile. Anche il secret M2M va ruotato se viene accidentalmente esposto o inserito in un log.
