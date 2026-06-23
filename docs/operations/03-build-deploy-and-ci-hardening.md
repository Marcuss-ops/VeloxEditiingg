# Runbook operativo 03 — Hardening build, deploy, CI e rilascio

Status: operativo

Data snapshot: 2026-06-21

Obiettivo: rendere master e worker costruibili da checkout pulito, eliminare prerequisiti nascosti, fare della CI il gate reale di `main`, rendere i rilasci riproducibili e verificare l'intero percorso Job -> Task -> Worker -> Artifact.

## 1. Principi non negoziabili

1. Un checkout pulito deve poter costruire ogni immagine senza binari precompilati locali.
2. La stessa entrypoint di verifica deve essere usata localmente e in CI.
3. Nessun push su `main` deve bypassare i check obbligatori.
4. Le immagini devono essere riproducibili, versionate e tracciabili al commit.
5. I runtime container devono essere non-root e contenere solo dipendenze necessarie.
6. I segreti non entrano in immagini, log, repository o artifact CI.
7. Un release è valido solo dopo smoke test master-worker reale.
8. Il rollback deve usare artifact già costruiti e database compatibile, non una ricompilazione di emergenza.

## 2. Stato target

```text
checkout pulito
  -> make verify
      -> arch guards
      -> gofmt/vet/race tests
      -> protobuf reproducibility
      -> C++ engine build/tests
      -> master image build
      -> worker image build autonomo
      -> smoke test master + worker
      -> release artifacts con digest
```

Il worker Dockerfile non deve dipendere da:

```text
RemoteCodex/native/worker-agent-go/bin/velox-worker-agent
```

precompilato fuori da Docker.

## 3. Rendere il Dockerfile worker autosufficiente

### 3.1 Problema da eliminare

Il Dockerfile worker corrente richiede un binario Go già presente nel build context. Questo crea tre pipeline diverse:

- build Go locale;
- build C++ nel Dockerfile;
- copia del binario locale nel runtime.

Il risultato dipende dallo stato della working tree e dall'ambiente che ha prodotto il binario.

### 3.2 Build context canonico

Usare la root repository come unico build context:

```bash
docker build \
  -f RemoteCodex/native/worker-agent-go/Dockerfile \
  -t velox-worker:dev \
  .
```

Questo permette al Dockerfile di accedere a:

```text
shared/
RemoteCodex/native/worker-agent-go/
RemoteCodex/native/video-engine-cpp/
RemoteCodex/scripts/
VERSION.txt
```

Aggiornare CI, Makefile e documentazione affinché nessun comando usi più `RemoteCodex` come context.

### 3.3 Stage Go builder

Aggiungere uno stage dedicato:

```dockerfile
FROM golang:1.25.8-bookworm AS go-builder

WORKDIR /src
COPY shared/go.mod shared/go.sum ./shared/
COPY RemoteCodex/native/worker-agent-go/go.mod \
     RemoteCodex/native/worker-agent-go/go.sum \
     ./RemoteCodex/native/worker-agent-go/

WORKDIR /src/RemoteCodex/native/worker-agent-go
RUN go mod download

WORKDIR /src
COPY shared/ ./shared/
COPY RemoteCodex/native/worker-agent-go/ ./RemoteCodex/native/worker-agent-go/
COPY VERSION.txt ./VERSION.txt

WORKDIR /src/RemoteCodex/native/worker-agent-go
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags "-s -w -X main.Version=${VERSION} -X main.BuildTime=${BUILD_TIME}" \
    -o /out/velox-worker-agent \
    ./cmd/velox-worker-agent
```

Adattare `CGO_ENABLED` solo se il worker importa realmente librerie CGO. Non assumere staticità senza verificare con:

```bash
file /out/velox-worker-agent
ldd /out/velox-worker-agent || true
```

### 3.4 Stage C++ builder

Mantenere uno stage separato per il video engine, ma copiarne le sorgenti dalla root context:

```dockerfile
COPY RemoteCodex/scripts/build-video-engine.sh /usr/local/bin/build-video-engine.sh
COPY RemoteCodex/native/video-engine-cpp /src/video-engine-cpp
```

Il build deve fallire se:

- il binario non viene prodotto;
- il binario non è eseguibile;
- mancano librerie runtime;
- un test nativo obbligatorio fallisce.

Aggiungere, se presenti test CMake:

```bash
cmake --build /tmp/velox-engine --parallel
ctest --test-dir /tmp/velox-engine --output-on-failure
```

### 3.5 Runtime image

Il runtime deve copiare esclusivamente:

```dockerfile
COPY --from=go-builder /out/velox-worker-agent /usr/local/bin/velox-worker-agent
COPY --from=cpp-builder /usr/local/bin/velox_video_engine /usr/local/bin/velox_video_engine
COPY RemoteCodex/scripts/worker-entrypoint.sh /usr/local/bin/worker-entrypoint.sh
```

Requisiti:

- utente non-root con UID/GID stabile;
- directory scrivibili create esplicitamente;
- nessun compilatore nel runtime;
- healthcheck reale;
- `tini` o gestione segnali equivalente solo se il processo corrente non gestisce correttamente SIGTERM;
- nessun token o file env copiato nell'immagine.

### 3.6 Verifiche del worker image

```bash
docker build -f RemoteCodex/native/worker-agent-go/Dockerfile -t velox-worker:test .
docker run --rm --entrypoint /usr/local/bin/velox-worker-agent velox-worker:test --version
docker run --rm --entrypoint /usr/local/bin/velox_video_engine velox-worker:test --version || true
docker inspect velox-worker:test --format '{{.Config.User}}'
```

Il valore user non deve essere vuoto o `root`.

## 4. Hardening dell'immagine master

Il Dockerfile master è già multi-stage, ma deve essere verificato rispetto ai seguenti requisiti.

### 4.1 Build riproducibile

Usare build args derivati dal release job:

```text
VERSION = contenuto di VERSION.txt o tag verificato
BUILD_TIME = timestamp UTC del job
COMMIT_SHA = SHA completo
```

Aggiungere label OCI:

```dockerfile
LABEL org.opencontainers.image.title="Velox Master"
LABEL org.opencontainers.image.version=$VERSION
LABEL org.opencontainers.image.revision=$COMMIT_SHA
LABEL org.opencontainers.image.created=$BUILD_TIME
LABEL org.opencontainers.image.source="https://github.com/Marcuss-ops/VeloxEditiingg"
```

Il server deve esporre versione e commit in un endpoint diagnostico non sensibile oppure nei log di startup.

### 4.2 Runtime filesystem

Verificare che l'utente `velox` possa scrivere solo nelle directory richieste:

- database/runtime data;
- staging blob;
- temporary processing;
- log, se non inviati a stdout.

Preferire log su stdout/stderr. Non fare `chown -R /app` senza necessità.

### 4.3 Health e readiness

Separare:

- liveness: processo HTTP risponde;
- readiness: database, outbox, blobstore e componenti obbligatori sono pronti.

Target:

```text
/health/live
/health/ready
```

Durante shutdown, readiness deve diventare false prima di chiudere listener e worker session.

## 5. Un solo comando canonico di verifica

`make verify` resta l'unica entrypoint completa.

Non duplicare in GitHub Actions la sequenza dei test. La workflow deve:

1. checkout completo;
2. setup toolchain;
3. cache;
4. installazione dipendenze native;
5. `make verify`.

Il Makefile non deve replicare internamente la logica già presente negli script, eccetto alias sottili.

### 5.1 Struttura consigliata

```text
make verify-fast
  architecture
  secrets
  migrations
  ownership
  gofmt
  go vet
  go test -race

make verify-native
  cmake configure/build
  ctest

make verify-images
  docker build master
  docker build worker
  image smoke checks

make verify-smoke
  master + worker integration

make verify
  tutti i target precedenti
```

Gli script restano la fonte di verità; i target Make sono dispatcher.

## 6. Aggiungere i controlli mancanti

### 6.1 Dependency scan

Per Go:

```bash
go version -m <binary>
go list -m all
govulncheck ./...
```

Usare `govulncheck` con versione fissata nella CI. Non introdurre aggiornamenti automatici durante il gate.

Per container, usare uno scanner supportato dall'organizzazione, con policy esplicita:

- bloccare vulnerabilità critical sfruttabili;
- registrare eccezioni con owner e scadenza;
- non bloccare su risultati senza fix senza una policy concordata.

### 6.2 Secret scan

Il check deve coprire:

- working tree;
- diff rispetto a `origin/main`;
- esempi deploy;
- history recente o intera history in un job dedicato.

Bloccare almeno:

- chiavi private;
- token GitHub/Google;
- OAuth client secrets reali;
- IP/hostname di produzione se classificati sensibili;
- inventory e vault non cifrati.

### 6.3 Generated code reproducibility

Per protobuf:

```bash
./scripts/gen-proto.sh
git diff --exit-code -- proto shared/controltransport/pb
```

Per ogni altro generated file aggiungere un comando equivalente.

### 6.4 Artifact cleanliness

Fallire se il repository contiene:

```text
node_modules/
dist/
binari Go/C++
*.tsbuildinfo
output/
cache runtime
SQLite DB
.env reali
```

Eccezioni solo per fixture piccole e dichiarate.

## 7. Smoke test master-worker end-to-end

Il release gate deve verificare il percorso reale, non soltanto che i container si avviino.

### 7.1 Ambiente smoke

Avviare:

- master con SQLite temporaneo;
- blobstore filesystem temporaneo;
- un worker CPU;
- un asset di test piccolo;
- un Job/Task deterministico con durata breve.

Usare Docker Compose dedicato, per esempio:

```text
deploy/smoke/compose.yml
scripts/ci/smoke-master-worker.sh
```

Il compose non deve contenere segreti reali.

### 7.2 Sequenza test

1. Avviare master.
2. Attendere readiness.
3. Avviare worker.
4. Verificare registrazione e heartbeat.
5. Sottomettere un Job di test.
6. Verificare creazione TaskGraph.
7. Verificare claim Task.
8. Verificare TaskAttempt RUNNING.
9. Attendere artifact output.
10. Verificare hash e finalizzazione.
11. Verificare Task e Job SUCCEEDED.
12. Riavviare master e confermare che lo stato resti leggibile.
13. Spegnere con SIGTERM e verificare uscita pulita.

### 7.3 Assert minimi

- un solo worker riceve la Task;
- nessuna Task resta RUNNING dopo completamento;
- artifact esiste nel BlobStore;
- outbox non contiene eventi bloccati non giustificati;
- nessun panic nei log;
- readiness diventa verde;
- teardown non supera il timeout previsto.

Conservare in CI:

- log master;
- log worker;
- report smoke JSON;
- eventuale DB SQLite come artifact solo su fallimento, dopo rimozione di dati sensibili;
- checksum degli output.

## 8. Test di recovery

Aggiungere scenari separati:

### 8.1 Master restart

- Task READY prima del restart;
- master riavviato;
- scheduler ricostruisce stato da repository;
- Task viene assegnata una sola volta.

### 8.2 Worker disconnect

- worker riceve Task;
- processo worker viene terminato;
- lease scade;
- Task torna recuperabile;
- nuovo attempt viene creato;
- report tardivo del vecchio attempt viene rifiutato o registrato come stale.

### 8.3 Upload interrotto

- upload chunked incompleto;
- master restart;
- sessione upload recuperata o marcata fallita secondo contratto;
- nessun Job viene finalizzato senza artifact verificato.

### 8.4 Database lock/concorrenza

Con SQLite:

- più writer concorrenti controllati;
- nessun `database is locked` non gestito nel workload supportato;
- busy timeout e pool misurati, non soltanto aumentati;
- test sotto race detector.

## 9. Branch protection e policy GitHub

Configurare `main` con:

- pull request obbligatoria;
- required status check `make verify`;
- branch aggiornato prima del merge;
- conversazioni review risolte;
- force push disabilitato;
- branch deletion disabilitata;
- amministratori inclusi nelle regole, salvo procedura break-glass documentata;
- linear history o merge strategy scelta e coerente.

Il repository supporta merge commit, squash e rebase: sceglierne una strategia primaria. Raccomandazione:

- squash per PR piccole;
- titolo Conventional Commit;
- nessun merge di checklist vuota;
- nessun push diretto su `main` dopo l'attivazione della protezione.

Il break-glass deve richiedere:

- incidente reale;
- issue associata;
- motivo nel commit;
- verifica post-merge;
- review retrospettiva.

## 10. Release pipeline

### 10.1 Versione

`VERSION.txt` è la fonte di verità. Il tag deve corrispondere esattamente:

```bash
VERSION="$(tr -d '[:space:]' < VERSION.txt)"
test "$GITHUB_REF_NAME" = "v$VERSION"
```

Non pubblicare se tag e file divergono.

### 10.2 Artifact

Pubblicare:

- immagine master;
- immagine worker;
- digest SHA256;
- SBOM;
- changelog;
- commit SHA;
- eventuali binari standalone solo se realmente supportati.

Tag immagini:

```text
velox-master:<version>
velox-master:sha-<shortsha>
velox-worker:<version>
velox-worker:sha-<shortsha>
```

`latest` può essere aggiornato soltanto dopo smoke test riuscito.

### 10.3 Promotion

Separare build da promotion:

1. build una volta;
2. testare gli stessi digest;
3. promuovere i digest verificati;
4. non ricostruire tra staging e production.

## 11. Deploy operativo

### 11.1 Preflight

Prima del deploy:

```bash
make verify
```

Verificare:

- backup DB completato;
- migrazioni previste e compatibili;
- spazio disco;
- directory e ownership;
- worker allowlist;
- certificati gRPC;
- BlobStore raggiungibile;
- digest immagini.

### 11.2 Ordine deploy

Per cambi protocol-compatible:

1. master compatibile con worker vecchi e nuovi;
2. aggiornamento worker graduale;
3. verifica flotta;
4. rimozione compatibilità in una release successiva.

Per cambi protocol-incompatible:

- introdurre prima una versione di transizione;
- negoziare protocol version esplicita;
- rifiutare versioni incompatibili con errore chiaro;
- non usare fallback silenziosi.

### 11.3 Canary worker

Aggiornare inizialmente un worker:

- verificare registrazione;
- eseguire Job smoke;
- osservare errori e performance;
- poi espandere alla flotta.

Il canary non deve ricevere lavori di produzione incompatibili durante il test.

## 12. Rollback

Preparare prima del deploy:

- digest immagini precedenti;
- backup database;
- matrice compatibilità schema/app;
- comandi systemd/compose per tornare indietro;
- responsabile decisione rollback.

Regole:

1. le migrazioni sono forward-only;
2. non fare downgrade applicativo se il vecchio binario non comprende il nuovo schema;
3. in quel caso usare una release correttiva compatibile, non rollback cieco;
4. non cancellare artifact prodotti durante il deploy fallito prima dell'audit;
5. fermare nuovi claim prima di spegnere worker o master.

## 13. Osservabilità minima per produzione

Registrare metriche e log strutturati per:

- worker connessi/offline/draining;
- Task READY/RUNNING/FAILED;
- lease scadute;
- durata claim;
- TaskAttempt per executor/versione;
- errori upload;
- outbox backlog;
- database busy/latency;
- cache hit bytes;
- render/encode/upload duration;
- graceful shutdown timeout.

Ogni log deve includere gli ID disponibili:

```text
job_id
task_id
attempt_id
worker_id
session_id
executor_id
artifact_id
```

Non loggare token, header Authorization o payload sensibili.

## 14. Sequenza consigliata dei cambi

1. Nuovo build context root per worker.
2. Stage Go builder nel Dockerfile worker.
3. Aggiornamento `make verify` e workflow image.
4. Smoke check dei due container.
5. Generated-code e artifact cleanliness checks.
6. Smoke test master-worker end-to-end.
7. Recovery test master restart.
8. Recovery test worker disconnect.
9. Branch protection e required checks.
10. Release pipeline con digest/SBOM/promotion.
11. Runbook deploy e rollback verificato.

Ogni cambiamento deve partire da `origin/main` aggiornato e contenere test mirati.

## 14.1 Sequenza di rollout protocol v3 (worker-first / master-second)

Il bump a `ProtocolVersionCurrent = "v3"` (luglio 2026) chiude il ciclo
dei typed metrics (PR-5 / F2). La master handshake accetta
contemporaneamente "v3" e "2026-06-worker-v1" (legacy) — ma NON tutte
le combinazioni di label set / payload sono retro-compatibili. La
sequenza deve quindi essere:

### 14.1.1 Worker PRIMA

1. Costruire il nuovo bundle/immagine del worker con
   ProtocolVersion="v3" (`velox_build_info.json` + costante in
   `pkg/config/config.go`).
2. Aggiornare la costante `Velox Worker Agent: protocol_version`.
3. Push immagine con tag `<version>-rc1` (non ancora scambiato
   col master di produzione).
4. Confermare che il worker prova ad aprire lo stream verso il
   master in produzione — la master handshake Risponde OK con
   [DEPRECATED] log (legacy path, in attesa del master upgrade).

### 14.1.2 Master DOPO

1. Costruire la nuova immagine master con `protocol_version="v3"`
   nella envelope HelloAck + `IsSupportedProtocol("v3") /
   IsSupportedProtocol("2026-06-worker-v1")` nel codepath gRPC.
2. Push immagine `velox-master:v3.0.0-rc1` solo dopo che
   almeno un worker v3 è registrato correttamente.
3. Cutover del master — la nuova binario rilegge worker sia v3
   che v1 (il codepath IsDeprecatedProtocol emette solo una log
   di deprecazione \u00e8 tollerabile).
4. Continuare a osservare `[DEPRECATED]` logs fino al drenaggio
   della flotta v1.

### 14.1.3 Drenaggio del legacy e rimozione

Dopo 6 mesi (audit-canonical grace period) si può rimuovere
`ProtocolVersionLegacy` da `SupportedProtocolVersions` e far
fallire i worker v1 con gRPC `FailedPrecondition`. La rimozione
NON deve avvenire in un'unica release; decrementare il set
amesso su due release successive (prima: warning esplicito,
poi: drop secco).

### 14.1.4 Cosa NON fare

- Non rilasciare master v3 con worker v1 collegati a Job che
  richiedono typed `TaskExecutionMetrics` (il master
  retrocompatibile ma i metric counters tornano a zero).
- Non assumere "backwards-compatible" = "forward-compatible":
  worker v3 + master v1 NON funziona (master v1 rifiuta v3
  con FailedPrecondition).
- Non bumpare `ProtocolVersionCurrent` senza allineare
  worker-agent-go + DataServer nello stesso commit.

## 15. Definition of done

L'hardening è completo quando:

- master e worker si costruiscono da checkout pulito con un solo comando;
- il Dockerfile worker non copia binari precompilati dalla working tree;
- `make verify` costruisce entrambe le immagini;
- CI esegue `make verify` su ogni PR e push autorizzato;
- `main` è protetto da push diretti;
- protobuf e altri generated file sono riproducibili;
- lo smoke test esegue un Job reale fino all'artifact verificato;
- master restart e worker disconnect sono coperti;
- release artifact hanno versione, commit, digest e SBOM;
- deploy e rollback usano gli stessi digest testati;
- nessun segreto entra negli artifact;
- shutdown è pulito e readiness è corretta.

## 16. Comandi finali di accettazione

```bash
git fetch origin
git status -sb
make verify

docker build -f DataServer/Dockerfile -t velox-master:acceptance .
docker build -f RemoteCodex/native/worker-agent-go/Dockerfile -t velox-worker:acceptance .

docker inspect velox-master:acceptance --format '{{.Config.User}}'
docker inspect velox-worker:acceptance --format '{{.Config.User}}'

./scripts/ci/smoke-master-worker.sh

git log -n 5 --oneline
```

Registrare SHA, digest immagini e risultato smoke nel release report.