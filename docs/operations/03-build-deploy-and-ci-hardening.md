# Runbook operativo 03 — Hardening build, deploy, CI e rilascio

Status: operativo

Data snapshot: 2026-06-21

Parte 1 di 4 — Build, immagini e gate di verifica. Continuazioni: [03a — Smoke e recovery](03a-smoke-and-recovery.md) · [03b — Governance, release e deploy](03b-governance-release-and-operations.md) · [03c — Rollout e accettazione](03c-rollout-and-acceptance.md).

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

---

Continue with [03a — Smoke test e recovery](03a-smoke-and-recovery.md).
