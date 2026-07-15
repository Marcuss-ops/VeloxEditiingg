# Runbook operativo 03c — Rollout e accettazione

Status: operativo

Data snapshot: 2026-06-21

Document set: [03 — Build e CI](03-build-deploy-and-ci-hardening.md) · [03a — Smoke e recovery](03a-smoke-and-recovery.md) · [03b — Governance, release e deploy](03b-governance-release-and-operations.md) · **03c — Rollout e accettazione**.

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
   di deprecazione è tollerabile).
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
