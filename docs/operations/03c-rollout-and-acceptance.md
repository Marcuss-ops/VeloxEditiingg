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
dei typed metrics (PR-5 / F2). La handshake del master accetta
esclusivamente `v3`: versioni vuote, `v1`, `v2` e identificatori legacy
vengono rifiutati con `FailedPrecondition`. La sequenza operativa è quindi
worker-first, con verifica della flotta prima del cutover:

### 14.1.1 Worker PRIMA

1. Costruire il nuovo bundle/immagine del worker con
   ProtocolVersion="v3" (`velox_build_info.json` + costante in
   `pkg/config/config.go`).
2. Aggiornare la costante `Velox Worker Agent: protocol_version`.
3. Push immagine con tag `<version>-rc1` (non ancora scambiato
   col master di produzione).
4. Confermare che il worker apre lo stream verso il master con
   `protocol_version=v3` e capability obbligatorie valide.

### 14.1.2 Master DOPO

1. Costruire la nuova immagine master con `protocol_version="v3"`
   nella envelope HelloAck; `IsSupportedProtocol("v3")` è l'unico
   percorso ammesso nel codepath gRPC.
2. Push immagine `velox-master:v3.0.0-rc1` solo dopo che
   tutti i worker candidati risultano configurati a `v3`.
3. Cutover del master e verifica che ogni worker legacy venga
   rifiutato prima della creazione di snapshot/sessione.

### 14.1.3 Gestione dei worker legacy

I worker con protocollo legacy devono essere drenati o aggiornati prima
del deploy. Non esiste un grace period runtime e non esiste un percorso
di compatibilità nel master. Gli identificatori legacy restano ammessi
soltanto nei test negativi e nella documentazione storica.

### 14.1.4 Cosa NON fare

- Non rilasciare worker con `protocol_version` diverso da `v3`.
- Non assumere che un template legacy sia compatibile: il master
  rifiuta il worker prima dell'ammissione durevole.
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
