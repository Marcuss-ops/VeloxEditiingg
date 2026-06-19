# 12 — Push Reale dei Job via Stream

## Stato attuale

Il worker fa polling HTTP ogni 5 secondi (`GET /api/v1/queue/job`). Il master non ha alcun meccanismo
di push/notifica. Il claim del job avviene in transazione SQLite durante la richiesta HTTP.

## Stato target

Il master invia `JobOffer` (o `JobAvailable`) sullo stream gRPC. Il worker risponde con `JobAccepted`
(o chiama `ClaimNext` via API). Il database rimane l'arbitro: lo stream è solo notifica, non
source of truth.

### Modello consigliato: notifica + claim API

```
Master                            Worker
  │                                  │
  │ ──── JobAvailable ────────────→ │  (notifica: "c'è un job per te")
  │                                  │
  │  ←─── ClaimNext (API HTTP) ──── │  (claim atomico in SQLite)
  │                                  │
  │ ──── JobOffer (lease) ────────→ │  (risultato del claim)
  │                                  │
  │  ←─── JobAccepted ───────────── │  (conferma)
```

Questo modello mantiene il claim atomico SQLite come unica source of truth e impedisce che il
trasporto (stream gRPC) diventi autoritativo. Se lo stream muore dopo la notifica, il job non
viene assegnato e scade. Se il worker muore dopo il claim ma prima di accettare, il lease
scade e il job torna disponibile.

### Alternativa: push diretto

```
Master                            Worker
  │                                  │
  │ ──── JobOffer ────────────────→ │  (con lease_id)
  │                                  │
  │  ←─── JobAccepted ───────────── │  (conferma)
```

In questo modello il master pre-claima il job prima di offrirlo. Il worker può accettare o
rifiutare. Se rifiuta, il master rilascia il claim. Meno robusto perché il master deve gestire
il ciclo di vita del lease pre-assegnato.

## File coinvolti

| File | Azione |
|---|---|
| `DataServer/internal/transport/grpc_server.go` | Modificare: inviare `JobAvailable` quando un job è in coda per il worker |
| `DataServer/internal/queue/orchestrator.go` o `orchestrator_events.go` | Modificare: hook per notificare nuova disponibilità job |
| `DataServer/internal/services/jobs/service.go` | Modificare: `NotifyWorker` o simile |
| `RemoteCodex/native/worker-agent-go/internal/worker/session.go` | Modificare: `handleJobAvailable` → `ClaimNext` HTTP |
| `RemoteCodex/native/worker-agent-go/internal/transport/grpc_transport.go` | Modificare: supporto `JobAvailable` + `JobOffer` inbound |
| `DataServer/internal/handlers/server/jobs/` | Verificare che `ClaimNext` sia idempotente e sicuro da chiamare dopo `JobAvailable` |

## Definition of Done

- [ ] `GRPCControlServer` riceve notifica dal job scheduler quando nuovi job sono disponibili
- [ ] `GRPCControlServer` matcha il job type con le capabilities dei worker connessi
- [ ] Invia `JobAvailable` (solo notifica, senza lease) ai worker compatibili
- [ ] Il worker, ricevuto `JobAvailable`, chiama `POST /api/v1/queue/job` (claim HTTP)
- [ ] Se il claim ha successo, il worker invia `JobAccepted` sullo stream
- [ ] Se il claim fallisce (job già preso da altro worker), il worker ignora
- [ ] Il master traccia: `JobAvailable` inviati vs `ClaimNext` ricevuti (metriche)
- [ ] Il claim atomico SQLite rimane identico a oggi (nessuna modifica)
- [ ] Test: 2 worker ricevono `JobAvailable` per lo stesso job → solo uno fa claim
- [ ] Test: stream muore dopo `JobAvailable` → job non assegnato, scade normalmente
- [ ] Test: con `shadow_mode=true`, le notifiche push sono shadow, il polling rimane primario
- [ ] Config `job_delivery: polling` (default) vs `job_delivery: push`

## Criteri di test

```bash
# Master: notifica job
cd refactored/DataServer && go test ./internal/transport/... -v -run TestJobNotification

# Worker: handleJobAvailable
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestJobPush

# Integration: 2 worker, 1 job, solo uno lo prende
cd refactored && go test ./... -tags=integration -v -run TestJobPushConcurrency
```

## Dipendenze

- **08** (GRPCStreamTransport) — prerequisito
- **10** (worker refactor) — il worker deve già usare `ControlTransport`
- **11** (shadow mode) — consigliato prima del push reale per validazione
