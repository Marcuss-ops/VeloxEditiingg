# 11 — Shadow Mode: Stream + Polling in Parallelo

## Stato attuale

Non esiste shadow mode. Il worker comunica esclusivamente via HTTP polling. Non c'è alcun meccanismo
per confrontare due trasporti in parallelo.

## Stato target

Shadow mode è uno stato transitorio in cui il worker:

1. **Mantiene il polling HTTP come trasporto primario** per il claim dei job
2. **Apre anche il gRPC stream** come trasporto shadow
3. Confronta metriche e comportamento tra i due canali
4. Il claim atomico SQLite impedisce che lo stesso job venga assegnato due volte
5. Dopo un periodo di shadow mode senza regressioni, si può passare al push reale (task 12)

```
Worker
  ├── PollingHTTPTransport (primario — claim reale)
  │     ├── GET /api/v1/queue/job → claim atomico SQLite
  │     ├── POST heartbeat
  │     └── POST result / complete
  │
  └── GRPCStreamTransport (shadow — sola osservazione)
        ├── riceve JobOffer → logga e confronta con claim HTTP
        ├── invia heartbeat → solo metriche
        └── riceve Command → confronta con comandi HTTP
```

Il worker:

1. Apre entrambi i trasporti
2. Per ogni `JobOffer` sullo stream shadow, **non fa il claim**, ma:
   - Registra il job_id
   - Quando il polling HTTP riceve lo stesso job (o uno diverso), confronta
   - Logga discrepanze
3. Le metriche Prometheus tracciano:
   - `velox_shadow_job_offer_total`
   - `velox_shadow_job_offer_match_total` (match con claim HTTP)
   - `velox_shadow_job_offer_mismatch_total`
   - `velox_shadow_latency_seconds` (differenza tra notifica stream e claim HTTP)

## File coinvolti

| File | Azione |
|---|---|
| `RemoteCodex/native/worker-agent-go/internal/worker/shadow.go` | Nuovo: shadow mode logic |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker.go` | Modificare: `Start()` supporta dual transport |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_types.go` | Aggiungere `shadowTransport` e metriche |
| `RemoteCodex/native/worker-agent-go/pkg/config/config.go` | Aggiungere `ShadowMode bool`, `ShadowModeGRPCURL string` |

## Definition of Done

- [ ] Config `shadow_mode: true` attiva il dual transport
- [ ] Worker apre `PollingHTTPTransport` (primario) e `GRPCStreamTransport` (shadow)
- [ ] `GRPCStreamTransport` in shadow mode:
  - Riceve `JobOffer`, `Command`, `Ping` dallo stream
  - **NON** invia `JobAccepted` (solo osservazione)
  - Invia `Heartbeat` con label `shadow=true`
  - Riceve comandi e li confronta con quelli ricevuti via polling
- [ ] `handleShadowJobOffer()`:
  - Registra il `job_id` offerto con timestamp
  - Quando il polling HTTP riceve un job, verifica se è lo stesso
  - Se diverso, logga WARN con entrambi i job_id
- [ ] Metriche Prometheus per shadow mode
- [ ] Log strutturato: `[SHADOW]` prefix per tutti i log shadow
- [ ] Nessun job viene perso o assegnato due volte (il claim SQLite è l'arbitro)
- [ ] Se lo stream shadow muore, il worker continua con solo HTTP (nessun impatto)
- [ ] Test: shadow mode con stream mock → verifica metriche e log

## Criteri di test

```bash
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestShadowMode

# Metriche
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestShadowMetrics
```

## Dipendenze

- **06** (ControlTransport interface) — prerequisito
- **08** (GRPCStreamTransport) — per lo stream shadow
- **09** (PollingHTTPTransport) — per il trasporto primario
- **10** (worker refactor) — il worker deve già usare `ControlTransport`
