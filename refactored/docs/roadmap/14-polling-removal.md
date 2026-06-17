# 14 — Gradual Polling Removal

## Stato attuale

Tutti gli endpoint HTTP legacy sono attivi e sono l'unico canale di comunicazione:
```
POST /api/workers/register
POST /api/workers/unregister
POST /api/workers/heartbeat
GET  /api/workers/commands
POST /api/workers/commands/ack
POST /api/workers/status
POST /api/jobs/get          (legacy)
GET  /api/v1/queue/job
POST /api/jobs/result       (legacy)
POST /api/v1/jobs/:id/result
POST /api/jobs/complete     (legacy)
POST /api/v1/jobs/:id/complete
POST /api/jobs/lease        (legacy)
POST /api/v1/jobs/:id/lease
```

## Stato target

Dopo che lo stream gRPC è stabile (verificato via shadow mode + push reale), gli endpoint HTTP
di controllo vengono gradualmente rimossi. **Gli endpoint di data plane (asset, upload video)
rimangono HTTP**.

```
RIMANGONO HTTP (data plane):
  GET  /api/worker/assets/voiceover/:job_id/:filename
  GET  /api/worker/assets/scene-image/:job_id/:filename
  POST /api/worker/upload  (video upload)
  (futuro) GET signed S3 URL

RIMOSSI (control plane — sostituiti da gRPC):
  POST /api/workers/register     → Hello su stream
  POST /api/workers/unregister   → Goodbye su stream
  POST /api/workers/heartbeat    → Heartbeat su stream
  GET  /api/workers/commands     → Command su stream
  POST /api/workers/commands/ack → CommandAck su stream
  POST /api/workers/status       → Heartbeat su stream
  GET  /api/v1/queue/job         → JobOffer su stream
  POST /api/v1/jobs/:id/result   → JobResult su stream
  POST /api/v1/jobs/:id/complete → JobResult su stream
  POST /api/v1/jobs/:id/lease    → LeaseRenewal su stream
```

## File coinvolti

| File | Azione |
|---|---|
| `DataServer/internal/handlers/remote/workers/lifecycle/` | Marcare endpoint come deprecated, poi rimuovere |
| `DataServer/internal/handlers/remote/workers/worker_status.go` | Rimuovere |
| `DataServer/internal/handlers/server/jobs/` | Rimuovere endpoint di controllo, tenere data plane |
| `DataServer/cmd/server/router.go` | Rimuovere route di controllo |
| `RemoteCodex/native/worker-agent-go/pkg/api/client_endpoints.go` | Rimuovere metodi HTTP di controllo |
| `RemoteCodex/native/worker-agent-go/pkg/api/client.go` | Rimuovere costanti endpoint di controllo |

## Definition of Done

### Fase A — Deprecation (1 release, endpoint ancora attivi)
- [ ] Ogni endpoint di controllo logga WARN: `[DEPRECATED] endpoint X will be removed in vX.Y`
- [ ] Header `Deprecation: true` e `Sunset: <date>` nelle risposte
- [ ] Documentazione endpoint marcata come deprecated
- [ ] Metriche `velox_deprecated_endpoint_usage_total` per tracciare chi li usa ancora

### Fase B — Soft removal (1 release, endpoint rispondono ma non fanno nulla)
- [ ] Endpoint restituiscono `410 Gone` con messaggio "use gRPC stream"
- [ ] I worker che ancora chiamano questi endpoint vedono l'errore e (se configurati) fallbackano
  al gRPC o si fermano

### Fase C — Hard removal
- [ ] Rimozione codice handler
- [ ] Rimozione route da `router.go`
- [ ] Rimozione metodi da `api.Client`
- [ ] Pulizia test che referenziano endpoint legacy

### Cleanup finale
- [ ] Nessuna chiamata HTTP di controllo nel worker (solo `PollingHTTPTransport` se usato come fallback)
- [ ] `PollingHTTPTransport` rimosso (o tenuto come fallback opzionale)
- [ ] Config `fallback_to_http_polling` default `false`
- [ ] Documentazione aggiornata con solo endpoint data plane

## Criteri di test

```bash
# Verifica che gli endpoint di controllo non esistano più
cd refactored/DataServer && go test ./cmd/... -v -run TestRoutes

# Verifica che il worker funzioni senza endpoint HTTP di controllo
cd refactored/RemoteCodex/native/worker-agent-go && go test ./... -v -run TestGRPCOnly
```

## Dipendenze

- **12** (job push) — prerequisito: il push deve essere stabile
- **11** (shadow mode) — deve aver validato che stream ≈ polling per un periodo sufficiente
- **08** (GRPCStreamTransport) — prerequisito
- **10** (worker refactor) — il worker deve già funzionare senza HTTP di controllo
