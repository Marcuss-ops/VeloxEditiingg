# 09 — PollingHTTPTransport Wrapper

## Stato attuale

Il worker usa direttamente `api.Client` per tutte le comunicazioni HTTP. Non esiste un wrapper
che implementi l'interfaccia `ControlTransport` usando il polling HTTP attuale.

## Stato target

`PollingHTTPTransport` è un'implementazione di `ControlTransport` che, internamente, usa
gli stessi endpoint HTTP di oggi. Questo permette di:

1. Avere un'unica interfaccia (`ControlTransport`) sia per HTTP che per gRPC
2. Testare il refactor del worker (task 10) senza introdurre gRPC
3. Fornire il fallback HTTP quando gRPC non è disponibile (task 11)

```go
type PollingHTTPTransport struct {
    client         *api.Client
    workerID       string
    sessionID      string
    recvChan       chan ControlMessage
    pollCancel     context.CancelFunc
    heartbeatTicker *time.Ticker
    jobTicker      *time.Ticker
    commandTicker  *time.Ticker
    leaseTicker    *time.Ticker
    mu             sync.Mutex
    closed         bool
}
```

Funzionamento:

1. `Connect()`: chiama `client.RegisterWorker()` (HTTP). Non c'è connessione persistente da mantenere.
2. `Receive()`: avvia goroutine di polling che, a intervalli, chiamano gli endpoint HTTP e pushano
   messaggi `ControlMessage` sul canale:
   - Heartbeat → non riceve risposte (fire-and-forget)
   - Job polling → pusha `JobOffer` se trovato
   - Command polling → pusha `Command` se trovati
3. `Send()`: invia un `ControlMessage` chiamando l'endpoint HTTP appropriato:
   - `Heartbeat` → `POST /api/workers/heartbeat`
   - `JobAccepted` → già implicito nel claim (il claim HTTP = accept implicito)
   - `JobResult` → `POST /api/v1/jobs/:id/result`
   - `CommandAck` → `POST /api/workers/commands/ack`
4. `Close()`: ferma i ticker, chiama `UnregisterWorker`.

## File coinvolti

| File | Azione |
|---|---|
| `RemoteCodex/native/worker-agent-go/internal/transport/polling_transport.go` | Nuovo: implementazione |
| `RemoteCodex/native/worker-agent-go/internal/transport/polling_transport_test.go` | Nuovo: test |

## Definition of Done

- [ ] `PollingHTTPTransport` implementa `ControlTransport` (da 06)
- [ ] `Connect()` chiama `RegisterWorker()` HTTP e ottiene/salva il token
- [ ] `Receive()` restituisce `<-chan ControlMessage` con:
  - `JobOffer` quando il polling trova un job disponibile
  - `Command` quando il polling trova comandi pending
  - `Ping` periodico per mantenere vivo il canale
- [ ] `Send()` dispatchesce in base al `ControlMessageType`:
  - `Heartbeat` → `POST /api/workers/heartbeat`
  - `LeaseRenewal` → `POST /api/v1/jobs/:id/lease`
  - `JobAccepted` → (no-op, il claim HTTP è già un accept)
  - `JobRejected` → logga (non c'è endpoint dedicato oggi)
  - `JobProgress` → incluso nell'heartbeat
  - `JobResult` → `POST /api/v1/jobs/:id/result`
  - `CommandAck` → `POST /api/workers/commands/ack` con `command_id`
  - `Goodbye` → `POST /api/workers/unregister`
- [ ] `Close()` ferma tutti i ticker e chiama `UnregisterWorker`
- [ ] Thread-safe: `Send` e `Receive` possono essere chiamati da goroutine diverse
- [ ] Test: `Connect` → `Send(Heartbeat)` → verificato sul mock server
- [ ] Test: `Receive` riceve `JobOffer` dopo polling che trova job

## Criteri di test

```bash
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/transport/... -v -run TestPolling
```

## Dipendenze

- **06** (ControlTransport interface) — deve esistere
- **02** (ACK by command_id) — consigliato per `CommandAck` corretto
- Non dipende da gRPC (07, 08)
