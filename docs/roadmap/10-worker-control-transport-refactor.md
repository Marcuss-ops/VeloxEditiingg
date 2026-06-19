# 10 — Worker Refactor to Use ControlTransport

## Stato attuale

Il worker (`worker.go:25`) orchestra manualmente tutti i loop:

```go
func (w *Worker) Start(ctx context.Context) error {
    w.concurrencyLimiter.Start(ctx)
    if err := w.register(ctx); err != nil { return err }
    go w.heartbeatLoop(ctx)
    go w.leaseRenewLoop(ctx)
    go w.jobLoop(ctx)
    go w.commandLoop(ctx)
    <-w.stopChan
    // ...
}
```

Ogni loop conosce i dettagli HTTP (chiama `apiClient.GetJobV2()`, `apiClient.SendHeartbeat()`, ecc.).

## Stato target

Il worker conosce solo `ControlTransport`. I loop vengono sostituiti da un unico `runSession()`:

```go
func (w *Worker) Start(ctx context.Context) error {
    transport := w.selectTransport() // gRPC o HTTP in base a config
    for !w.IsStopped() {
        if err := w.runSession(ctx, transport); err != nil {
            w.logger.Warn("Session ended: %v — reconnecting", err)
            backoff := w.nextBackoff()
            time.Sleep(backoff)
        }
    }
    return nil
}

func (w *Worker) runSession(ctx context.Context, transport ControlTransport) error {
    // 1. Connect
    hello := w.buildHello()
    if err := transport.Connect(ctx, hello); err != nil {
        return err
    }
    defer transport.Close()

    // 2. Receive loop
    recvChan, err := transport.Receive(ctx)
    if err != nil {
        return err
    }

    // 3. Event loop
    for {
        select {
        case msg := <-recvChan:
            w.handleMessage(ctx, transport, msg)
        case <-ctx.Done():
            return ctx.Err()
        case <-w.stopChan:
            transport.Send(ctx, ControlMessage{Type: MsgGoodbye})
            return nil
        }
    }
}
```

Metodi separati per ogni messaggio:
- `handleJobOffer()` — valida, accetta/rifiuta, lancia esecuzione
- `handleCommand()` — esegue e invia ACK
- `handleCancelJob()` — cancella job
- `handleDrain()` — attiva drain mode
- `sendHeartbeat()` — invio periodico (goroutine separata)
- `sendLeaseRenewals()` — rinnovo periodico lease (goroutine separata)
- `sendProgress()` — aggiornamento progresso durante esecuzione

## File coinvolti

| File | Azione |
|---|---|
| `RemoteCodex/native/worker-agent-go/internal/worker/worker.go` | Riscrivere `Start()` e `runSession()` |
| `RemoteCodex/native/worker-agent-go/internal/worker/session.go` | Nuovo: `runSession`, `handleMessage`, `selectTransport` |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_comms.go` | Estrarre `register()` in `buildHello()` |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_jobs.go` | Sostituire `jobLoop` con `handleJobOffer` |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_commands.go` | Sostituire `commandLoop` con `handleCommand` |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_types.go` | Aggiungere `transport` al `Worker` struct |
| `RemoteCodex/native/worker-agent-go/pkg/config/config.go` | Aggiungere `ControlTransport string`, `ControlGRPCURL string`, `FallbackToHTTPPolling bool` |

## Definition of Done

- [ ] `Worker` struct ha campo `transportFactory func() ControlTransport`
- [ ] `selectTransport()` sceglie in base a config:
  ```go
  if cfg.ControlTransport == "grpc" { return NewGRPCStreamTransport(cfg) }
  return NewPollingHTTPTransport(cfg)
  ```
- [ ] `Start()` implementa loop infinito con `runSession()` + backoff
- [ ] `runSession()` usa solo `ControlTransport`, nessun riferimento a `api.Client` o endpoint HTTP
- [ ] `handleMessage()` dispatcha per `ControlMessageType`
- [ ] `handleJobOffer()` sostituisce `jobLoop` + `pollJob`
- [ ] `handleCommand()` sostituisce `commandLoop`
- [ ] `sendHeartbeat()` goroutine invia heartbeat periodici via `transport.Send()`
- [ ] `sendLeaseRenewals()` goroutine invia lease renewal via `transport.Send()`
- [ ] `sendProgress()` aggiorna progresso durante esecuzione job
- [ ] Test e2e con `PollingHTTPTransport`: comportamento identico a oggi
- [ ] Test con mock `ControlTransport`: verifica gestione messaggi
- [ ] Config `control_transport` accetta `"http"` e `"grpc"`
- [ ] Config `fallback_to_http_polling: true` → se gRPC fallisce, usa HTTP

## Criteri di test

```bash
# Unit test: gestione messaggi con mock transport
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestSession

# Integration: worker con PollingHTTPTransport
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestWorkerIntegration
```

## Dipendenze

- **06** (ControlTransport) — prerequisito
- **09** (PollingHTTPTransport) — per test e2e senza gRPC
- **08** (GRPCStreamTransport) — per test con gRPC
- **04** (re-registration loop) — già integrato in `Start()` con backoff
