# 04 — Automatic Re-Registration Loop with Backoff

## Stato attuale

`worker.go:25` — Il metodo `Start()` chiama `register()` **una volta sola**:

```go
func (w *Worker) Start(ctx context.Context) error {
    w.concurrencyLimiter.Start(ctx)
    if err := w.register(ctx); err != nil {
        return fmt.Errorf("failed to register with master: %w", err)
    }
    // ... avvia heartbeat, job loop, command loop ...
}
```

Se `register()` fallisce, `Start()` ritorna errore e il worker si ferma. Non esiste:
- Retry con backoff
- Riconnessione automatica dopo disconnect
- State machine di connessione
- Resume della sessione dopo re-registration

Oggi il recovery è delegato a systemd/Docker (restart del container/processo). Funziona, ma non è
un reconnect applicativo robusto.

## Stato target

Il worker implementa un **connection state machine** con re-registration automatica:

```
DISCONNECTED
    ↓
CONNECTING ───(fallito, backoff)──→ CONNECTING
    ↓ (riuscito)
AUTHENTICATING
    ↓ (token ricevuto)
READY
    ↓ (stop richiesto)
DRAINING
    ↓
DISCONNECTED
```

Comportamento:

1. `Start()` non fallisce mai per errori di rete — ritenta con exponential backoff.
2. Dopo N errori consecutivi, backoff fino a un massimo configurabile (es. 5 minuti).
3. Jitter per evitare thundering herd.
4. Dopo re-registration riuscita, i loop (heartbeat, job, command, lease) ripartono
   automaticamente.
5. Se il master è irraggiungibile per troppo tempo (configurabile), il worker può:
   - Continuare a processare il job corrente (lease ancora valida)
   - Andare in stato `ERROR` e notificare
6. Il `context` passato ai loop figli deve supportare la cancellazione e ri-creazione
   durante il reconnect.

## File coinvolti

| File | Azione |
|---|---|
| `RemoteCodex/native/worker-agent-go/internal/worker/worker.go` | Riscrivere `Start()` con connection state machine |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_types.go` | Aggiungere stati `WorkerConnState` e costanti |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_comms.go` | Estrarre `register()` in `connect()` con retry |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_config.go` | Aggiungere config per max retry, backoff max |
| `RemoteCodex/native/worker-agent-go/pkg/config/config.go` | Aggiungere `MaxRegistrationRetries`, `RegistrationBackoffMax` |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_test.go` | Test per reconnect loop |

## Definition of Done

- [ ] `Worker` ha un campo `connState` di tipo `ConnectionState` con stati:
  `ConnDisconnected`, `ConnConnecting`, `ConnAuthenticating`, `ConnReady`, `ConnDraining`
- [ ] `Start()` implementa un loop infinito di connessione:
  ```
  for !stopped {
      state = CONNECTING
      err = connect()
      if err == nil { state = READY; runLoops() }
      backoff = min(backoff * 2, maxBackoff) + jitter
      sleep(backoff)
  }
  ```
- [ ] Backoff inizia da 5s, raddoppia fino a 5 minuti max
- [ ] Jitter ±25% sul backoff
- [ ] `connect()` chiama `register()` e se riceve token lo salva
- [ ] `runLoops()` avvia heartbeat, job, command, lease renewal con context cancellabile
- [ ] Su disconnect, `runLoops()` viene cancellato e il loop ricomincia
- [ ] Circuit breaker si resetta dopo una connessione riuscita
- [ ] Logging strutturato per ogni transizione di stato
- [ ] Test: simulazione master down → backoff → master up → riconnessione
- [ ] Test: 3 errori consecutivi → backoff crescente verificato
- [ ] Test: `Stop()` durante CONNECTING → uscita pulita senza retry infiniti

## Criteri di test

```bash
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestReconnect
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestBackoff
```

## Dipendenze

- Consigliato completare 01 (worker credentials) prima, così la re-registration reinvia
  la credential persistente.
- Indipendente dagli altri task.
