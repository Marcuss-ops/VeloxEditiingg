# 03 — MarkCommandDelivered nel flusso di polling comandi

## Stato attuale

La funzione `MarkCommandDelivered(commandID)` esiste in `store_worker_control.go:124-132`:

```go
func (s *SQLiteStore) MarkCommandDelivered(commandID string) error {
    _, err := s.db.Exec(
        `UPDATE worker_commands SET status = 'delivered', delivered_at = ?,
                attempt_count = attempt_count + 1
         WHERE command_id = ? AND status = 'pending'`,
        now, commandID,
    )
    return err
}
```

**Non viene mai chiamata.** I comandi passano direttamente da `pending` ad `acked`, senza mai
transitare per `delivered`. Questo significa che non c'è traccia di:

- Quando un comando è stato effettivamente consegnato al worker
- Quanti tentativi di consegna sono stati fatti
- Se il worker ha ricevuto il comando ma non l'ha ancora processato

## Stato target

1. Quando il master risponde a `GET /api/workers/commands` (polling), marca i comandi restituiti come `delivered`.
2. Il `CommandManager.GetPendingCommands()` (o un wrapper HTTP) chiama `MarkCommandDelivered` per ogni
   comando restituito al worker.
3. `delivered_at` viene popolato con il timestamp di consegna.
4. `attempt_count` viene incrementato ad ogni consegna.
5. I comandi in stato `delivered` ma non `acked` entro la scadenza vengono marcati `expired` da
   `ExpireCommands()` (già implementato).

Il ciclo di vita completo diventa:

```
pending → delivered → acked
pending → delivered → expired (timeout)
pending → expired (mai consegnato, timeout)
```

## File coinvolti

| File | Azione |
|---|---|
| `DataServer/internal/workers/commands.go` | Modificare: `GetPendingCommands` (o nuovo metodo) marca delivered dopo fetch |
| `DataServer/internal/handlers/remote/workers/lifecycle/` | Modificare: handler `GET /api/workers/commands` chiama `MarkCommandDelivered` |
| `DataServer/internal/store/store_worker_control.go` | Nessuna modifica (già pronto) |

## Definition of Done

- [ ] `CommandManager` espone metodo `GetPendingCommandsAndMarkDelivered(workerID)` che:
  - Fetcha i comandi pending
  - Per ognuno, chiama `MarkCommandDelivered(commandID)`
  - Ritorna i comandi (ora in stato `delivered`)
- [ ] Handler `GET /api/workers/commands` usa `GetPendingCommandsAndMarkDelivered`
- [ ] `ExpireCommands()` marca come `expired` anche i comandi in stato `delivered` oltre la scadenza
  (verificare che sia già così o modificare)
- [ ] Test: comando pending → dopo polling → stato `delivered` con `delivered_at` popolato
- [ ] Test: comando delivered → dopo ack → stato `acked`
- [ ] Test: comando delivered → dopo expiry → stato `expired`
- [ ] Test: `attempt_count` incrementa a ogni polling (re-delivery)

## Criteri di test

```bash
cd refactored/DataServer && go test ./internal/workers/... -v -run TestDelivered
cd refactored/DataServer && go test ./internal/store/... -v -run TestCommandLifecycle
```

## Dipendenze

- Complementare a 02 (ACK by ID) — insieme completano il ciclo di vita comandi.
- Nessuna hard dependency.
