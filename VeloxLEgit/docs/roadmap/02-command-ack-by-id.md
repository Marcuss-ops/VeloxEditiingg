# 02 — Worker ACK by command_id (non per tipo)

## Stato attuale

Il worker, dopo aver processato un comando, invia l'ACK così (`worker_commands.go:120-123`):

```go
if ackErr := w.apiClient.AckCommand(ctx, w.config.WorkerID, cmd.Command); ackErr != nil {
```

Dove `cmd.Command` è il **tipo** del comando (es. `"drain"`, `"restart_worker"`).

L'API client invia:
```go
func (c *Client) AckCommand(ctx context.Context, workerID, command string) error {
    _, err := c.doRequest(ctx, "POST", endpointAckCommand, map[string]string{
        "worker_id": workerID, "command": command,
    })
    return err
}
```

Sul master, `CommandManager.AckCommand()` (`commands.go:95-99`) chiama `AckCommandByType` che marca
come acked il **più vecchio comando pending di quel tipo**:

```go
func (s *SQLiteStore) AckCommandByType(workerID, commandType string) error {
    // ORDER BY sequence_num ASC LIMIT 1
}
```

### Problema

Se il master invia due comandi `drain` allo stesso worker, e il worker li processa entrambi, solo il primo
viene marcato come acked. Il secondo rimane `pending` per sempre (o fino a scadenza).

Inoltre, il worker non ha modo di tracciare **quale** comando sta confermando. Il campo `CommandID` esiste
nel `WorkerCommand` ricevuto, ma viene ignorato durante l'ACK.

## Stato target

1. Il `WorkerCommand` lato worker contiene `CommandID` (già presente in `api_types.go` ma non esposto).
2. L'API client ha un metodo `AckCommandByID(ctx, workerID, commandID)`.
3. Il worker chiama `AckCommandByID` invece di `AckCommand`.
4. Il master ha già `AckCommandByID` implementato in `store_worker_control.go:95-111` — va solo esposto
   via HTTP endpoint e chiamato.
5. L'endpoint legacy `POST /api/workers/commands/ack` rimane per retrocompatibilità ma logga un warning.
6. Nuovo endpoint (o modifica dell'esistente) che accetta `command_id` oltre a `command`.

## File coinvolti

| File | Azione |
|---|---|
| `RemoteCodex/native/worker-agent-go/pkg/api/api_types.go` | Modificare: `WorkerCommand` già ha `Command` e `Timestamp`, va aggiunto `CommandID` |
| `RemoteCodex/native/worker-agent-go/pkg/api/client_endpoints.go` | Aggiungere `AckCommandByID()` e l'endpoint corrispondente |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_commands.go` | Modificare: chiamare `AckCommandByID` invece di `AckCommand` |
| `DataServer/internal/handlers/remote/workers/lifecycle/` | Nuovo/Modificare: handler HTTP per ACK by command_id |
| `DataServer/internal/workers/commands.go` | Modificare: esporre `AckCommandByID` anche via HTTP handler |

## Definition of Done

- [ ] `WorkerCommand` nel worker include `CommandID string` (valorizzato dalla risposta del master)
- [ ] `api.Client` espone `AckCommandByID(ctx, workerID, commandID string) error`
- [ ] `processCommand()` in `worker_commands.go` chiama `AckCommandByID(ctx, w.config.WorkerID, cmd.CommandID)`
- [ ] Master endpoint `POST /api/workers/commands/ack` supporta campo `command_id` nel body JSON
- [ ] Se `command_id` è presente, il master chiama `cm.AckCommandByID(commandID)` (già implementato)
- [ ] Se solo `command` è presente (legacy), fallback a `AckCommandByType` con log warning
- [ ] Test lato worker: due comandi stesso tipo → entrambi acked correttamente
- [ ] Test lato master: `AckCommandByID` con ID inesistente → errore
- [ ] Test lato master: ACK legacy (solo tipo) ancora funzionante
- [ ] Deduplication: due ACK per lo stesso `command_id` → il secondo è no-op (idempotente)

## Criteri di test

```bash
# Master: test ACK by ID
cd refactored/DataServer && go test ./internal/workers/... -v -run TestAck

# Worker: test che invia command_id nell'ACK
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestCommandAck

# Integrazione: invia due comandi stesso tipo, verifica che entrambi siano acked
cd refactored/DataServer && go test ./internal/store/... -v -run TestCommandAckById
```

## Dipendenze

- Nessuna hard dependency.
- È complementare a 03 (MarkCommandDelivered) — insieme rendono il ciclo di vita dei comandi completo.
