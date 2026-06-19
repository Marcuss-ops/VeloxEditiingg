# 06 — ControlTransport Interface

## Stato attuale

Il worker comunica con il master tramite chiamate HTTP dirette dal `api.Client`. Non esiste
un'astrazione del trasporto: ogni loop (heartbeat, job, command, lease) chiama direttamente
metodi HTTP come `GetJobV2()`, `SendHeartbeat()`, `GetCommands()`, `RenewJobLeaseV2()`.

Il master espone endpoint HTTP Gin (`POST /api/workers/heartbeat`, `GET /api/workers/commands`, ecc.)
senza un layer di astrazione lato server.

## Stato target

Un'interfaccia `ControlTransport` che astrae il meccanismo di comunicazione:

```go
// ControlTransport definisce il canale di comunicazione worker↔master.
type ControlTransport interface {
    // Connect stabilisce la connessione e autentica il worker.
    Connect(ctx context.Context, hello WorkerHello) error

    // Receive restituisce un canale di messaggi dal master.
    Receive(ctx context.Context) (<-chan ControlMessage, error)

    // Send invia un messaggio al master.
    Send(ctx context.Context, message ControlMessage) error

    // Close chiude il trasporto.
    Close() error
}
```

`ControlMessage` è un messaggio tipizzato comune a tutti i trasporti:

```go
type ControlMessageType string

const (
    MsgHello        ControlMessageType = "hello"
    MsgHelloAck     ControlMessageType = "hello_ack"
    MsgHeartbeat    ControlMessageType = "heartbeat"
    MsgLeaseRenewal ControlMessageType = "lease_renewal"
    MsgJobAvailable ControlMessageType = "job_available"
    MsgJobAccepted  ControlMessageType = "job_accepted"
    MsgJobRejected  ControlMessageType = "job_rejected"
    MsgJobProgress  ControlMessageType = "job_progress"
    MsgJobResult    ControlMessageType = "job_result"
    MsgCommand      ControlMessageType = "command"
    MsgCommandAck   ControlMessageType = "command_ack"
    MsgCancelJob    ControlMessageType = "cancel_job"
    MsgDrain        ControlMessageType = "drain"
    MsgPing         ControlMessageType = "ping"
    MsgGoodbye      ControlMessageType = "goodbye"
)

type ControlMessage struct {
    MessageID       string                 `json:"message_id"`
    Type            ControlMessageType     `json:"type"`
    WorkerID        string                 `json:"worker_id"`
    SessionID       string                 `json:"session_id,omitempty"`
    SequenceNumber  int64                  `json:"sequence_number,omitempty"`
    SentAt          time.Time              `json:"sent_at"`
    ProtocolVersion string                 `json:"protocol_version"`
    Payload         map[string]interface{} `json:"payload,omitempty"`
}
```

## File coinvolti

| File | Azione |
|---|---|
| `shared/controltransport/transport.go` | Nuovo: definizione `ControlTransport` interface e `ControlMessage` |
| `shared/controltransport/message.go` | Nuovo: tipi di messaggio, helper di serializzazione |
| `shared/controltransport/errors.go` | Nuovo: errori tipizzati del trasporto |

## Definition of Done

- [ ] Package `shared/controltransport` creato con i tipi sopra
- [ ] `ControlTransport` interface con metodi `Connect`, `Receive`, `Send`, `Close`
- [ ] `ControlMessage` struct con tutti i campi: `MessageID`, `Type`, `WorkerID`, `SessionID`,
  `SequenceNumber`, `SentAt`, `ProtocolVersion`, `Payload`
- [ ] `ControlMessageType` enum con tutti i tipi di messaggio del documento:
  Worker→Master: `Hello`, `Heartbeat`, `LeaseRenewal`, `JobAccepted`, `JobRejected`,
  `JobProgress`, `CommandAck`, `ArtifactUploaded`, `JobResult`, `Goodbye`
  Master→Worker: `HelloAck`, `JobOffer`, `Command`, `CancelJob`, `Drain`,
  `ConfigurationUpdate`, `LeaseRevoked`, `Ping`
- [ ] `WorkerHello` struct con le info di registrazione
- [ ] Errori tipizzati: `ErrTransportClosed`, `ErrSessionExpired`, `ErrAuthFailed`
- [ ] Test di serializzazione/deserializzazione round-trip

## Criteri di test

```bash
cd refactored/shared && go test ./controltransport/... -v
```

## Dipendenze

- Nessuna — è pura definizione di interfaccia.
- È **prerequisito** per 08, 09, 10.
