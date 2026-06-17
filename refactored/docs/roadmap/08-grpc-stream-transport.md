# 08 — GRPCStreamTransport Implementation

## Stato attuale

Non esiste alcuna implementazione gRPC nel progetto. La comunicazione è HTTP/JSON via Gin (master)
e `net/http` (worker).

## Stato target

Implementazione di `ControlTransport` basata su gRPC bidirectional stream.

### Lato worker

```go
type GRPCStreamTransport struct {
    conn      *grpc.ClientConn
    client    controlv1.WorkerControlClient
    stream    controlv1.WorkerControl_ConnectClient
    workerID  string
    sessionID string
    recvChan  chan ControlMessage
    ctx       context.Context
    cancel    context.CancelFunc
    mu        sync.Mutex
}
```

1. `Connect()`: apre connessione gRPC, chiama `client.Connect()`, invia `Hello`, riceve `HelloAck`.
2. `Receive()`: goroutine che legge dallo stream e pusha su `recvChan`.
3. `Send()`: invia un `ControlMessage` sullo stream (conversione da `ControlMessage` a proto).
4. `Close()`: invia `Goodbye`, chiude stream e connessione.

### Lato master

```go
type GRPCControlServer struct {
    controlv1.UnimplementedWorkerControlServer
    registry    *workers.Registry
    cmdMgr      *workers.CommandManager
    tokenMgr    *workers.TokenManager
    jobService  *jobservice.Service
    connections map[string]*workerConnection  // workerID → stream
    mu          sync.RWMutex
}

type workerConnection struct {
    stream    controlv1.WorkerControl_ConnectServer
    workerID  string
    sessionID string
    sendChan  chan *controlv1.MasterToWorker
    ctx       context.Context
    cancel    context.CancelFunc
}
```

1. `Connect()`: riceve `Hello`, autentica, registra la connessione.
2. Goroutine di ricezione: legge messaggi dallo stream e li dispatcha.
3. Goroutine di invio: legge da `sendChan` e invia sullo stream.
4. Su disconnect: pulisce la connessione dalla mappa.

## File coinvolti

| File | Azione |
|---|---|
| `RemoteCodex/native/worker-agent-go/internal/transport/grpc_transport.go` | Nuovo: implementazione worker |
| `RemoteCodex/native/worker-agent-go/internal/transport/grpc_transport_test.go` | Nuovo: test worker |
| `DataServer/internal/transport/grpc_server.go` | Nuovo: implementazione master |
| `DataServer/internal/transport/grpc_server_test.go` | Nuovo: test master |
| `DataServer/cmd/server/main.go` o `bootstrap.go` | Modificare: avviare gRPC server su porta separata |
| `RemoteCodex/native/worker-agent-go/go.mod` | Modificare: aggiungere `google.golang.org/grpc` |
| `DataServer/go.mod` | Modificare: aggiungere `google.golang.org/grpc` |

## Definition of Done

### Lato worker
- [ ] `GRPCStreamTransport` implementa `ControlTransport`
- [ ] `Connect()`: stabilisce connessione TLS, invia `Hello`, attende `HelloAck`, setta `sessionID`
- [ ] `Receive()`: restituisce `<-chan ControlMessage` popolato da goroutine di lettura stream
- [ ] `Send()`: converte `ControlMessage` in proto `WorkerToMaster` e invia sullo stream
- [ ] `Close()`: invia `Goodbye`, chiude stream, chiude connessione
- [ ] Riconnessione automatica su errore di stream (delegata a 04)
- [ ] Timeout configurabile per Connect

### Lato master
- [ ] `GRPCControlServer` implementa `controlv1.WorkerControlServer`
- [ ] `Connect()`: riceve `Hello`, valida credential (vedi 01), genera session, registra connessione
- [ ] Dispatch messaggi ricevuti: `Heartbeat` → registry, `LeaseRenewal` → job service, ecc.
- [ ] Invio comandi: `CommandManager` pusha su `sendChan` della connessione corretta
- [ ] Gestione disconnect: cleanup connessione, scadenza session
- [ ] Graceful shutdown: notifica `Drain` a tutti i worker connessi

### Integrazione
- [ ] Server gRPC su porta configurabile (default 8443)
- [ ] Health check gRPC (`grpc.health.v1.Health`)
- [ ] Interceptor per logging e metrics
- [ ] Test e2e: worker si connette, scambia heartbeat, riceve comando

## Criteri di test

```bash
# Unit test worker
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/transport/... -v

# Unit test master
cd refactored/DataServer && go test ./internal/transport/... -v

# Integration test (richiede entrambi in esecuzione)
cd refactored && go test ./... -tags=integration -v -run TestGRPCIntegration
```

## Dipendenze

- **06** (ControlTransport interface) — deve esistere
- **07** (proto) — deve essere generato
- **01** (worker credentials) — consigliato per l'auth del `Hello`
