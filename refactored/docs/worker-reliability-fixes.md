# Worker Reliability Fixes — Riepilogo Architetturale

**Data**: 19 Giugno 2026
**Commit**: `00383890` (7 fix + collectAllowedJobTypes + mTLS) e `f1a39b7e` (go vet warnings) — entrambi su `origin/main`

Questo documento descrive le modifiche architetturali apportate in questa sessione al control plane Velox (master + worker) per migliorare l'affidabilità, la correttezza e la resilienza del sistema.

---

## Panoramica

| Fase | Area | Problemi Risolti |
|------|------|------------------|
| 1 | Master — Job Offer + Claim | #1, #2, `collectAllowedJobTypes` |
| 2 | Master — Command Delivery | #3 |
| 3 | Worker — Lease/Config handling | #4, #5 |
| 4 | Worker — gRPC Transport | #6 |
| 5 | Worker — Local Persistence | #7 |
| 6 | Test Fixes | mTLS certs, go vet warnings |

---

## Master Side

### Fix #1 — JobOffer Completo

**File modificati**:
- `DataServer/internal/store/jobs_writer_types.go` — `store.Job` esteso con `RunID` e `PayloadJSON`
- `DataServer/internal/store/sqlite_jobs_writer.go` — proiezione SQL estesa (`run_id`, `request_json`) + scanner
- `DataServer/internal/grpcserver/handler_workers.go` — `sendPushJobOffer` ora popola tutti i campi
- `DataServer/internal/queue/file_queue.go` — `ClaimNextJob` ora popola tutti i campi

**Prima**: `sendPushJobOffer` ricostruiva manualmente un `queue.Job` ma **non copiava** `Payload`, `RunID`, `LeaseExpiry`. Il messaggio gRPC risultava con `Payload = nil` e `RunID = ""`.

**Dopo**: `store.Job` proietta `run_id` e `request_json` da SQLite. Il JSON del payload viene deserializzato con `json.Unmarshal`. I campi `RunID`, `Payload`, `LeaseExpiry` sono tutti popolati nel messaggio gRPC.

```
Prima:                                Dopo:
store.Job {                           store.Job {
  JobID, Status, ...                    JobID, Status, ...
  // nessun Payload                     PayloadJSON: "{...}"
  // nessun RunID                       RunID: "run-xyz"
}                                     }
     ↓                                     ↓
JobOffer {                           JobOffer {
  Payload: nil    ❌                    Payload: {...}  ✅
  RunID: ""       ❌                    RunID: "run-xyz" ✅
  LeaseExpiry: 0  ❌                    LeaseExpiry: ts  ✅
}                                     }
```

### Fix #2 — Claim Leak

**File modificati**:
- `DataServer/internal/grpcserver/handler_workers.go`
- `DataServer/internal/queue/file_queue.go`

**Prima**: dopo `ClaimNext`, se `GetJob` falliva o restituiva `nil`, la funzione usciva senza chiamare `ReleaseClaim`. Il job restava leased fino all'intervento del lease reaper.

**Dopo**: `ReleaseClaim` viene chiamato in tutti i path di errore:

```go
sj, err := h.lifecycleSvc.Repo().GetJob(ctx, claimResult.JobID)
if err != nil {
    h.lifecycleSvc.ReleaseClaim(ctx, claimResult.JobID)  // ✅ Nuovo
    return
}
if sj == nil {
    h.lifecycleSvc.ReleaseClaim(ctx, claimResult.JobID)  // ✅ Nuovo
    return
}
```

### Fix #3 — Comandi Marked Delivered Solo Dopo stream.Send

**File modificati**:
- `DataServer/internal/grpcserver/handler.go` — `dispatchCommands`
- `DataServer/internal/workers/commands.go` — nuovo `MarkCommandDelivered`

**Prima**: `GetPendingCommandsAndMarkDelivered` marcava i comandi come `delivered` **prima** di inviarli sullo stream gRPC. Se `stream.Send` falliva, il comando risultava delivered nel DB ma mai ricevuto dal worker.

**Dopo**:
```
Prima:  pending → delivered → stream.Send (può fallire)  ❌
Dopo:   GetPendingCommands → safeSend → MarkCommandDelivered  ✅
```

```
dispatchCommands(workerID) {
    cmds := cmdMgr.GetPendingCommands(workerID)   // NON marca delivered
    for _, cmd := range cmds {
        if safeSend(cmd) {                         // stream.Send riuscito
            cmdMgr.MarkCommandDelivered(cmd.ID)    // Solo ora marca delivered
        }
    }
}
```

### `collectAllowedJobTypes` — Filtro ClaimNext Basato su Capability

**File modificati**:
- `DataServer/internal/grpcserver/handler.go` — nuovo campo `supportedJobTypes atomic.Value` su `workerSession`
- `DataServer/internal/grpcserver/handler_workers.go` — `collectAllowedJobTypes` implementato

**Prima**: `collectAllowedJobTypes` restituiva sempre `nil`, quindi `ClaimNext` non filtrava per tipo di job.

**Dopo**:
1. Alla connessione (`Stream()`), il master estrae `supported_job_types` dalle capability del messaggio `Hello`
2. A ogni heartbeat, il master aggiorna `supported_job_types` da `Extra["capabilities"]`
3. `collectAllowedJobTypes` restituisce i tipi salvati (o `nil` = nessun filtro per retrocompatibilità)

```
Worker: Hello { capabilities: { supported_job_types: ["render", "process_video"] } }
                                                          ↓
Master: session.supportedJobTypes.Store(["render", "process_video"])
                                                          ↓
ClaimNext: AllowedJobTypes: ["render", "process_video"]   ✅ (prima: nil)
```

---

## Worker Side

### Fix #4 — LeaseRevoked Ora Cancella il Rendering

**File modificati**: `RemoteCodex/native/worker-agent-go/internal/worker/worker.go`

**Prima**: il worker riceveva `LeaseRevoked` ma stampava solo un log — non chiamava `cancelJob`, non rimuoveva da `activeJobs`, non fermava il processo locale.

**Dopo**:
```go
case controltransport.MsgLeaseRevoked:
    w.logger.Warn("[RECEIVE] Lease revoked for job %s: %s", ...)
    w.cancelJob(jobID)                          // ✅ Cancella il context → ferma processo
    w.activeJobsMu.Lock()
    delete(w.activeJobs, jobID)                 // ✅ Rimuove da activeJobs
    delete(w.pendingLeaseJobs, jobID)           // ✅ Rimuove da pendingLeaseJobs
    w.activeJobsMu.Unlock()
```

### Fix #5 — ConfigurationUpdate Ora Applicato

**File modificati**: `RemoteCodex/native/worker-agent-go/internal/worker/worker.go`

**Prima**: `ConfigurationUpdate` veniva solo loggato — nessuna configurazione applicata, nessun ack inviato.

**Dopo**: il worker estrae `max_parallel_jobs` e `log_level` dal `Configuration` struct e li applica immediatamente:
```go
case controltransport.MsgConfigurationUpdate:
    cfg := msg.TypedPayload.(*pb.ConfigurationUpdate)
    if cfg.Configuration != nil {
        if mpj := cfg.Configuration.Fields["max_parallel_jobs"]; mpj != nil {
            maxParallel := int(mpj.GetNumberValue())
            w.concurrencyLimiter.SetMax(maxParallel)
        }
        if ll := cfg.Configuration.Fields["log_level"]; ll != nil {
            w.logger.SetLevel(ll.GetStringValue())
        }
    }
    // Invia ack al master via sendCh
    w.sendCh <- controltransport.Message{
        Type:    controltransport.MsgConfigurationAck,
        Payload: map[string]interface{}{"status": "applied"},
    }
```

### Fix #6 — gRPC Transport: Error Propagation + Close() Race Protection

**File modificati**: `RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream.go`

**Bug 1 — Error loss in recvLoop**: `recvLoop` terminava su `Recv()` error senza pubblicare l'errore su `errCh`, chiudendo il canale senza errore. Il worker riconnetteva ma perdeva la diagnostica.

**Bug 2 — Race in Close()**: `Close()` chiudeva `recvCh` mentre `recvLoop` poteva ancora scriverci → `send on closed channel` panic.

**Fix**:
1. `recvLoop` ora pubblica l'errore su `errCh`:
```go
if err != nil {
    t.errCh <- err          // ✅ Pubblica errore prima di chiudere
    close(t.errCh)
    return
}
```

2. Aggiunto canale `recvDone` per sincronizzazione:
```go
type GRPCStreamTransport struct {
    ...
    recvDone chan struct{}   // ✅ Chiuso quando recvLoop esce
}

func (t *GRPCStreamTransport) Close() error {
    t.stream.CloseSend()
    select {
    case <-t.recvDone:       // ✅ Aspetta recvLoop
    case <-time.After(5s):   // ✅ Timeout di sicurezza
    }
    close(t.recvCh)          // ✅ Ora sicuro: recvLoop è uscito
}
```

3. `Goodbye` inviato con `sendMu` invece che direttamente.

### Fix #7 — Worker Local Persistence (Recovery)

**Nuovo file**: `RemoteCodex/native/worker-agent-go/internal/worker/worker_persistence.go`

**Prima**: tutto lo stato operativo del worker era in mappe in memoria: `activeJobs`, `seenCommands`, `pendingLeaseJobs`, `jobCancelFuncs`. Un riavvio perdeva job locali, deduplicazione comandi e lease pending.

**Dopo**: persistenza su file JSON (`worker_state.json`) con salvataggio periodico ogni 30 secondi e su shutdown.

```
WorkerState {
    SeenCommands      map[string]CommandRecord   // Deduplicazione comandi
    ActiveJobs        map[string]ActiveJobRecord // Job in esecuzione
    PendingLeaseJobs  map[string]PendingLeaseRecord // Lease in attesa
}
```

**Integrazione**:
- `worker_init.go`: carica lo stato salvato all'avvio, popola `seenCommands` e ripristina i job attivi
- `worker.go`: avvia il loop di persistenza in `runSession()`, salva su shutdown via `defer`
- Salvataggio automatico a ogni cambiamento di stato (heartbeat, claim, completamento)

---

## Test Fixes

### mTLS Certificate Mismatch

**File**: `RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream_test.go`

**Bug**: `startTestMTLSServer` generava un set di certificati (CA1) ma i test chiamavano `generateTestCertsDir` una seconda volta generando una CA2 diversa. Il client presentava un cert firmato da CA2, ma il server si fidava solo di CA1 → `x509: certificate signed by unknown authority`.

**Fix**: `startTestMTLSServer` ora restituisce `certsDir` come terzo valore di ritorno, e i test lo riusano:
```go
func startTestMTLSServer(t *testing.T, srv pb.WorkerControlServer) (*grpc.Server, string, string) {
    //                                        prima: 2 valori  ↑ ora: 3 valori
```

I 4 test mTLS (`Handshake`, `NoClientCert`, `WrongCA`, `HeartbeatSend`) ora passano tutti.

### go vet Warnings (DataServer)

**File**: `DataServer/internal/audit/data_layer.go`, `data_layer_test.go`
**File rimosso**: `DataServer/internal/handlers/server/jobs/job_submission_test.go`

| Warning | Causa | Fix |
|---------|-------|-----|
| `AllowLegacy undefined` | Metodo mai implementato su `DataLayerAuditor` | Aggiunto campo `allowedLegacy` + metodo `AllowLegacy()` |
| `buildSingleJob undefined` | Test orfano (funzione + handler rimossi in refactor) | File cancellato — nessuna funzione esiste più nel codebase |

Test audit smascherati dal fix e riparati:
- `TestCheckDuplicateSources_WorkersWarning`: `checkDuplicateSources` ora controlla `workers.json` e produce warning
- `TestCheckDatabase_MissingDB`: match corretto su `"VELOX_DB_PATH"` invece di `"velox.db"`

---

## File Coinvolti

| File | Modifica |
|------|----------|
| `DataServer/internal/store/jobs_writer_types.go` | +2 campi (`RunID`, `PayloadJSON`) |
| `DataServer/internal/store/sqlite_jobs_writer.go` | +2 colonne proiezione, scanner esteso |
| `DataServer/internal/grpcserver/handler.go` | `supportedJobTypes atomic.Value`, `dispatchCommands` fix |
| `DataServer/internal/grpcserver/handler_workers.go` | `sendPushJobOffer` completo, `collectAllowedJobTypes`, `ReleaseClaim` path |
| `DataServer/internal/queue/file_queue.go` | `ClaimNextJob` completo + `ReleaseClaim` path |
| `DataServer/internal/workers/commands.go` | `MarkCommandDelivered` metodo |
| `DataServer/internal/audit/data_layer.go` | `allowedLegacy`, `AllowLegacy()`, `workers.json` check |
| `DataServer/internal/audit/data_layer_test.go` | Fix match `VELOX_DB_PATH` |
| `DataServer/internal/handlers/server/jobs/job_submission_test.go` | **Rimosso** (orfano) |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker.go` | #4 `LeaseRevoked`, #5 `ConfigurationUpdate`, #7 persistenza |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_init.go` | #7 caricamento stato persistito |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_persistence.go` | **Nuovo** — #7 persistenza JSON-file |
| `RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream.go` | #6 `errCh` + `recvDone` sync |
| `RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream_test.go` | mTLS certs fix |

---

## Verifica

| Verifica | DataServer | Worker |
|----------|-----------|--------|
| `go build ./...` | ✅ | ✅ |
| `go vet ./...` | ✅ | ✅ |
| `go test` (modificati) | ✅ | ✅ |
| `go test` (mTLS) | — | ✅ |

Tutti i test pre-esistenti continuano a passare. Nessuna regressione.
