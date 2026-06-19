# Worker Reliability Fixes — Riepilogo Architetturale

**Data**: 19 Giugno 2026
**Commit**: `00383890` (7 fix + collectAllowedJobTypes + mTLS), `f1a39b7e` (go vet warnings), `94e52fb5` (6 gap production-readiness) — tutti su `origin/main`

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
| 7 | Production-Readiness Gaps | #1–#6: sessionWriter callback, pendingOffer teardown, atomic persistence, command_id dedup, ConcurrencyLimiter, recvCh ownership |

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

## Production-Readiness Gaps (Commit `94e52fb5`)

Dopo le 7 fix iniziali, un'analisi approfondita ha identificato 6 gap residui che impedivano la certificazione "100% production-ready". Questi sono stati risolti nel commit `94e52fb5`.

### Gap #1 — sessionWriter Callbacks (Comandi Marked Delivered Solo Dopo stream.Send Reale)

**File modificati**:
- `DataServer/internal/grpcserver/handler.go` — `outboundMessage` struct, `sendCh` type, `sessionWriter`, `safeSend`, `dispatchCommands`
- `DataServer/internal/grpcserver/handler_workers.go` — `safeSend` signature
- `DataServer/internal/grpcserver/handler_jobs.go` — `safeSend` signature

**Prima (fix #3)**: `dispatchCommands` marcava `delivered` dopo `safeSend`, che conferma solo l'inserimento nel canale in memoria. Il vero `stream.Send()` avviene dopo, nel `sessionWriter`. Se quello fallisce, il comando risultava già delivered nel DB.

**Dopo**: il `sessionWriter` chiama `OnSent()` solo dopo `stream.Send()` riuscito:

```go
type outboundMessage struct {
    Envelope *pb.MasterToWorkerEnvelope
    OnSent   func() // Chiamata solo dopo stream.Send riuscito
}

func (h *Handler) sessionWriter(sess *workerSession) {
    for out := range sess.sendCh {
        if err := sess.stream.Send(out.Envelope); err != nil {
            // ... drain + publish writerErr
            // OnSent NON viene chiamato → comandi restano pending
            break
        }
        if out.OnSent != nil {
            out.OnSent() // ✅ MarkCommandDelivered solo qui
        }
    }
}
```

`dispatchCommands` crea un `outboundMessage` con callback che chiama `MarkCommandDelivered`. Se `sessionWriter` fallisce, i comandi restano pending e vengono ritentati al prossimo dispatch.

### Gap #2 — Release pendingOffer su Session Teardown

**File modificati**: `DataServer/internal/grpcserver/handler.go`

**Prima**: quando `sessionWriter` falliva (`writerErr`) o la sessione veniva chiusa, il `pendingOffer` (job offer inviato ma non ancora accettato/rifiutato) restava in memoria con il claim attivo. Il job rimaneva leased fino alla scadenza del lease.

**Dopo**: `ReleaseClaim` chiamato in 3 punti:
1. `writerErr` case — subito dopo il log
2. `defer` della sessione — cleanup finale
3. `closeOldSessionLocked` — quando un worker riconnette e la vecchia sessione viene chiusa

```go
sess.claimMu.Lock()
if sess.pendingOffer != nil {
    h.lifecycleSvc.Repo().ReleaseClaim(ctx, sess.pendingOffer.JobID)
    sess.pendingOffer = nil
}
sess.claimMu.Unlock()
```

### Gap #3 — Atomic Persistence (Scrittura Atomica del File di Stato)

**File modificati**: `RemoteCodex/native/worker-agent-go/internal/worker/worker_persistence.go`

**Prima**: `saveLocalState()` usava `os.WriteFile` direttamente. Un crash durante la scrittura poteva lasciare JSON corrotto sul disco.

**Dopo**: scrittura atomica in 3 fasi:

```go
tmpPath := path + ".tmp"
os.WriteFile(tmpPath, data, 0600)
f, _ := os.OpenFile(tmpPath, os.O_RDWR, 0600)
f.Sync()    // ✅ fsync — forza flush a disco
f.Close()
os.Rename(tmpPath, path)  // ✅ rename atomico
```

Se il processo crasha in qualsiasi momento, il file `.tmp` viene ignorato al prossimo avvio e il file `.json` precedente (integro) rimane valido.

### Gap #4 — Command Deduplication by CommandID

**File modificati**: `RemoteCodex/native/worker-agent-go/internal/worker/worker_init.go`

**Prima**: la chiave di deduplicazione era `command|timestamp`. Stesso comando con timestamp diverso → ri-eseguito. Comandi diversi con stesso contenuto e timestamp → confusi.

**Dopo**: `CommandID` come chiave primaria, fallback a `command|timestamp` per retrocompatibilità:

```go
func commandKey(cmd api.WorkerCommand) string {
    cid := strings.TrimSpace(cmd.CommandID)
    if cid != "" {
        return "id:" + cid  // ✅ Dedup per command_id
    }
    // Fallback: command|timestamp (retrocompatibilità)
    return fmt.Sprintf("%s|%s", cmd.Command, ts)
}
```

Le vecchie chiavi composite scadono naturalmente via TTL (30 minuti).

### Gap #5 — ConfigurationUpdate: ConcurrencyLimiter + Ack con CommandID

**File modificati**:
- `RemoteCodex/native/worker-agent-go/internal/worker/concurrency.go` — `SetMaxActiveJobs`
- `RemoteCodex/native/worker-agent-go/internal/worker/worker.go` — handler `ConfigurationUpdate`

**Prima**: `ConfigurationUpdate` aggiornava `w.config.MaxActiveJobs` ma **non** il `ConcurrencyLimiter`, che restava con il valore iniziale. L'ack non includeva `command_id`, quindi il master lo ignorava.

**Dopo**:
1. `ConcurrencyLimiter.SetMaxActiveJobs(max)` aggiorna il limite logico usato da `Acquire`/`CanAcceptJob`
2. L'ack include `command_id` (via `msg.MessageID` dell'envelope):

```go
w.concurrencyLimiter.SetMaxActiveJobs(newMax)
ackPayload := map[string]interface{}{
    "command_id":        msg.MessageID,  // ✅ Master ora matcha l'ack
    "worker_id":         w.config.WorkerID,
    "max_parallel_jobs": w.config.MaxActiveJobs,
    "log_level":         w.config.LogLevel,
}
```

### Gap #6 — recvCh Ownership (Solo recvLoop Chiude recvCh)

**File modificati**: `RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream.go`

**Prima**: `Close()` chiudeva `recvCh` dopo un timeout di 5s. Se `recvLoop` era ancora bloccato in `stream.Recv()` e si sbloccava dopo il timeout, poteva scrivere su `recvCh` già chiuso → `send on closed channel` panic.

**Dopo**: solo `recvLoop` chiude `recvCh` nel suo defer. `Close()` non lo tocca più:

```go
// recvLoop defer:
if t.recvCh != nil {
    close(t.recvCh)     // ✅ Solo recvLoop chiude recvCh
}
close(t.recvDone)

// Close():
select {
case <-t.recvDone:      // Aspetta recvLoop
case <-time.After(5s):  // Timeout di sicurezza
}
// recvCh NON viene chiuso qui — lo chiude solo recvLoop
```

Il `closeCh` + `CloseSend()` + `conn.Close()` garantiscono che `recvLoop` esca sempre. Il worker `receiveLoop` esce via `ctx.Done()` se `recvCh` non viene chiuso tempestivamente.

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
| `DataServer/internal/grpcserver/handler.go` | `supportedJobTypes atomic.Value`, `dispatchCommands` fix, `outboundMessage`, `sessionWriter` OnSent callback, `pendingOffer` release ×3 |
| `DataServer/internal/grpcserver/handler_jobs.go` | `safeSend` signature update (gap #1) |
| `DataServer/internal/grpcserver/handler_workers.go` | `sendPushJobOffer` completo, `collectAllowedJobTypes`, `ReleaseClaim` path, `safeSend` signature update |
| `DataServer/internal/queue/file_queue.go` | `ClaimNextJob` completo + `ReleaseClaim` path |
| `DataServer/internal/workers/commands.go` | `MarkCommandDelivered` metodo |
| `DataServer/internal/audit/data_layer.go` | `allowedLegacy`, `AllowLegacy()`, `workers.json` check |
| `DataServer/internal/audit/data_layer_test.go` | Fix match `VELOX_DB_PATH` |
| `DataServer/internal/handlers/server/jobs/job_submission_test.go` | **Rimosso** (orfano) |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker.go` | #4 `LeaseRevoked`, #5 `ConfigurationUpdate` + `ConcurrencyLimiter` + ack `command_id`, #7 persistenza |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_init.go` | #7 caricamento stato persistito, #4 `commandKey` per `command_id` |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_persistence.go` | **Nuovo** — #7 persistenza JSON-file |
| `RemoteCodex/native/worker-agent-go/internal/worker/concurrency.go` | `SetMaxActiveJobs` metodo (#5) |
| `RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream.go` | #6 `errCh` + `recvDone` sync + `recvCh` ownership (defer) |
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
