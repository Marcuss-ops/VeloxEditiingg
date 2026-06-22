# PR-07 — Remove Job protocol compatibility

> **Audit anchor:** [§P1.1](../LEGACY_SSOT_AUDIT.md#p11--protocollo-job-e-task-attivi-contemporaneamente)
> **Target milestone:** cutover finale.
> **Branch:** `cutover/pr-07-remove-job-protocol-compat`
> **Dipendenze:** PR-03 + PR-04 + PR-05 + PR-06.
> **Prerequisito esterno:** bump in `proto/velox/control/worker_control.proto`
> a `protocol_min_version = N+1`; nessun worker del fleet deve avere
> versione < N.

## Contesto

Il protocollo supporta ancora i messaggi Job (`JobOffer`, `JobAccepted`,
`JobRejected`, `JobLeaseGranted`, `JobResult`, `JobProgress`,
`LeaseRenewal`) oltre ai nuovi Task. Il worker ha entrambi i percorsi.
Il rischio è:

- doppio protocollo operativo,
- fallback silenziosi,
- manutenzione duplicata,
- test e bugfix da replicare in due stack.

## Scope

- Eliminare i messaggi Job runtime dal `.proto`.
- Eliminare gli handler `handleJobAccepted`, `handleJobRejected`,
  `handleJobResult`, `handleJobProgress`, `handleLeaseRenewal` sul
  master.
- Eliminare dal worker i percorsi `MsgJobOffer`, `MsgJobLeaseGranted`,
  `sendAccept`, `sendReject`, `storePendingJob`, `takePendingJob`,
  `submitLegacyJobResult`, `extractLegacyOutput`, ricostruzione
  legacy di `TaskSpec`, e `pendingLeaseJobs`.
- `CancelJob` resta come comando **business** (non runtime):
  deve cancellare tutte le Task attive.

## Files to touch

```text
proto/velox/control/worker_control.proto
scripts/gen-proto.sh
velox-server/internal/grpcserver/handler_jobs.go
velox-server/internal/grpcserver/handler_workers.go
RemoteCodex/native/worker-agent-go/internal/worker/worker.go
RemoteCodex/native/worker-agent-go/internal/worker/job_executor.go
RemoteCodex/native/worker-agent-go/internal/worker/job_executor_test.go
RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream.go
RemoteCodex/native/worker-agent-go/internal/protocol/dispatch.go
```

## Sequenza operativa

```text
1. proto bump protocol_min_version = N+1.
2. Rimuovere dal .proto i messaggi Job runtime (eccetto CancelJob).
   Rigenerare `pb.go` via scripts/gen-proto.sh.
3. Master: rimuovere gli handler Job (eccetto handleCancelJob).
4. Worker: rimuovere i percorsi legacy. Mantenere CancelJob come
   dispatch che chiama Task.Cancel su ogni Task attiva del Job.
5. CI guard: anche solo la presenza dei simboli vietati in
   check-no-legacy.sh → fail.
```

## Acceptance criteria

- [ ] Nessun handler gRPC `handleJob*` (eccetto CancelJob) presente in
      `velox-server/internal/grpcserver/`.
- [ ] Nessun path worker con `MsgJobOffer` / `MsgJobLeaseGranted` /
      `pendingLeaseJobs`.
- [ ] Test `protocol_job_messages_removed_test`: scansione statica del
      .pb.go generato.
- [ ] Worker fleet aggiornato: nessun worker < N.
- [ ] Smoke E2E verde con due worker ibridi: solo Task path attivo.

## Test

- **Unit:** test per `handleCancelJob` deve cancellare tutte le task
  attive del Job.
- **Integration:** golden E2E senza alcun messaggio Job runtime.
- **Compilazione:** `go build ./...` verde dopo rimozione.

## CI guards introdotti

In `check-no-legacy.sh` (full-tree):

```text
JobOffer
JobAccepted
JobRejected
JobLeaseGranted
JobResult
JobProgress
LeaseRenewal           # job-scoped
sendAccept             # worker legacy
sendReject             # worker legacy
storePendingJob
takePendingJob
pendingLeaseJobs
submitLegacyJobResult
extractLegacyOutput
handleJobAccepted
handleJobRejected
handleJobResult
handleJobProgress
handleLeaseRenewal
```

In `check-architecture.sh`: il .pb.go generato non deve elencare i
messaggi rimossi.

## Rischi

- Worker datati in produzione che parlano ancora Job: **rigettati al
  handshake** perché `protocol_min_version = N+1`. Coordinamento con
  operazioni necessario.
- Una regression del processo di bump protocol può causare outage.

## Out of scope

- Rimozione di qualsiasi endpoint orchestrator (`/api/v1/orchestrator/*`).
- Eventuale pulizia di route HTTP che parlano Job.
- Aggiornamento golden E2E (vedi PR-10).
