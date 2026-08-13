# Velox — complessità ciclomatica e package affollati (2026-08-13)

Misura precisa della complessità ciclomatica con `gocyclo` (installato in
questa sessione) sui moduli Go, più la dimensione delle funzioni C++ e il
conteggio file/righe per package. Integra
`REFACTOR-HOTSPOTS-2026-08-13.md` (frequenza × dimensione): qui il metro è
la complessità reale per funzione, non il proxy "righe di codice".

Soglia di attenzione: complessità **≥ 30**. Convenzione: una funzione sopra
~15 va già nominata ed estratta per decisione di dominio.

## 1. Complessità ciclomatica — Go DataServer (produzione, ≥ 32)

| Cyc | Funzione | File | Proposta di estrazione |
|---|---|---|---|
| 88 | `(*SQLiteArtifactFinalizer).FinalizeVerified` | store/artifact_finalization.go | split per stato: `finalizeSucceeded` / `finalizeFailed` / `finalizeQuarantined`, ognuna con la propria transazione CAS |
| 82 | `(*DeliveryRunner).processLease` | deliveries/runner_process.go | estrarre `classifyPhaseError` + `nextAction` (policy pura, testabile senza DB) |
| 68 | `evaluateOne` | fleet/opsalerts/evaluator.go | tabella regole per severity; ogni regola un predicato nominato |
| 64 | `(*DeliveryRunner).runPublicationPhases` | deliveries/runner_phases.go | estrarre la macchina a fasi in `phaseplan` con transizioni esplicite |
| 62 | `buildSupervisor` | cmd/server/bootstrap_supervisor.go | una `wireX` per capability (alerts, reconciler, smoke…) invece di un corpo unico |
| 54 | `(*Service).SummarizeTask` | observability/summarize.go | proiezioni dimensioni → helper `projectAttempt`, `projectArtifact` |
| 54 | `(*Handler).Stream` | grpcserver/handler_stream.go | dispatch per message-type in tabella (map tipo → handler) |
| 53 | `wireFleetOperatorHandlers` | cmd/server/bootstrap_wiring.go | split per sotto-risorsa fleet (workers, smoke, update) |
| 47 | `(*MediaProbeRepository).CompleteMediaProbe` | store/media_probe_jobs.go | separare read-modello dal side-effect (write status + cleanup) |
| 46 | `buildAppComponents` | cmd/server/bootstrap_composition.go | composizione → builder per feature, non elenco sequenziale |
| 45 | `(*TargetResolver).ResolveSelection` | publishing/resolver_selection.go | matcher di destinazione come chain di `matcher` ordinati |
| 44 | `computeParallelism` | store/sqlite_task_atomic_persistence_parallelism.go | policy in una tabella `(conditions → value)` |
| 43 | `(*Handler).handleHeartbeat` | grpcserver/handler_workers.go | estrarre `parseHeartbeatExtra` + `applyHeartbeat` |
| 41 | `applyMetadataFields` | workers/worker_info.go | mappatura campo→trasformazione come slice ordinata |
| 41 | `(*Handler).handleTaskAccepted` | grpcserver/handler_accept.go | estrarre validazione fencing (già separata) dalla persistenza |
| 41 | `(*CreatorForwardingRunner).processLease` | forwarding/runner_lease.go | simmetrico a `deliveries.processLease`: `classifyLeaseError` |
| 39 | `(*AssetService).ResolveAndRegister` | assets/registration.go | `Resolve` vs `Register` in due funzioni coordinate |
| 38 | `(*Handlers).RetryPipelineRun` | pipeline/pipeline_run_submit.go | estrarre `validateRetryTransition` |
| 36 | `buildSceneImagePayload` | jobs/enqueue/enqueue_scene_image.go | builder per variante (master/voiceover) |
| 35 | `normalizeSceneVideoPayloadContext` | jobs/enqueue/normalize_core.go | normalizzatore per tipo di campo |
| 35 | `sniffMIME` | inputsecurity/validate.go | regole MIME in tabella ordinata |
| 35 | `(*Handler).handleTaskResult` | grpcserver/handler_result.go | estrarre proiezione `TaskResult→IngestResultCommand` |
| 35 | `(*UpdateExecutor).Execute` | fleet/update_executor.go | macchina a stati `ROLLOUT→DRAIN→VERIFY` in `updatestate` |
| 34 | `DeriveStatus` | pipelineruns/status.go | tabella `(stato run, stato stage) → stato` |
| 33 | `(*DriveHandlers).DriveHealthCheckHandler` | handlers/server/drive/health.go | estrarre `collectHealthSignals` |
| 33 | `registerReadinessChecks` | cmd/server/bootstrap_readiness.go | generazione check da un elenco dichiarativo |
| 32 | `(*StaleExecutionReconciler).applyCommittedArtifact` | store/stale_execution_reconciler_apply.go | separare `buildArtifactMutation` dalla transazione |

## 2. Complessità ciclomatica — worker-agent-go (produzione, ≥ 31)

| Cyc | Funzione | File | Proposta |
|---|---|---|---|
| 81 | `(*Worker).receiveLoop` | internal/worker/worker_claimloop.go | dispatch per tipo comando in tabella; `handleClaim`/`handleLease`/`handleCancel` |
| 55 | `main` | cmd/velox-worker-agent/main.go | composition root → `wireWorker()` + `wireTelemetry()` |
| 54 | `(*Worker).publishArtifactsV1` | internal/worker/active_task_publish.go | `planUpload` / `uploadOne` / `reportPublish` |
| 47 | `(*WorkerConfig).Validate` | pkg/config/config_validate.go | validatore per sezione (endpoint, worker_id, drive, telemetry) |
| 43 | `parseRequest` | pkg/video/pipelines/hybrid/compiler_parse.go | parser per campo, non monolitico |
| 43 | `writeVeloxAssetToCacheAtOffset` | internal/worker/asset_cache.go | separare chunk-write dal bookkeeping hash |
| 40 | `applyEnvOverrides` | pkg/config/env.go | tabella `(env → setter)` |
| 39 | `(*masterAssetTransferer).Transfer` | internal/worker/asset_downloader.go | `planTransfer` / `downloadSegments` / `verify` |
| 38 | `attachWorkerIdentityAndTimings` | internal/worker/report_observability.go | builder di attributi |
| 37 | `Run` | internal/cacheevict/evict.go | policy LRU + scan separati |
| 36 | `(*Worker).dispatchTaskRunner` | internal/task_dispatch.go | registry executor per tipo task |
| 34 | `PerformFullAssociation` | pkg/video/entity_association.go | match per categoria in helper |
| 34 | `(*Worker).submitTaskResult` | internal/worker/task_result_builder.go | builder risultato per outcome |
| 31 | `chrononPlanJSON` | pkg/video/services/native/chronon_adapter.go | mapping JSON in funzione per nodo |
| 31 | `migrateLegacySchemaWithHook` | internal/workercache/cache_helpers.go | migrazione per versione |

## 3. Complessità C++ — engine

`src/core/render_engine.cpp` = **2005 righe**, funzioni di picco:

| Funzione | Righe (circa) | Stato |
|---|---|---|
| `render()` | ~939 (778–1717) | ancora monolitica dopo l'estrazione di `renderCopyOnly`/`renderMixed`; da spezzare in `resolveInputs`, `buildTimeline`, `dispatchSegments`, `muxAndFinalize` |
| `renderCopyOnly()` | ~268 (224–492) | già estratta; verificare ulteriore split del path zero-intermediates |
| `sidecarJson()` | ~175 (1822–1997) | spostare in `render_engine_helpers.cpp` |
| `renderMixed()` | ~158 (620–778) | già estratta; i sotto-step `transcodeMixedSegment`/`resolveMixedFinalAudio` possono diventare helper file-locali |

## 4. Package affollati

### DataServer (file non-test per package)

| Package | File | Righe | Nota |
|---|---|---|---|
| `internal/store` | **154** | **30,422** | god package: conosce ~20 domini. Prima vittima strutturale. |
| `internal/handlers/server` | 109 | 17,192 | aggregato di sottopackage (pipeline, api, drive, calendar…) |
| `internal/metrics` | 46 | — | |
| `internal/handlers/server/pipeline` | 46 | — | validazione ≠ projection ≠ serialization |
| `internal/handlers/server/api` | 32 | — | |
| `internal/grpcserver` | 32 | — | 4 handler > 35 cyc |
| `internal/fleet` | 32 | — | `controller.go` 593 righe |
| `cmd/server` | 27 | — | composition root: 3 funzioni > 45 cyc |

### worker-agent-go

| Package | Nota |
|---|---|
| `internal/worker` | `receiveLoop` 81, `publishArtifactsV1` 54, ~10 file > 400 righe |
| `internal/telemetry` | `attempt_session.go`/`phase_recorder.go`/`collectors.go` > 500 righe |
| `internal/taskrunner/executors` | `scene_composite.go` 634 + `render_batch_executor.go` 633 condividono compilazione piano |

## 5. Ordine di intervento proposto

1. **`internal/store` god package** (154 file) — estrarre cluster coesi in
   sottopackage (`store/artifacts`, `store/deliveries`, `store/forwarding`,
   `store/reconciliation`, `store/workers`) dietro `store/contracts`. È il
   moltiplicatore: quasi ogni funzione > 40 cyc vive qui.
2. **`FinalizeVerified` (88)** — prima singola funzione: split per stato con
   transazione CAS invariata (il gate allowlist `FinalizeVerified` resta).
3. **`deliveries.processLease` (82) + `runPublicationPhases` (64)** — policy
   pura estraibile senza I/O, alta frequenza di modifica.
4. **`render_engine.cpp render()` (~939 righe)** — continuare lo split già
   iniziato; è l'hotspot #1 per frequenza.
5. **`cmd/server` composition root** — `buildSupervisor`/`buildAppComponents`/
   `wireFleetOperatorHandlers` → builder per capability (allinea §6 fail-closed).
6. **`grpcserver` handler** — tabella di dispatch per message-type.
7. **worker-agent-go `internal/worker`** — `receiveLoop` (81) per primo.

## Vincoli operativi (invariati)

- Un solo hotspot alla volta; commit atomico + push `main` dopo ogni blocco.
- Split di package = cambio di contratto cross-package → `pre-removal-verify.sh`.
- Nessun `nil`/noop/stub nel wiring produttivo (AGENTS.md §6); i builder di
  `cmd/server` devono restare fail-closed.
- Preservare i gate `check-architecture.sh`, `check-db-access.sh`,
  `check-capability-contract.sh` e la SSOT dei contratti in `store/contracts`.
