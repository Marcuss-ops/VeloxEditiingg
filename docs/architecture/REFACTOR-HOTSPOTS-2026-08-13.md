# Velox — mappa hotspot (commit × complessità) al 2026-08-13

Questo documento incrocia la frequenza di modifica (touches su `git log`,
740 commit totali) con la complessità/fragilità (righe di codice, file
enormi, package affollati) per produrre la lista prioritaria di intervento.
Integra — non sostituisce — `REFACTOR-HOTSPOTS-2026-08-11.md` e
`REFACTOR-TODO-2026-08-12.md`: qui l'ordinamento nasce dai dati, non dalla
sequenza logica delle fasi.

## Metodo

- **Frequenza** = numero di volte che un file/directory compare in
  `git log --name-only` (740 commit).
- **Complessità** = righe di codice e numero di file oltre soglia per
  package (proxy della complessità ciclomatica, in assenza di `gocyclo`).
- **Quadranti** (dal metodo priorità/rischio):

| Complessità \ Frequenza | Alta frequenza | Bassa frequenza |
|---|---|---|
| **Alta complessità** | 🔥 Priorità Assoluta | Priorità Media (toccare solo se si rompe) |
| **Bassa complessità** | Priorità Fluida (manutenzione ordinaria) | Priorità Bassa (lasciare) |

## Dati grezzi

### Frequenza di commit per area (touches / 740 commit)

| Area | Touches | Note |
|---|---|---|
| `RemoteCodex/native/worker-agent-go` | 1036 | `internal/worker` 236, `telemetry` 151, `taskrunner` 127, `pkg/video` 109, `pkg/performance` 84 |
| `DataServer/internal/store` | 834 | di cui `migrations` 283 |
| `DataServer/internal/handlers` | 461 | `server/pipeline` 162, `server/api` 120, `remote/workers` 59, `server/instaedit` 51 |
| `RemoteCodex/native/video-engine-cpp` | 280 | `render_engine.cpp` da solo 34 |
| `DataServer/internal/fleet` | 191 | `controller.go` 593 righe |
| `DataServer/cmd/server` | 138 | bootstrap 6 file > 400 righe |
| `DataServer/internal/metrics` | 101 | |
| `DataServer/internal/jobs` | 99 | |
| `DataServer/internal/grpcserver` | 96 | |
| `DataServer/internal/deliveries` | 85 | |

### File singoli più toccati

| File | Touches |
|---|---|
| `video-engine-cpp/src/core/render_engine.cpp` | 34 |
| `video-engine-cpp/CMakeLists.txt` | 25 |
| `scripts/fleetctl` | 22 |
| `DataServer/cmd/server/bootstrap_wiring.go` | 19 |
| `taskrunner/executors/scene_composite.go` | 15 |
| `video-engine-cpp/src/services/media_utils.cpp` | 14 |
| `worker/worker_types.go` | 13 |
| `taskrunner/report_metrics.go` | 13 |
| `prefetch/scheduler.go` | 13 |
| `handlers/server/api/admin_workers_handler.go` | 13 |

### File più grandi (complessità)

C++ (video-engine-cpp):

| File | Righe |
|---|---|
| `src/core/render_engine.cpp` | 1888 |
| `src/services/media_packet_pipeline.cpp` | 944 |
| `src/services/frame_pipeline.cpp` | 923 |
| `src/services/media_utils.cpp` | 861 |

Go — DataServer (soglia 500 righe):

| File | Righe |
|---|---|
| `internal/renderplan/compiler.go` | 633 |
| `internal/fleet/controller.go` | 593 |
| `internal/store/store_publication_state.go` | 589 |
| `internal/store/store_deployment_records.go` | 585 |
| `internal/store/store_assets.go` | 532 |
| `cmd/server/bootstrap_supervisor.go` | 518 |
| `internal/store/store_creator_forwardings.go` | 515 |
| `cmd/server/bootstrap_modules.go` | 506 |
| `internal/store/artifact_finalization.go` | 499 |

Go — worker-agent-go (soglia 500 righe):

| File | Righe |
|---|---|
| `internal/prefetch/scheduler.go` | 717 |
| `internal/worker/asset_downloader.go` | 638 |
| `internal/taskrunner/executors/scene_composite.go` | 634 |
| `internal/taskrunner/executors/render_batch_executor.go` | 633 |
| `cmd/velox-worker-agent/main.go` | 592 |
| `internal/telemetry/attempt_session.go` | 550 |
| `internal/telemetry/phase_recorder.go` | 527 |
| `pkg/performance/assembler.go` | 527 |
| `internal/telemetry/collectors.go` | 515 |
| `pkg/performance/performance_receipt_v1.go` | 500 |

## Lista prioritaria di intervento

### 🔥 PRIORITÀ ASSOLUTA (alta complessità × alta frequenza)

1. **`render_engine.cpp` (1888 righe, 34 commit)** — il file più grande e più
   toccato di tutto il repo. Da spezzare per stage (resolution, timeline,
   mix, mux, finalize) e per decisione di dominio, non per riga.
2. **`DataServer/internal/store` (834 touches, package più grande)** — ~40 file
   oltre 400 righe, 283 migration. Un writer per aggregate, estrazione di
   query helper ripetute, split per dominio (delivery, forwarding, worker,
   publication) mantenendo `store/contracts` come confine.
3. **`handlers/server/pipeline` (162 touches, 4 file > 400 righe)** —
   `intake_types.go` (424), `publication_intake_validation.go` (474),
   `worker_payload_projection.go`. Validazione da separare dal projection
   e dal serialization.
4. **`worker-agent-go/internal/worker` (236 touches, 5 file > 400 righe)** —
   `asset_downloader.go` (638), `worker_init.go` (478), `task_dispatch.go`
   (474), `worker_lifecycle.go` (444), `worker_types.go` (436). Il package
   fa troppe cose: claim, download, dispatch, lifecycle, cache.
5. **`worker-agent-go/internal/telemetry` (151 touches, 3 file > 500 righe)** —
   `attempt_session.go`, `phase_recorder.go`, `collectors.go`. Modello e
   registrazione andrebbero separati dal sampling/emissione.
6. **`worker-agent-go/internal/taskrunner` (127 touches)** —
   `scene_composite.go` (634, 15 commit) e `render_batch_executor.go` (633).
   I due executor condividono troppa logica di compilazione piano/render.
7. **`DataServer/internal/fleet` (191 touches, `controller.go` 593)** — la
   state machine `REQUESTED→…→SUCCEEDED` (Fase E del TODO) va formalizzata
   e tolta dal controller monolitico.
8. **`media_utils.cpp` (861 righe, 14 commit) + `media_packet_pipeline.cpp`
   (944 righe, 9 commit)** — helper audio/packet con probabile feature envy
   e duplicazione di parsing.

### Priorità Media (alta complessità × bassa frequenza — toccare solo se si rompe)

- `internal/renderplan/compiler.go` (633 righe) — estrarre solo decisioni
  nominate quando il batch executor migra ai `segments[]`.
- `cmd/server/bootstrap_*.go` (5 file > 400 righe) — split solo quando si
  tocca un capability nuovo; i gate fail-closed restano vincolanti.
- `internal/apiwire/apiwire.go` (495) e `internal/ingest/service.go` (498).
- `internal/prefetch/scheduler.go` (717) — fa parte della Fase D congelata;
  non toccare finché il prefetch non è certificato.

### Priorità Fluida (bassa complessità × alta frequenza — manutenzione ordinaria)

- `CMakeLists.txt` (25 commit) — verificare che i target riflettano i file
  nuovi/rimossi dopo ogni split C++.
- `scripts/fleetctl` (22 commit) — launcher; già delegante al client Go.
- `VERSION.txt` (12) e `bootstrap_wiring.go` (19) — punti di attrito di
  build, non di complessità.

### Priorità Bassa (bassa × bassa — lasciare)

- File di configurazione e documentazione stabile, fixture operative.

## Vincoli operativi

- Un solo hotspot alla volta; commit atomico + push `main` dopo ogni blocco.
- Split dei package = rimozione/change di contratti cross-package: eseguire
  `bash scripts/ci/pre-removal-verify.sh` prima del push.
- Nessun fallback `nil`/noop/stub nel wiring produttivo; capability solo
  `DISABLED` / `READY` / `MISCONFIGURED` (vedi AGENTS.md §6).
- `worker_id` immutabile; i nomi operativi su `worker_name`.
- Ogni split deve preservare i gate: `check-architecture.sh`,
  `check-db-access.sh`, `check-capability-contract.sh` e i test di package.
