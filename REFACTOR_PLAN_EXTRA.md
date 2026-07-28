# REFACTOR_PLAN_EXTRA.md — VeloxEditiingg

> **Piano supplementare** per il workspace `VeloxEditiingg/`.
> Complementare a `VeloxFrontend/REFACTOR_PLAN.md` (che copre `VeloxEditiingg/VeloxFrontend/web/dark_editor/`).
> Stessa metodologia (commit atomici su `main`, NO branch, push immediato).

- **Repo**: `git@github.com:Marcuss-ops/VeloxEditiingg`
- **Aree principali**:
  - `DataServer/` — Go backend (largest contributor)
  - `RemoteCodex/native/worker-agent-go/` — Go worker agent
  - `RemoteCodex/native/video-engine-cpp/` — C++ video engine
  - `shared/` — Go contracti condivisi
  - `VeloxFrontend/web/dark_editor/` — già coperto dal plan principale
- **Soglia**: ≤ 400 LOC per file post-refactor (`*_test.go` ≤ 600)
- **Scan**: `find VeloxEditiingg -type f ... | sort -rn | head -30` (2026-07-28, esclusi binari / `node_modules` / `vcpkg_installed` / `.git`)

## Top 20 file (ranked by LOC, esclusi binari)

| # | LOC | Path | Tipo | Ruolo |
|---|---:|---|---|---|
| 1 | **1561** | `DataServer/internal/handlers/server/pipeline/job_submit.go` | prod | Pipeline job submission handler |
| 2 | 1240 | `DataServer/internal/handlers/server/pipeline/job_submit_e2e_test.go` | e2e test | E2E test del submit flow |
| 3 | 1235 | `DataServer/api/openapi.yaml` | spec | OpenAPI spec (non refactor in code-sense, ma da spezzare in parti) |
| 4 | 1160 | `DataServer/internal/handlers/server/pipeline/job_submit_test.go` | unit test | Unit test del submit handler |
| 5 | 946 | `DataServer/internal/remoteengine/client_test.go` | unit test | Remote engine client tests |
| 6 | 863 | `VeloxFrontend/web/dark_editor/components/editor/ExportDialog.tsx` | prod | (già coperto da REFACTOR_PLAN.md §3.6) |
| 7 | 840 | `DataServer/internal/remoteengine/errors_test.go` | unit test | Error mapping tests |
| 8 | 816 | `VeloxFrontend/web/dark_editor/components/editor/canvas/CanvasRenderers.tsx` | prod | (già coperto da REFACTOR_PLAN.md §3.10) |
| 9 | 789 | `DataServer/internal/workers/registry_test.go` | unit test | Worker registry tests |
| 10 | 771 | `DataServer/internal/completion/e2e_test.go` | e2e test | Completion protocol E2E |
| 11 | 761 | `DataServer/internal/store/sqlite_task_atomic_persistence.go` | prod | SQLite task atomic persistence |
| 12 | 757 | `tests/operational/artlist_live_e2e_verify.sh` | shell | E2E verification shell script |
| 13 | 756 | `DataServer/internal/store/store_creator_forwardings_test.go` | unit test | Store creator forwardings tests |
| 14 | 751 | `shared/contract/deliveryplan/parser_test.go` | unit test | Delivery plan parser tests |
| 15 | 737 | `tests/e2e/grpc-control-plane/run.sh` | shell | gRPC control plane E2E |
| 16 | 734 | `cmd/archcheck/scan/percheck_voiceover_alias_ban_test.go` | unit test | Architectural check |
| 17 | 725 | `DataServer/internal/handlers/server/pipeline/creator_push_e2e_test.go` | e2e test | Creator push E2E |
| 18 | 721 | `VeloxFrontend/web/dark_editor/stores/templateStore.ts` | prod | (già coperto da REFACTOR_PLAN.md §3.2) |
| 19 | 712 | `DataServer/internal/store/sqlite_task_atomic_claim_test.go` | unit test | Atomic claim tests |
| 20 | 699 | `docs/SOCIAL_API_MIGRATION_RUNBOOK.md` | docs | Migration runbook |

## Top production code (non-test) — focus del refactor

| # | LOC | Path | Ruolo | Rischio |
|---|---:|---|---|---|
| 1 | **1561** | `DataServer/internal/handlers/server/pipeline/job_submit.go` | Pipeline job submission handler | 🔴 CRITICAL |
| 2 | 761 | `DataServer/internal/store/sqlite_task_atomic_persistence.go` | SQLite atomic task persistence | 🟡 HIGH |

Più le 7 entry VeloxFrontend/web/dark_editor (già coperte da REFACTOR_PLAN.md) e i file RemoteCodex (top: `main.go` 660 LOC, `config.go` 618 LOC, `cache.go` 497 LOC, `compiler.go` 469 LOC, ecc.).

## Estrazioni proposte (DataServer)

### `DataServer/internal/handlers/server/pipeline/job_submit.go` (1561 LOC) — 🔴 CRITICAL
Ruolo: orchestratore pipeline submit (validation → queue → track → ack).
Dipendenti: altri handler in `pipeline/`, store/, workers/.

Estrazioni:
- `pipeline/job_submit.go` (~200 LOC) — entrypoint handler
- `pipeline/job_submit_validation.go` (~250 LOC) — payload validation
- `pipeline/job_submit_queue.go` (~300 LOC) — coda + dispatch
- `pipeline/job_submit_track.go` (~250 LOC) — tracking + progress
- `pipeline/job_submit_persistence.go` (~250 LOC) — DB writes
- `pipeline/job_submit_response.go` (~150 LOC) — formatting response

### `DataServer/internal/store/sqlite_task_atomic_persistence.go` (761 LOC) — 🟡 HIGH
Estrazioni:
- `sqlite_task_atomic_persistence.go` (~200 LOC) — facade pubblica
- `sqlite_task_atomic_claim.go` (~200 LOC) — claim logic
- `sqlite_task_atomic_release.go` (~150 LOC) — release
- `sqlite_task_atomic_retry.go` (~150 LOC) — retry/backoff

### `DataServer/internal/openapi.yaml` (1235 LOC) — 🟢 spec-only
Non è codice eseguibile, ma se ne raccomanda lo split in più file con `$ref`:
- `openapi.yaml` (root, paths summary)
- `openapi/schemas.yaml`
- `openapi/paths.yaml`
- `openapi/parameters.yaml`
- `openapi/responses.yaml`

## Estrazioni RemoteCodex (già dettagliate nel vecchio piano)

Sintesi (mantenuta per continuità):
- `cmd/velox-worker-agent/main.go` (660) → split in commands/*
- `pkg/config/config.go` (618) → structs / loader / validate
- `internal/transport/grpc_stream.go` (462) → encoder / retry / errors
- `internal/workercache/cache.go` (497) → store / eviction
- `internal/worker/worker_*.go` (3 file 449/433/429) → persistence / init / claimloop / lifecycle
- `internal/taskrunner/runner.go` (429) → split per state
- `video-engine-cpp/src/services/ffmpeg_progress_parser.cpp` (483) → Parser / Token / State
- `video-engine-cpp/src/core/render_engine.cpp` (479) → Engine / RenderContext

## Rischi specifici
- **DataServer pipeline/job_submit.go**: è probabilmente il cuore dell'orchestrazione server. Refactor rischioso → aggiungere prima test E2E (già esistono 1240 LOC di test).
- **`openapi.yaml`**: lo split richiede tooling compatibile con `$ref`; molte pipeline lo gestiscono, ma verificare che `oapi-codegen` risolva correttamente.
- **shell scripts in `tests/operational/*.sh`** (757 LOC) e `tests/e2e/*.sh`: refactor possibile ma a basso valore — sono read-once.
- **Duplicazione VeloxFrontend/web/dark_editor**: già coperto; questo plan referenzia ma non duplica.
- **RemoteCodex/test files di 661/627/479 LOC**: molti test file grossi (transport_registry_test.go, job_executor_dispatch_test.go, resolver_test.go) — considerare split in più test file, ma test file grossi in Go sono tollerati.

## Suggested execution order (Fase 1)

1. `DataServer/internal/store/sqlite_task_atomic_persistence` (4 commit) — basso rischio
2. `DataServer/internal/openapi.yaml` split (5 commit, mantiene backward compat)
3. `DataServer/internal/handlers/server/pipeline/job_submit` (6 commit) — più rischioso, dopo i test
4. `RemoteCodex/pkg/config/` (5 commit) — isolato
5. `RemoteCodex/internal/transport/grpc_stream` (4 commit)
6. `RemoteCodex/internal/worker/` (3 file × 4 commit = 12 commit)
7. `RemoteCodex/cmd/main` split (3 commit)
8. `RemoteCodex/video-engine-cpp` split (10 commit)

Stima: **~49 commit** totali per VeloxEditiingg.
