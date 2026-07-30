# REFACTOR_PLAN_EXTRA.md — VeloxEditiingg

> **Piano supplementare** per il workspace `VeloxEditiingg/`.
> Complementare a `VeloxFrontend/REFACTOR_PLAN.md` (path relativo: `../VeloxFrontend/REFACTOR_PLAN.md`).
> Stessa metodologia (commit atomici su `main`, NO branch, push immediato).

- **Repo**: `git@github.com:Marcuss-ops/VeloxEditiingg`
- **Aree principali**:
  - `DataServer/` — Go backend (largest contributor)
  - `RemoteCodex/native/worker-agent-go/` — Go worker agent
  - `RemoteCodex/native/video-engine-cpp/` — C++ video engine
  - `shared/` — Go contracti condivisi
  - `VeloxFrontend/web/dark_editor/` — React editor (già coperto dal plan principale)
- **Soglia**: ≤ 400 LOC per file post-refactor (`*_test.go` ≤ 600)
- **Scan**: `find VeloxEditiingg -type f ... | sort -rn | head -30` (2026-07-28, esclusi binari)

---

## Already covered (cross-ref §VeloxFrontend/REFACTOR_PLAN.md)

I seguenti file `VeloxFrontend/web/dark_editor/` (con LOC > 700) **sono già analizzati** nel piano principale e **non** vengono ri-analizzati qui per evitare duplicazione.

| File | LOC | Sezione di riferimento |
|---|---:|---|
| `VeloxFrontend/web/dark_editor/components/editor/ExportDialog.tsx` | 863 | §3.6 (ExportDialog) |
| `VeloxFrontend/web/dark_editor/components/editor/canvas/CanvasRenderers.tsx` | 816 | §3.10 (CanvasRenderers) |
| `VeloxFrontend/web/dark_editor/stores/templateStore.ts` | 721 | §3.2 (TemplateStore) |

---

## Top-20 by type (esclusi binari + file già coperti sopra)

| # | LOC | Type | Path | Area |
|---|---:|---|---|---|
| 1 | 1561 | Go-prod | `DataServer/internal/handlers/server/pipeline/job_submit.go` | DataServer pipeline |
| 2 | 1240 | Go-test | `DataServer/internal/handlers/server/pipeline/job_submit_e2e_test.go` | DataServer pipeline |
| 3 | 1235 | spec | `DataServer/api/openapi.yaml` | DataServer API |
| 4 | 1160 | Go-test | `DataServer/internal/handlers/server/pipeline/job_submit_test.go` | DataServer pipeline |
| 5 | 946  | Go-test | `DataServer/internal/remoteengine/client_test.go` | DataServer remote |
| 6 | 840  | Go-test | `DataServer/internal/remoteengine/errors_test.go` | DataServer remote |
| 7 | 789  | Go-test | `DataServer/internal/workers/registry_test.go` | DataServer workers |
| 8 | 771  | Go-test | `DataServer/internal/completion/e2e_test.go` | DataServer completion |
| 9 | 761  | Go-prod | `DataServer/internal/store/sqlite_task_atomic_persistence.go` | DataServer store |
| 10 | 757 | shell | `tests/operational/artlist_live_e2e_verify.sh` | e2e harness |
| 11 | 756 | Go-test | `DataServer/internal/store/store_creator_forwardings_test.go` | DataServer store |
| 12 | 751 | Go-test | `shared/contract/deliveryplan/parser_test.go` | shared contracti |
| 13 | 737 | shell | `tests/e2e/grpc-control-plane/run.sh` | e2e harness |
| 14 | 734 | Go-test | `cmd/archcheck/scan/percheck_voiceover_alias_ban_test.go` | archcheck |
| 15 | 725 | Go-test | `DataServer/internal/handlers/server/pipeline/creator_push_e2e_test.go` | DataServer pipeline |
| 16 | 712 | Go-test | `DataServer/internal/store/sqlite_task_atomic_claim_test.go` | DataServer store |
| 17 | 699 | docs | `docs/SOCIAL_API_MIGRATION_RUNBOOK.md` | docs |
| 18 | 660 | Go-prod | `RemoteCodex/native/worker-agent-go/cmd/velox-worker-agent/main.go` | RemoteCodex |
| 19 | 618 | Go-prod | `RemoteCodex/native/worker-agent-go/pkg/config/config.go` | RemoteCodex |
| 20 | 497 | Go-prod | `RemoteCodex/native/worker-agent-go/internal/workercache/cache.go` | RemoteCodex |

### Distribuzione per type (20 file)

| Type | Count | Range LOC |
|---|---:|---|
| Go-prod | 5 | 497 → 1561 |
| Go-test | 11 | 712 → 1240 |
| spec | 1 | 1235 |
| shell | 2 | 737 → 757 |
| docs | 1 | 699 |
| TS | 0 | (già coperti) |
| C++ (RemoteCodex) | 0 (in top-20) | vedi §"RemoteCodex appendix" |

> Nota: il C++ `RemoteCodex` ha file grossi (ffmpeg_progress_parser.cpp 483, render_engine.cpp 479, cmd_full_video.cpp 414) ma escono dalla top-20 per via dei tanti test file DataServer.

---

## Top production code (focus del refactor)

| # | LOC | Path | Ruolo | Rischio |
|---|---:|---|---|---|
| 1 | 1561 | `DataServer/internal/handlers/server/pipeline/job_submit.go` | Pipeline job submission handler | 🔴 CRITICAL |
| 2 | 761 | `DataServer/internal/store/sqlite_task_atomic_persistence.go` | SQLite atomic task persistence | 🟡 HIGH |
| 3 | 660 | `RemoteCodex/.../cmd/velox-worker-agent/main.go` | Worker-agent CLI entry | 🟢 MEDIUM |
| 4 | 618 | `RemoteCodex/.../pkg/config/config.go` | Worker-agent config | 🟢 MEDIUM |

---

## Estrazioni proposte (DataServer)

### `DataServer/internal/handlers/server/pipeline/job_submit.go` (1561 LOC) — 🔴 CRITICAL
Ruolo: orchestratore pipeline submit (validation → queue → track → ack).
Estrazioni:
- `pipeline/job_submit.go` (~200 LOC) — entrypoint handler
- `pipeline/job_submit_validation.go` (~250 LOC) — payload validation
- `pipeline/job_submit_queue.go` (~300 LOC) — coda + dispatch
- `pipeline/job_submit_track.go` (~250 LOC) — tracking + progress
- `pipeline/job_submit_persistence.go` (~250 LOC) — DB writes
- `pipeline/job_submit_response.go` (~150 LOC) — formatting response
- Residui (~150 LOC): import block + error vars + facade shell

### `DataServer/internal/store/sqlite_task_atomic_persistence.go` (761 LOC) — 🟡 HIGH
Estrazioni:
- `sqlite_task_atomic_persistence.go` (~200 LOC) — facade
- `sqlite_task_atomic_claim.go` (~200 LOC)
- `sqlite_task_atomic_release.go` (~150 LOC)
- `sqlite_task_atomic_retry.go` (~150 LOC)

### `DataServer/api/openapi.yaml` (1235 LOC) — 🟢 spec-only
Split consigliato (rimanda §"Tooling" per il tool):
- `openapi.yaml` (root)
- `openapi/schemas.yaml`
- `openapi/paths.yaml`
- `openapi/parameters.yaml`
- `openapi/responses.yaml`

---

## Estrazioni RemoteCodex (estratto da top-20)

### `RemoteCodex/.../cmd/velox-worker-agent/main.go` (660 LOC)
Split in `commands/<subcmd>.go` (~50-150 LOC ciascuno) + facade ~80 LOC.

### `RemoteCodex/.../pkg/config/config.go` (618 LOC)
Split in `pkg/config/{structs,loader,validate}.go` (~150-250 LOC ciascuno).

### `RemoteCodex/.../internal/workercache/cache.go` (497 LOC)
Split in `internal/workercache/{cache,store,eviction}.go` (~150 LOC ciascuno).

---

## Additional RemoteCodex files (>600 LOC, beyond top-30)

| LOC | Path | Note |
|---:|---|---|
| 660 | `RemoteCodex/native/worker-agent-go/cmd/velox-worker-agent/main.go` | Go-prod |
| 618 | `RemoteCodex/native/worker-agent-go/pkg/config/config.go` | Go-prod |
| 497 | `RemoteCodex/native/worker-agent-go/internal/workercache/cache.go` | Go-prod |
| 469 | `RemoteCodex/native/worker-agent-go/pkg/video/pipelines/hybrid/compiler.go` | Go-prod |
| 462 | `RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream.go` | Go-prod |
| 460 | `RemoteCodex/native/worker-agent-go/internal/publisher/manifest.go` | Go-prod |
| 449 | `RemoteCodex/native/worker-agent-go/internal/worker/worker_persistence.go` | Go-prod |
| 433 | `RemoteCodex/native/worker-agent-go/internal/worker/worker_init.go` | Go-prod |
| 429 | `RemoteCodex/native/worker-agent-go/internal/worker/worker_claimloop.go` | Go-prod |
| 429 | `RemoteCodex/native/worker-agent-go/internal/taskrunner/runner.go` | Go-prod |

---

## Tooling specifico

- **OpenAPI split**: usare `oapi-codegen` (Go) + `swagger-cli bundle` per gestire `$ref` cross-file.
- **shell scripts**: nessun refactor proposto (costo/beneficio sfavorevole).

## Rischi specifici
- **DataServer pipeline/job_submit.go**: cuore dell'orchestrazione server. Refactor rischioso → usare i 1240 LOC di test E2E esistenti come safety net.
- **`openapi.yaml`**: split richiede tooling compatibile con `$ref`.
- **shell scripts in `tests/operational/*.sh`** (757 LOC) e `tests/e2e/*.sh`: low-value refactor.
- **RemoteCodex/test files > 600 LOC**: tollerati in Go convention.

## Suggested execution order (Fase 1)

1. `DataServer/internal/store/sqlite_task_atomic_persistence` (4 commit)
2. `DataServer/api/openapi.yaml` split (5 commit, mantiene backward compat)
3. `DataServer/internal/handlers/server/pipeline/job_submit` (6 commit)
4. `RemoteCodex/pkg/config/` (5 commit)
5. `RemoteCodex/cmd/main` split (3 commit)
6. `RemoteCodex/internal/workercache/` (3 commit)
7. `RemoteCodex/internal/transport/grpc_stream` (4 commit)
8. `RemoteCodex/internal/worker/` (3 file × 4 commit = 12 commit)
9. `RemoteCodex/video-engine-cpp` split (10 commit)

Stima: **~52 commit** totali per VeloxEditiingg.
