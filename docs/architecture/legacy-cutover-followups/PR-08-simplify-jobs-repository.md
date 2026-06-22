# PR-08 — Simplify `jobs.Repository`

> **Audit anchor:** [§P1.2](../LEGACY_SSOT_AUDIT.md#p12--repository-job-mantiene-api-runtime)
> **Target milestone:** post-cutover P1.
> **Branch:** `cutover/pr-08-simplify-jobs-repository`
> **Dipendenze:** PR-02 (single SUCCEEDED writer), PR-04 (atomic
> acceptance), PR-06 (ingestion). PR-05 desiderabile.

## Contesto

L'interfaccia `jobs.Writer` contiene ancora metodi di esecuzione:

```text
Lease, Start, RenewLease, ClaimNext, ClaimNextForProfile,
ReleaseLease, RecordRenderFinished, RequeueExpiredLeases, FailWithRetry
```

Concetti che appartengono a Task e TaskAttempt, non all'aggregato
business `Job`.

## Scope

- Ridurre `velox-server/internal/jobs.Writer` (e Reader coerentemente)
  a sole operazioni **business**:
  - `SetAggregateStatus`,
  - `Cancel`,
  - `FailAggregate`,
  - `Delete`.
- Spostare `ClaimNext`, `ClaimNextForProfile`, `Lease`, `Start`,
  `RenewLease`, `ReleaseLease`, `RecordRenderFinished`,
  `RequeueExpiredLeases`, `FailWithRetry` nei repository di Task /
  TaskAttempt.
- Eliminare SQL e test obsoleti.

## Files to touch

```text
velox-server/internal/jobs/lifecycle_service.go
velox-server/internal/jobs/jobs_writer_types.go
velox-server/internal/store/sqlite_jobs_writer.go
velox-server/internal/store/sqlite_jobs_writer_pr3.go
velox-server/internal/store/postgres_jobs_repository.go
velox-server/internal/store/store_jobs_mutation.go
velox-server/internal/store/store_jobs.go
velox-server/internal/taskgraph/repository.go              # assorbimento Lease/Start/...
velox-server/internal/taskattempts/repository.go           # assorbimento RecordRenderFinished
velox-server/internal/grpcserver/handler_jobs.go           # adattamento caller
velox-server/internal/jobs/lifecycle_service_test.go
```

## Sequenza operativa

```text
1. CENSIRE le call site dei metodi runtime (Lease, Start, RenewLease,
   ClaimNext, ClaimNextForProfile, ReleaseLease,
   RecordRenderFinished, RequeueExpiredLeases, FailWithRetry).
2. Per ogni metodo, definire la nuova casa (taskgraph vs.
   taskattempts vs. lifecycle service).
3. Aggiornare ogni caller.
4. Ridurre l'interfaccia Writer.
5. Eliminare SQL: rivedere `sqlite_jobs_writer*.go` e
   `postgres_jobs_repository.go` rimuovendo statement non più referenziati.
6. Aggiornare i test mock.
7. Eseguire l'invariant test (vedi sotto).
```

## Acceptance criteria

- [ ] `grep -R "func.*Lease\b\|func.*Start\b\|func.*RenewLease\b\|func.*Claim" velox-server/internal/jobs/` non trova corrispondenze
      runtime.
- [ ] Caller aggiornati: ogni metodo ha un'unica casa.
- [ ] Test mock dei rimanenti metodi ridotti a 4 (`SetAggregateStatus`,
      `Cancel`, `FailAggregate`, `Delete`).
- [ ] CI guard §9.2 dell'audit (nessun runtime Job lease) passa.
- [ ] Test `runtime_job_methods_removed_test` verde.

## Test

- **Unit:** test per le 4 operazioni rimanenti.
- **Integration:** E2E con nessun metodo runtime su `jobs.Writer`.
- **Architectural invariant:**
  `check-single-writer.sh: jobs_writer_methods_count == 4`

## CI guards introdotti

In `check-single-writer.sh`:

```bash
# Vietato (definizione o chiamata):
#   Job.ClaimNext, Job.ClaimNextForProfile,
#   Job.Lease, Job.Start, Job.RenewLease, Job.ReleaseLease,
#   Job.RecordRenderFinished, Job.RequeueExpiredLeases,
#   Job.FailWithRetry.
#   solo se il path non è taskgraph/taskattempts.
```

In `check-no-legacy.sh`:

```text
# Niente Claim/Release/RenewLease su jobs (full-tree).
```

## Rischi

- Refactor di superficie ampia (chiama PostgreSQL + SQLite + contract
  tests). Rimanere atomico per package.
- Regressione in transaction boundary: il contract test
  `contracts/job_repository_contract_test.go` deve continuare a
  passare con il numero ridotto di metodi.

## Out of scope

- Eliminazione completa del file `store_jobs_mutation.go` sezione
  runtime (potrebbe restare sezione business).
- Rimozione PostgresJobsRepository (è il mirror — verifica solo
  congruenza).

---

> [!NOTE]
> Questa PR è il candidato naturale per **rinominare** `jobs.Writer`
> in qualcosa come `jobs.AggregateWriter` o `jobs.JobAggregate` per
> riflettere la natura business dei 4 metodi rimanenti. La PR
> successiva (oltre PR-10) può occuparsene in isolamento.
