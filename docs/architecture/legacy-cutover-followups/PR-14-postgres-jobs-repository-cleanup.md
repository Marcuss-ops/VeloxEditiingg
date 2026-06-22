# PR-14 — PostgresJobsRepository suite cleanup

> **Audit anchor:** [§P1.2](../LEGACY_SSOT_AUDIT.md#p12--repository-job-mantiene-api-runtime) — mirror PostgreSQL dell'`internal/jobs`.
> **Target milestone:** post-cutover P1 (parallelo a PR-08).
> **Branch:** `cutover/pr-14-postgres-jobs-cleanup`
> **Dipendenze:** PR-08 (che fissa la nuova firma di `jobs.Writer`).

## Contesto

`DataServer/internal/store/sqlite_jobs_writer.go` (target SQLite)
viene semplificato da PR-08 a sole 4 operazioni business
(`SetAggregateStatus`, `Cancel`, `FailAggregate`, `Delete`).

Tuttavia `DataServer/internal/store/postgres_jobs_repository.go` ha
ancora l'intera suite PR3 paths runtime:

```text
PR3Start
PR3Fail
PR3RenewLease
PR3Cancel
PR3RecordRenderFinished
PR3RequeueExpiredLeases
```

più i CAS guards commentati `// PR #9: lease_id, lease_expiry,
assigned_to, claimed_by, retry_count columns dropped` — i.e. il
mirror PG ha ancora la logica CAS per colonne che nel repo SQLite
sono state droppate in 048.

Conseguenza: dopo PR-08 il target SQLite è "pulito" ma il mirror PG
rimane una fonte di verità alternativa che confonde il single-writer
rule dell'audit.

## Scope

- Eliminare da `postgres_jobs_repository.go` i metodi PR3 runtime
  non più richiesti:
  - `PR3Start`, `PR3Fail`, `PR3RenewLease`, `PR3RecordRenderFinished`,
    `PR3RequeueExpiredLeases`.
- Aggiungere le 4 firme business `SetAggregateStatus`, `Cancel`,
  `FailAggregate`, `Delete` come wrapper su `UPDATE jobs` (in
  transazione PG, non SQLite).
- Aggiornare i test `postgres_jobs_repository_test.go` affinché
  riflettano la nuova interfaccia.
- Verificare che PostgreSQL 13+ sia ancora la baseline (test
  pre-fallimento se versione diversa).

## Files to touch

```text
DataServer/internal/store/postgres_jobs_repository.go
DataServer/internal/store/postgres_jobs_repository_test.go
DataServer/internal/store/postgres_artifact_repository.go    # confronto
DataServer/internal/store/contracts/factories_jobs_postgres.go
DataServer/internal/store/contracts/job_repository_postgres_contract_test.go
DataServer/internal/jobs/repository.go                       # firma ridotta (PR-08)
DataServer/cmd/server/bootstrap_postgres_dispatch_test.go    # dispatch test
```

## Sequenza operativa

```text
1. PR-08 avrà già ridotto jobs.Writer. Importare la nuova firma.
2. In postgres_jobs_repository.go:
   - Rimuovere funzioni PR3*.
   - Implementare le 4 business wrapped in tx pgx.
   - Commentare esplicitamente "This Postgres repository is the
     MIRROR of the SQLite jobs.Writer; same SingleWriter rule applies.
     StatusSucceeded writes are NOT allowed here: see PR-02 single
     writer."
3. Rimuovere tutti i commenti "PR #9: columns dropped" che sono
   diventati irrilevanti (i CAS guard non ci sono più).
4. Test: convertire le vecchie `TestPR3*` in nuovi test per le 4
   business.
5. CI: assicurarsi che `run-tests-postgres.sh` (repository root)
   passi.
```

## Acceptance criteria

- [ ] `grep '^func.*PR3' postgres_jobs_repository.go` non ritorna
      risultati.
- [ ] `grep '^func.*\(SetAggregateStatus\|Cancel\|FailAggregate\|Delete\)' postgres_jobs_repository.go`
      ritorna 4 risultati (più eventuali helpers).
- [ ] `postgres_jobs_repository_test.go` non contiene test con nomi
      `TestPR3*`.
- [ ] `run-tests-postgres.sh` verde.
- [ ] Niente COMMENTI residui che parlino di `assigned_to, lease_id`
      nelle query SQL PG (le colonne sono state droppate anche in
      SQLite; PG deve essere simmetrico).

## Test

- **Unit (PG):** test delle 4 nuove business operations usando
  testcontainers PostgreSQL 13-alpine.
- **Contract:** `job_repository_postgres_contract_test.go` aggiornato
  alla nuova firma (eredita gli scenari da PR-08 contract test).
- **Cross-backend invariant:** stesso identico comportamento tra
  SQLite e PG per le 4 operazioni.
- **Smoke:** `run-tests-postgres.sh` deve passare.

## CI guards introdotti

In `check-single-writer.sh`:

```bash
# Vietato (definizione O chiamata) nei due file:
#   DataServer/internal/store/sqlite_jobs_writer.go
#   DataServer/internal/store/postgres_jobs_repository.go
# I pattern:
#   PR3Start\(|PR3Fail\(|PR3RenewLease\(|PR3RecordRenderFinished\(|PR3RequeueExpiredLeases\(
```

In `check-architecture.sh`:

```bash
# Vietata l'acquisizione dell'interfaccia jobs.Repository con metodi
# non business in qualunque nuovo file.
```

## Rischi

- PG potrebbe essere di sola lettura in alcuni ambienti: i test
  containerizzati devono essere l'unica fonte di verità. Non testare
  PG in CI direttamente se è OPZIONALE: rendere il job CI opt-in.
- I contratti cross-backend sono delicati; eventuali differenze tra
  SQLite e PG (es. timestamp, NULL handling) vanno documentate.

## Out of scope

- Rimozione del mirror PG (è ancora attivo per produzione).
- Refactor di `pgx` driver.
- Test di concorrenza PG (rinviato a oltre DoD).

---

> [!NOTE]
> Se la spec roadmap decide di **abbandonare PostgreSQL**, PR-14
> collassa a "rimozione mirror". Aggiornare questo design-doc in
> quella evenienza.
