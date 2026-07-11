# PR-13 — Verify and fix Job-side reaper post-migration 048

> **Audit anchor:** [§P0.4](../LEGACY_SSOT_AUDIT.md#p04--lease-task-senza-expiry-persistita) — effetto collaterale della migration 048.
> **Target milestone:** cutover P0 (parallelo a PR-05).
> **Branch:** `cutover/pr-13-verify-job-reaper-048`
> **Dipendenze:** PR-11; PR-05 benefica (tabella `tasks.lease_expires_at`).

## Contesto

L'audit P0.4 dichiara che il reaper è "ancora orientato ai Job" e che
"una Task già accettata e in stato RUNNING può rimanere bloccata per
sempre". L'analisi empirica rileva che `RequeueExpiredLeases` su
`jobs.Writer` è implementato e gira ad ogni tick da
`DataServer/cmd/server/bootstrap.go:384`.

Tuttavia `jobs.lease_expiry` colonna è stata droppata in
`DataServer/internal/store/migrations/sqlite/048_drop_jobs_runtime_columns.sql`.

Conseguenza attesa: dal merge del 048 in poi, il reaper Job
potrebbe essere silenziosamente rotto (SQL `WHERE lease_expiry < ?`
riferito a colonna inesistente) senza che nessun crash o allarme
segnali il problema finché non si manifesta una perdita di Job.

PR-13 chiude questo gap temporale PRIMA che PR-05 rimuova del tutto
il reaper Job (perché a regime non sarà più necessario: la lease
expiry è sulle tasks).

## Scope

- Verifica SQL del `PR3RequeueExpiredLeases` su
  `sqlite_jobs_writer_pr3.go` e del rispettivo mirror su
  `postgres_jobs_repository.go`: il `WHERE` clause non deve
  riferirsi a colonne droppate.
- Se il reaper è rotto, **disattivarlo con un log esplicito**
  durante il periodo di cutover, finché PR-05 non introduce il Task
  reaper.
- Aggiungere un `health` endpoint o uno status in `outbox` che
  rilevi l'assenza del reaper.
- Aggiungere un integration test che applichi 048 e poi tenti una
  reap: deve fare fail-graceful, non panic.

## Files to touch

```text
DataServer/internal/jobs/repository.go             # RequeueExpiredLeases interface doc
DataServer/internal/jobs/lifecycle_service.go      # doc + safe-skip durante PR-05
DataServer/internal/store/sqlite_jobs_writer_pr3.go # clausola WHERE
DataServer/internal/store/postgres_jobs_repository.go # clausola WHERE
DataServer/cmd/server/bootstrap.go                # gate al reaper + log esplicito
DataServer/cmd/server/bootstrap_jobs.go           # telemetria
DataServer/internal/health/*.go (se esiste)        # sezione "reaper_status"
```

## Sequenza operativa

```text
1. Static check: copiare il file migrations su un DB di test; eseguire
   il reaper. Se errore SQL → è rotto.
2. Se rotto:
     a) Inserire gate `if hasLegacyReaperColumns(sqlStore)` →
        skip graceful + log strutturato {"component":"job_reaper","state":"disabled_until_cutover"}.
     b) Restituire da RequeueExpiredLeases un risultato vuoto invece
        di fallire.
3. Aggiungere una metrica Prometheus health.label
   velox_job_reaper_disabled_until_cutover.
4. Aggiungere test integration `TestJobReaper_DisabledPost048`.
5. Documentare in OWNERSHIP.md il dual-status "leasy reaper on tasks
   (PR-05) + disabled on jobs (interim post 048, pre PR-05)".
```

## Acceptance criteria

- [ ] `go test ./cmd/server/...` test di avvio con DB post-migration 048:
      il server parte senza errori SQL.
- [ ] Log strutturato `job_reaper_disabled_until_cutover` emesso
      esattamente una volta per boot.
- [ ] Nessun `WHERE posizione_riferita_a_cols_dropee` nei due
      `RequeueExpiredLeases` runtime-active.
- [ ] Test `TestJobReaper_DisabledPost048` verde.
- [ ] OWNERSHIP.md annota il dual-status.

## Test

- **Unit:**
  - `lifecycle_service_test.go` con un mock repo che ritorna `nil`
    invece di errori quando la `lease_expiry` non esiste più.
- **Integration:**
  - `bootstrap_jobs_test.go` (se non esiste, creare): applica
    migration 048, esegue `bootstrap.go`, asserisce no panic.
- **Smoke:**
  - Test CLI: `velox-server migrate --status` non riporta il reaper
    Job come rotto.

## CI guards introdotti

```bash
# scripts/ci/check-migrations.sh — estensione:
# Dopo aver applicato fino a 048, eseguire RequeueExpiredLeases su DB vuoto
# e verificare:
#   - nessun errore SQL,
#   - ritorno slice vuota,
#   - log "disabled" presente.
```

In `check-architecture.sh`:

```bash
# Vietato: imports che assumono lease_expiry in jobs dopo 048.
```

## Rischi

- Se PR-13 NON viene mergiata prima di PR-05, e PR-05 rimuove i
  reaper Job, si rischia un buco temporale di observability
  (nessuno sa se il reaper è attivo o no). Mitigazione: PR-13 deve
  entrare PRIMA di PR-05.

## Out of scope

- Aggiungere `tasks.lease_expires_at` (PR-05).
- Introdurre il TaskLeaseReaper transazionale (PR-05).
- Rimuovere `RequeueExpiredLeases` da `jobs.Writer` (PR-08 lo coprirà).
