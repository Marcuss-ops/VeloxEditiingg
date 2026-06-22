# PR-05 — Task lease expiry + reaper

> **Audit anchor:** [§P0.4](../LEGACY_SSOT_AUDIT.md#p04--lease-task-senza-expiry-persistita)
> **Target milestone:** cutover P0
> **Branch:** `cutover/pr-05-task-lease-expiry-reaper`
> **Dipendenze:** PR-04 (transizioni task/attempt affidabili). PR-03
> utile per la firma di `RenewLease`.

## Contesto

Il master calcola una deadline di lease e la invia in `TaskOffer` /
`TaskLeaseGranted` ma la tabella `tasks` non la persiste. Il reaper
master-side è ancora orientato ai Job. Conseguenza: una Task `RUNNING`
con worker crashato **non viene mai recuperata**.

## Scope

- Aggiungere colonna `lease_expires_at TEXT` alla tabella `tasks`
  (migration `050_task_lease_expires.sql` o simile — numerazione da
  confermare al momento del merge).
- Estendere `taskgraph.Repository` con:
  - `RenewLease(ctx, taskID, leaseID, newExpiry) error`,
  - `RequeueExpiredLeases(ctx, now) (reaped []Task, error)`.
- Registrare un nuovo `TaskLeaseReaper` nel supervisor master-side.
- Rimuovere il reaper Job precedente (preserva solo l'invariant sui
  Job già accettati da PR-01/02: niente zombie `RUNNING`).
- Definire il comportamento del reaper come da audit:
  - `LEASED` scaduta → `READY`,
  - `RUNNING` scaduta con retry disponibile → `Attempt TIMED_OUT`,
    `Task READY`,
  - `RUNNING` scaduta con retry esauriti → `Attempt FAILED`,
    `Task FAILED`, `Job FAILED` (aggregato).

## Files to touch

```text
DataServer/internal/store/migrations/sqlite/050_task_lease_expires.sql
velox-server/internal/taskgraph/repository.go
velox-server/internal/taskgraph/lifecycle.go
velox-server/internal/taskgraph/reaper.go             # nuovo
velox-server/internal/taskattempts/repository.go      # TIMED_OUT
velox-server/internal/grpcserver/handler_workers.go   # TaskLeaseRenewal
velox-server/internal/jobs/lifecycle_service.go       # Job FAILED aggregato
cmd/server/bootstrap.go                              # registra TaskLeaseReaper
proto/velox/control/worker_control.proto              # TaskLeaseRenewal msg
```

## Sequenza operativa

```text
1. Migration 050:
     ALTER TABLE tasks ADD COLUMN lease_expires_at TEXT;

2. ClaimNextReadyTask scrive tasks.lease_expires_at = now + lease_ttl.
   AcceptTaskOffer (PR-04) non aggiorna la expiry (è solo start).
   Il worker può fare RenewLease via TaskLeaseRenewal: master aggiorna
   lease_expires_at = now + lease_ttl.

3. TaskLeaseReaper (tick configurabile, default 30s):
     SELECT id FROM tasks
       WHERE status IN ('LEASED','RUNNING')
         AND COALESCE(lease_expires_at,'') < ?
         AND worker_id IS NOT NULL
         AND lease_id IS NOT NULL;
     Per ogni id:
       - Attempt TIMED_OUT o FAILED in transazione,
       - Task READY o FAILED,
       - outbox event per Job.FAILED se esauriti.

4. Rimozione Job reaper legacy:
     - cancellazione del file con COMPATIBILITY block secondo OWNERSHIP.md.
     - check-single-writer.sh: nessun altro path scrive jobs zombie.
```

## Acceptance criteria

- [ ] Migration applicabile in test suite senza errori.
- [ ] Worker crash → entro 1× TTL le Task tornano `READY` o `FAILED`.
- [ ] `RenewLease` rifiuta lease non più attivi.
- [ ] Reaper Job legacy rimosso (o coperto da COMPATIBILITY block).
- [ ] `JobLeaseReaper` non lascia zombie se un nuovo Task gli succede.

## Test

- **Unit:**
  - `reaper_test.go` con tabella mocked.
  - `RenewLease` rifiuta lease scaduti.
- **Integration:**
  - simulare worker crash dopo AcceptTaskOffer: entro TTL, Task torna
    `READY` o `FAILED` a seconda di `attempt_count`.
  - simulare worker che rinnova lease due volte: nessuna reap.
- **Race:** due reaper concorrenti su stessa Task: atteso un solo
  TIMED_OUT.

## CI guards introdotti

In `check-single-writer.sh`:

```bash
# Reaper Job legacy rimosso: vietata la presenza di:
#   'jobs.LeaseReaper', 'JobReap', 'JobZombieReaper', 'job_lease_reaper'
```

In `check-migrations.sh`:

```bash
# Qualsiasi tabella `tasks` riferita senza lease_expires_at in SELECT
# deve essere vietata nelle PR future. Per ora solo check che la
# colonna è presente dopo migration 050.
```

## Rischi

- Riga di lease scaduta durante transazione di `RenewLease`:
  `RenewLease` deve usare CAS `lease_id=?`.
- Time skew tra master e worker: tolleranza minima configurabile
  (default 30s) prima di considerare scaduto.

## Out of scope

- Persistenza di `attempt_count` (PR-08 limpia il `jobs.Writer`):
  viene letto da `tasks.attempt_count` (o equivalente), non dal Job.
- Distribuzione della deadline renewal oltre TTL (follow-up).
