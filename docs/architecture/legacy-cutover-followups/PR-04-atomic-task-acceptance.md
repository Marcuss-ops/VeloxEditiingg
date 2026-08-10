# PR-04 — Atomic task acceptance

> **Audit anchor:** [§P1.5](../LEGACY_SSOT_AUDIT.md#p15--start-task-e-creazione-attempt-non-atomici) (e prerequisito storico di PR-06/08).
> **Target milestone:** cutover P1.
> **Dipendenze:** PR-03 (Attempt ID canonico). PR-01 utile ma non bloccante.
>
> **Stato documentale:** piano storico riconciliato con `main` il 2026-08-10.
> Il percorso atomico descritto sotto è già presente; resta da verificare e
> completare soltanto il timestamp `task_attempts.started_at`.

## Contesto storico

Una Task poteva passare a `RUNNING` prima che il relativo `TaskAttempt`
venisse creato. Se la creazione dell'Attempt falliva, il risultato poteva
essere:

```text
Task.status = RUNNING
TaskAttempt = assente
```

Il percorso corrente su `main` elimina questa finestra creando l'Attempt
`PENDING` durante il claim e promuovendo Task e Attempt nella stessa
transazione durante l'accept. Il campo canonico di assegnazione è
`tasks.worker_id`/`task_attempts.worker_id`; `assigned_worker_id` non è una
colonna o un modello persistente corrente.

## Percorso canonico corrente su `main`

```text
ListReadyCandidates / claim
  → ClaimTaskForWorkerAtomic o ClaimNextWithAttemptAtomic
  → Task LEASED + TaskAttempt PENDING
  → TaskOffer{task_id, job_id, attempt_id, lease_id, attempt_number, revision}
  → handleTaskAccepted
  → AcceptTaskAtomic
  → Task RUNNING + TaskAttempt RUNNING + Job RUNNING
  → TaskLeaseGranted
```

### Punti di assegnazione dell'identità

- `DataServer/internal/store/sqlite_task_atomic.go:128-145`
  - `ClaimNextWithAttemptAtomic` genera e assegna `attempt_id`,
    `attempt_number`, `worker_id` e `lease_id` sul task.
- `DataServer/internal/store/sqlite_task_atomic.go:173-181`
  - persiste il `TaskAttempt` `PENDING` con la stessa identità.
- `DataServer/internal/store/sqlite_task_lease_claim.go`
  - `ClaimTaskForWorkerAtomic` applica la stessa garanzia al percorso scelto
    dal placement matcher.
- `DataServer/internal/grpcserver/handler_placement.go:157-240`
  - proietta la tupla canonica nel `TaskOffer`.
- `DataServer/internal/grpcserver/handler_accept.go:39-240`
  - `handleTaskAccepted` valida la tupla e delega la mutation a
    `AcceptTaskAtomic`.

### Punto ancora da aggiornare nel codice

`DataServer/internal/store/sqlite_task_atomic_accept.go:90-98` aggiorna oggi
l'Attempt con `status = 'RUNNING'` e `updated_at`, ma non assegna esplicitamente
`task_attempts.started_at`.

Il contratto completo richiede, nella stessa transazione:

```sql
SET status = 'RUNNING',
    started_at = COALESCE(started_at, ?),
    updated_at = ?
```

`tasks.started_at` e `jobs.started_at` vengono invece già assegnati nel
percorso `AcceptTaskAtomic`.

## Scope corrente

- Mantenere `AcceptTaskAtomic` come unico writer della promozione
  `LEASED → RUNNING` / `PENDING → RUNNING`.
- Mantenere il claim anticipato come unico punto di assegnazione canonica di
  `attempt_id`, `worker_id` e `lease_id` prima del `TaskOffer`.
- Completare `task_attempts.started_at` e il relativo test transazionale.
- Non introdurre writer paralleli negli handler.

## File e simboli canonici

```text
DataServer/internal/store/sqlite_task_atomic.go
  ClaimNextWithAttemptAtomic

DataServer/internal/store/sqlite_task_lease_claim.go
  ClaimTaskForWorkerAtomic

DataServer/internal/store/sqlite_task_atomic_accept.go:39
  AcceptTaskAtomic

DataServer/internal/grpcserver/handler_placement.go:54
  sendPushTaskOffer

DataServer/internal/grpcserver/handler_placement.go:157
  sendClaimedTaskOffer

DataServer/internal/grpcserver/handler_accept.go:39
  handleTaskAccepted

DataServer/internal/taskgraph/repository.go
  Writer.AcceptTaskAtomic
```

## Sequenza operativa

```text
1. ClaimTaskForWorkerAtomic / ClaimNextWithAttemptAtomic:
     BEGIN TRANSACTION;
       UPDATE tasks
          SET status='LEASED', worker_id=?, lease_id=?,
              attempt_id=?, attempt_number=?;
       INSERT task_attempts(..., status='PENDING', ...);
     COMMIT;

2. TaskOffer porta task_id/job_id/attempt_id/lease_id/attempt_number/revision;
   `worker_id` resta vincolato alla riga Master e all'envelope/sessione gRPC
   autenticata.

3. AcceptTaskAtomic:
     BEGIN TRANSACTION;
       UPDATE tasks
          SET status='RUNNING', started_at=? ...;
       UPDATE task_attempts
          SET status='RUNNING',
              started_at=COALESCE(started_at, ?),
              updated_at=? ...;
       UPDATE jobs
          SET status='RUNNING',
              started_at=COALESCE(started_at, ?) ...;
       INSERT master execution event;
     COMMIT;

4. Dopo il commit, il Master invia TaskLeaseGranted con la tupla canonica.
```

## Acceptance criteria

- [x] Il percorso task-native di produzione passa `tasks.status` a `RUNNING`
      tramite la transazione `AcceptTaskAtomic`; l'API storica
      `LifecycleService.Start`/`SQLiteTaskRepository.Start` resta una
      superficie di compatibilità da non usare per il dispatch corrente.
- [x] Il claim crea `TaskAttempt PENDING` e assegna `attempt_id` prima del
      `TaskOffer`.
- [x] Task e Attempt vengono promossi atomicamente a `RUNNING`.
- [ ] `task_attempts.started_at` viene valorizzato al passaggio
      `PENDING → RUNNING` nella stessa transazione.
- [ ] Esiste un test mirato che verifica `started_at` e il rollback in caso di
      fallimento del CAS dell'Attempt.
- [x] L'invariante §9.5 (Task RUNNING ⇒ Attempt RUNNING) è protetto dal CAS
      atomico e dai test esistenti.

## Test richiesti

- Unit/integration su `AcceptTaskAtomic` con CAS mismatch e rollback.
- Claim → TaskOffer → TaskAccepted → TaskLeaseGranted con verifica della
  tupla completa.
- Verifica SQL che non esista Task `RUNNING` senza Attempt attivo.
- Dopo il fix, asserzione che `task_attempts.started_at` sia valorizzato e
  non venga sovrascritto in un accept replay.

## Read model live e rischi

La proiezione operativa del worker è separata dalla storia canonica:

```text
heartbeat.active_jobs
  → PersistWorkerHeartbeat
  → worker_task_runtime
```

`worker_task_runtime` può essere aggiornato prima del `TaskResult` e deve
essere usato solo per la vista live. Non sostituisce `task_attempts`, che resta
l'autorità per la storia dell'Attempt.

Un `started_at` mancante sull'Attempt rende incompleti la durata, il lookup
`current_task` ordinato per avvio e la correlazione immediata del read model
admin.

## CI guards

I guard devono impedire scritture di `tasks.status='RUNNING'` fuori dal
percorso atomico e devono verificare che il percorso accept aggiorni Task e
Attempt nella stessa transazione. I riferimenti storici a `lifecycle.go` e ai
repository pre-PR-2 non sono più i punti canonici su `main`.

## Out of scope

- Reaper (PR-05).
- Ingestione completa del report (PR-06).
- Recovery di Task orfane oltre l'invariante §9.5.
