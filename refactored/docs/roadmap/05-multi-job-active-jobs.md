# 05 — Multi-Job: currentJob → activeJobs Map

## Stato attuale

Il `Worker` ha un singolo `currentJob` (`worker_types.go:44`):

```go
type Worker struct {
    // ...
    currentJob *api.Job
    // ...
}
```

Il `jobLoop` (`worker_jobs.go:15`) chiama `executeJob` in modo **sincrono** e non torna al polling
finché il job non è completato:

```go
case <-ticker.C:
    if w.Status() != StatusIdle { continue }
    job, err := w.pollJob(ctx)
    if job != nil {
        w.executeJob(ctx, job)  // bloccante!
    }
```

Nonostante esista `concurrencyLimiter` e `max_parallel_jobs`, il percorso principale è di fatto
**1 worker process = 1 job attivo**.

### Problemi

- `max_parallel_jobs > 1` nella config è ingannevole: non viene realmente usato per esecuzione parallela
- `heartbeat` riporta UN solo `currentJob`
- `lease renewal` rinnova UN solo lease
- `cancel_job` cerca in `jobCancelFuncs` ma può cancellare solo il job corrente
- `progress` traccia UN solo progresso

## Stato target

Il worker supporta **N slot di esecuzione parallela**:

```go
type Worker struct {
    activeJobs   map[string]*ActiveJob    // jobID → job context
    activeJobsMu sync.RWMutex
}

type ActiveJob struct {
    Job       *api.Job
    LeaseID   string
    StartedAt time.Time
    Cancel    context.CancelFunc
    Progress  JobProgress
}

type JobProgress struct {
    Percent int32
    Scene   int32
    Total   int32
    Stage   string
}
```

Modello di esecuzione:

1. `jobLoop` non è più bloccante: quando riceve un job, lo lancia in una goroutine e torna subito
   al polling (se ci sono slot liberi).
2. `concurrencyLimiter` controlla effettivamente il numero di job attivi.
3. `heartbeat` riporta TUTTI i job attivi.
4. `lease renewal` rinnova il lease per OGNI job attivo.
5. `progress` è per-job, non globale.
6. `cancel_job` cancella il job specifico dalla mappa.

### Nota

Se si preferisce il modello semplice (1 worker = 1 job), questo task si riduce a:
- Rimuovere `max_parallel_jobs > 1` dalla configurazione o documentarlo come "riservato per futuro"
- Rendere esplicito nel codice che il modello è single-job

## File coinvolti

| File | Azione |
|---|---|
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_types.go` | `currentJob *api.Job` → `activeJobs map[string]*ActiveJob` |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_jobs.go` | `jobLoop` non bloccante, goroutine per job |
| `RemoteCodex/native/worker-agent-go/internal/worker/job_executor.go` | `executeJob` lavora su `ActiveJob` |
| `RemoteCodex/native/worker-agent-go/internal/worker/worker_comms.go` | `heartbeat` e `leaseRenewal` iterano su `activeJobs` |
| `RemoteCodex/native/worker-agent-go/internal/worker/concurrency.go` | Già pronto — va solo integrato |

## Definition of Done

- [ ] `Worker.activeJobs` è `map[string]*ActiveJob` con mutex
- [ ] `ActiveJob` struct con `Job`, `LeaseID`, `StartedAt`, `Cancel`, `Progress`
- [ ] `jobLoop` lancia `executeJob` in una goroutine e torna subito al polling (se slot disponibili)
- [ ] `concurrencyLimiter` controlla `len(activeJobs) < maxActiveJobs`
- [ ] `executeJob` registra l'`ActiveJob` nella mappa prima di eseguire, lo rimuove dopo
- [ ] `sendHeartbeat` riporta tutti gli `activeJobs` con i loro progressi
- [ ] `leaseRenewLoop` rinnova il lease per ogni `ActiveJob`
- [ ] `cancelJob` cerca per `jobID` in `activeJobs`
- [ ] `Stop()` / `Drain` cancella tutti gli `activeJobs` e attende terminazione
- [ ] Test: avvio 2 job in parallelo → entrambi eseguiti
- [ ] Test: `maxActiveJobs=1` → solo un job alla volta (retrocompatibile)
- [ ] Test: heartbeat contiene tutti i job attivi
- [ ] Test: lease renewal per ogni job

## Criteri di test

```bash
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestMultiJob
cd refactored/RemoteCodex/native/worker-agent-go && go test ./internal/worker/... -v -run TestConcurrency
```

## Dipendenze

- Indipendente.
- Facilita 04 (re-registration loop) perché il loop deve gestire il resume dei job dopo reconnect.
