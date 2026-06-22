# Runbook operativo 01 — Cutover da `workflow` a `taskgraph`

Status: **COMPLETED** (giugno 2026)

Data snapshot: 2026-06-22

> **Stato attuale.** Il package `DataServer/internal/workflow` è stato
> rimosso. La tabella canonical `taskgraph` + `task_attempts` +
> `task_phase_timings` + `task_attempt_metrics` + `task_attempt_reports`
> + `artifact_attachments` rappresenta la sola fonte di verità per lo
> stato runtime. Vedere [docs/architecture/OWNERSHIP.md §Removed
> packages](../architecture/OWNERSHIP.md#removed-packages-historical-retained-for-traceability)
> per la storia del pacchetto rimosso.
>
> Questo runbook viene mantenuto come **retrospettiva operativa** del
> cutover, non come guida attiva. Le sezioni "Fase 0..Fase 7" sotto
> restano utili come storico per operatori che vogliono ricostruire
> _perché_ il codice attuale è fatto così; non vanno ri-eseguite.

Obiettivo (originario): eliminare la doppia fonte di verità tra `DataServer/internal/workflow` e il nuovo dominio canonico composto da `taskgraph`, `taskattempts`, `observability`, `jobs`, `artifacts` e `outbox`.

Questo documento non autorizza un dual-write temporaneo. Durante tutto il cutover deve esistere un solo writer per ogni stato mutabile.

## 1. Problema attuale

Oggi il repository contiene due modelli che descrivono lavoro dipendente:

- `internal/workflow` crea run, step e dipendenze, gestisce transizioni di stato e sblocca gli step successivi;
- `internal/taskgraph` definisce Task persistenti, stato, revisioni, dipendenze e lifecycle canonico;
- `/api/v1/orchestrator/jobs` continua a scrivere nel vecchio `workflow.Repository`;
- `buildTasks` costruisce il nuovo stack, ma il percorso HTTP e parte del percorso di dispatch continuano a usare il modello precedente.

Questa convivenza non deve diventare permanente. Il target è:

```text
Job
  -> RenderPlan immutabile
      -> TaskGraph persistente
          -> Task
              -> TaskAttempt
                  -> Artifact verificato
```

## 2. Regole non negoziabili

1. `taskgraph.Repository` è l'unico writer dello stato Task.
2. `taskattempts.Repository` è l'unico writer di tentativi, report e metriche.
3. `jobs.Repository` e `jobs.LifecycleService` restano proprietari dello stato Job.
4. `artifacts.Service` resta l'unico gate che può finalizzare output verificati.
5. Nessun handler esegue SQL diretto.
6. Nessun adapter di compatibilità può scrivere sia su `workflow` sia su `taskgraph`.
7. Gli adapter temporanei sono read-only, hanno issue proprietaria e data di rimozione.
8. Il master pianifica e sblocca dipendenze; i worker eseguono Task assegnate senza reinterpretare il progetto.

## 3. Risultato finale richiesto

Al termine del cutover:

- una richiesta crea un Job e un TaskGraph nella stessa transazione logica;
- gli endpoint orchestrator leggono dal nuovo dominio;
- il dispatcher assegna Task pronte, non step del vecchio workflow;
- i report worker creano o aggiornano TaskAttempt;
- il completamento verificato di una Task sblocca le dipendenze;
- il Job viene finalizzato solo quando il grafo è concluso e gli artifact richiesti sono verificati;
- `internal/workflow` e le relative tabelle vengono eliminati oppure mantenuti esclusivamente come archivio read-only con scadenza esplicita;
- non esistono due scheduler, due lifecycle o due grafi autorevoli.

## 4. Sequenza operativa

### Fase 0 — Baseline e inventario

Prima di modificare il codice:

```bash
git fetch origin
git checkout main
git pull --ff-only origin main
make verify-fast
```

Registrare in una issue dedicata:

- risultato di `make verify-fast`;
- endpoint che importano `internal/workflow`;
- servizi o background runner che chiamano `CreateRun`, `MarkStepRunning`, `CompleteStepAndReleaseDependents`, `FailStep` o `CancelRun`;
- tabelle SQLite e Postgres usate dal vecchio workflow;
- outbox event type che iniziano con `WORKFLOW_`;
- test che dipendono da DTO o stato del vecchio modello.

Comandi utili:

```bash
git grep -n 'internal/workflow'
git grep -n 'CreateRun\|MarkStepRunning\|CompleteStepAndReleaseDependents\|FailStep\|CancelRun'
git grep -n 'WORKFLOW_'
git grep -n 'workflow_runs\|workflow_steps\|workflow_dependencies\|workflow_events'
```

Output richiesto: una tabella nell'issue con colonna `caller`, `tipo accesso`, `writer/read-only`, `destinazione nuova`, `PR di rimozione`.

### Fase 1 — Congelare il vecchio writer

Creare una guardia CI che impedisca nuovi writer nel package `workflow`.

Azioni:

1. Aggiornare `docs/architecture/OWNERSHIP.md` dichiarando esplicitamente che `internal/workflow` è in dismissione.
2. Aggiungere una regola in `scripts/ci/check-single-writer.sh` che permetta le mutazioni workflow solo nei file già censiti.
3. Vietare nuovi import di `internal/workflow` fuori dall'adapter di compatibilità e dai test di migrazione.
4. Aggiungere il blocco di compatibilità ai file mantenuti temporaneamente:

```go
// COMPATIBILITY:
// Owner:        issue #<numero>
// Remove after: <YYYY-MM-DD>
// Read-only:    yes
```

La fase è completa quando una nuova chiamata write al vecchio repository fa fallire la CI.

### Fase 2 — Definire il contratto di creazione canonico

Creare un application service unico, ad esempio:

```text
DataServer/internal/renderjobs/CreateService
```

oppure estendere un owner esistente, evitando un nuovo dominio parallelo se `creatorflow` è già il composition point corretto.

Responsabilità del servizio:

1. validare input e RenderPlan;
2. creare il Job;
3. derivare le Task e le dipendenze;
4. creare Job + Task nella stessa transazione tramite `store.AtomicJobTaskCreator`;
5. pubblicare gli eventi outbox necessari;
6. restituire un risultato stabile con `job_id`, task count e stato iniziale.

Il servizio non deve:

- creare workflow step;
- eseguire SQL;
- chiamare direttamente il worker;
- duplicare validazione già presente in registry, jobs o taskgraph;
- mantenere stato autorevole in memoria.

Test obbligatori:

- Job e Task vengono creati atomicamente;
- errore nella creazione di una Task esegue rollback del Job;
- dipendenze inesistenti vengono rifiutate;
- le Task senza predecessori partono `READY`;
- le Task con predecessori partono `BLOCKED`;
- una richiesta ripetuta con la stessa idempotency key non duplica il grafo.

### Fase 3 — Migrare `/api/v1/orchestrator/jobs`

File principale coinvolto:

```text
DataServer/cmd/server/router.go
```

Procedura:

1. Sostituire la dipendenza `workflow.Repository` con il nuovo application service.
2. Conservare temporaneamente la shape HTTP esterna solo se serve al frontend.
3. Convertire il payload legacy in un RenderPlan o TaskSpec tipizzato all'edge.
4. Non passare `[]map[string]interface{}` oltre l'handler.
5. Restituire errori validati con codici stabili.
6. Rimuovere dal router qualsiasi costruzione manuale di `WorkflowSpec`.

Target del POST:

```text
HTTP request
  -> DTO validation
  -> canonical compiler/service
  -> AtomicJobTaskCreator
  -> Job + TaskGraph
```

Per le GET legacy:

- implementare un adapter read-only che proietta Job e Task nei vecchi campi;
- non aggiornare mai le vecchie tabelle;
- annotare il codice con owner e data di rimozione;
- aggiungere header o campo deprecation se compatibile con i client.

Criteri di completamento:

- nessun POST scrive su `workflow.Repository`;
- `git grep 'CreateRun('` mostra solo test storici o codice destinato alla rimozione;
- test HTTP verificano la persistenza del nuovo TaskGraph.

### Fase 4 — Dispatch di Task, non di Job opachi

Il dispatcher deve selezionare una Task pronta e costruire un contratto worker tipizzato.

Azioni:

1. Introdurre o completare il repository query per le Task `READY`.
2. Implementare claim con CAS/revision o lease, evitando doppia assegnazione.
3. Applicare il cost model al `TaskSpec`, non a payload opachi ricostruiti manualmente.
4. Trasmettere almeno:
   - task ID;
   - job ID;
   - executor ID e versione;
   - input artifact references;
   - output contract;
   - requirements;
   - attempt number;
   - lease deadline.
5. Creare TaskAttempt prima o atomicamente con l'assegnazione.
6. Su rifiuto o timeout, rilasciare la lease e aggiornare il tentativo attraverso il lifecycle canonico.

Non aggiungere un secondo scheduler. Estendere il percorso già presente in `grpcserver` e `costmodel` oppure introdurre `internal/scheduler` come unico owner e spostare lì la selezione, eliminando la logica precedente nello stesso cutover.

Test obbligatori:

- due goroutine non possono claimare la stessa Task;
- worker incompatibile non riceve la Task;
- worker offline o draining è escluso;
- una lease scaduta torna recuperabile;
- un riavvio master ricostruisce lo stato da SQLite.

### Fase 5 — Ingestione report e sblocco dipendenze

Il worker deve inviare report tipizzati. Il master deve:

1. validare task ID, attempt ID, worker ID e lease;
2. scrivere il report in `taskattempts.Repository`;
3. verificare gli artifact prodotti;
4. completare la Task tramite `taskgraph.LifecycleService`;
5. sbloccare le Task dipendenti nella stessa transazione o tramite outbox idempotente;
6. aggiornare il Job solo quando le condizioni del grafo sono soddisfatte;
7. passare da `artifacts.Service` per la finalizzazione.

Gestire esplicitamente:

- report duplicato;
- report tardivo da lease scaduta;
- worker che si riconnette con nuova sessione;
- artifact mancante o hash errato;
- retry esauriti;
- cancellazione del Job mentre una Task è in esecuzione.

### Fase 6 — Migrazione dati legacy

Non eseguire dual-write.

Scegliere una delle due strategie:

#### Strategia A — Cutover senza import storico

Usarla se i vecchi workflow non devono riprendere dopo deploy.

- bloccare la creazione di nuovi workflow;
- lasciare le vecchie GET read-only per una finestra limitata;
- creare nuovi Job solo nel TaskGraph;
- archiviare ed eliminare le tabelle dopo la scadenza.

#### Strategia B — Import una tantum

Usarla se esistono workflow attivi da recuperare.

- fermare i writer;
- esportare un backup verificato;
- eseguire un migrator una tantum con checkpoint;
- convertire ogni run in Job + TaskGraph;
- preservare ID esterni in una tabella mapping read-only;
- verificare conteggi e stati;
- riaprire il traffico solo dopo il confronto.

Il migrator deve essere idempotente e produrre un report:

```text
runs letti
runs convertiti
runs ignorati
steps convertiti
dipendenze convertite
errori
mapping legacy -> canonical
```

### Fase 7 — Eliminazione definitiva

Solo dopo almeno un ciclo operativo stabile:

1. rimuovere route write legacy;
2. rimuovere adapter read-only se la data di rimozione è raggiunta;
3. eliminare package `internal/workflow` se non ha più responsabilità;
4. eliminare background handler e outbox consumer `WORKFLOW_*` sostituiti;
5. creare migrazioni forward-only per rimuovere le vecchie tabelle quando sicuro;
6. eliminare DTO, test e config legacy;
7. aggiornare README, ownership e roadmap.

Comandi finali:

```bash
git grep -n 'internal/workflow' || true
git grep -n 'WORKFLOW_' || true
git grep -n 'workflow_runs\|workflow_steps\|workflow_dependencies\|workflow_events' || true
make verify
```

## 5. Suddivisione consigliata in cambi piccoli

1. Guard CI e ownership del cutover.
2. Application service di creazione Job + TaskGraph.
3. Migrazione POST orchestrator.
4. Adapter GET read-only.
5. Claim e dispatch Task.
6. Report TaskAttempt e dependency release.
7. Migrazione dati legacy, se necessaria.
8. Eliminazione package e tabelle workflow.

Ogni cambiamento deve avere una sola responsabilità e test mirati.

## 6. Definition of done

Il cutover è concluso quando:

- esiste un solo grafo autorevole;
- nessun handler scrive nel vecchio workflow;
- nessun dual-write esiste;
- tutte le Task vengono create, claimate, eseguite e concluse attraverso owner canonici;
- i retry appartengono a TaskAttempt;
- il Job riflette il risultato aggregato del TaskGraph;
- la finalizzazione passa da artifact verificati;
- il master può riavviarsi senza perdere stato;
- `make verify` è verde;
- i test end-to-end coprono Job -> Task -> Worker -> Attempt -> Artifact -> Finalizzazione.

## 7. Rollback

Il rollback non deve riattivare due writer.

Durante il cutover usare feature rollout solo all'edge:

- prima del cutover, writer legacy attivo e nuovo writer inattivo;
- al cutover, writer legacy disabilitato e writer nuovo attivo;
- in caso di rollback, fermare il traffico, ripristinare il database dal backup coerente e riattivare un solo writer.

È vietato mantenere entrambi attivi per facilitare un rollback.