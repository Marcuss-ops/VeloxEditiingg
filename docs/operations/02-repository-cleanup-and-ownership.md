# Runbook operativo 02 — Pulizia repository, ownership e rimozione del legacy

Status: operativo

Data snapshot: 2026-06-21

Obiettivo: ridurre complessità accidentale, dipendenze residue, documentazione falsa, percorsi duplicati e compatibilità senza scadenza. Questo runbook non elimina componenti canonici: elimina soltanto superfici morte, transitorie o duplicate.

## 1. Principio guida

Ogni capacità importante deve avere:

- un solo owner;
- un solo writer;
- un solo entrypoint canonico;
- un solo registry, resolver o sampler;
- una documentazione allineata al codice reale;
- una guardia CI che impedisca la regressione.

La pulizia non deve essere mescolata con nuove feature. Ogni intervento va eseguito come cambiamento piccolo, verificabile e reversibile.

## 2. Ordine obbligatorio

Eseguire i lavori in questo ordine:

1. baseline e inventario;
2. rimozione commenti e documentazione falsa;
3. aggiornamento ownership e CODEOWNERS;
4. pulizia dipendenze;
5. consolidamento migrazioni;
6. eliminazione config alias e compatibilità scadute;
7. separazione frontend dal core;
8. scansione finale e guard CI.

Non iniziare dal punto 7 se i punti 2-6 non sono stabili.

## 3. Baseline e inventario

Preparazione:

```bash
git fetch origin
git checkout main
git pull --ff-only origin main
git status -sb
make verify-fast
```

Produrre un inventario con questi comandi:

```bash
git grep -n 'TODO('
git grep -n 'legacy\|deprecated\|compatibility\|old path\|temporary'
git grep -n 'intentionally broken\|not yet wired\|was dropped'
git grep -n 'os.Getenv' -- 'DataServer/**/*.go' ':!DataServer/internal/config/**'
git grep -n 'CREATE TABLE\|ALTER TABLE\|DROP TABLE' -- 'DataServer/**/*.go'
git grep -n 'map\[string\].*Executor\|switch.*job.*type\|supported_job_types'
```

Classificare ogni risultato:

- `vero e necessario`;
- `commento obsoleto`;
- `compatibilità con owner e scadenza`;
- `compatibilità senza owner`;
- `codice morto`;
- `duplicazione di owner`;
- `falso positivo`.

Non eliminare codice soltanto in base al nome. Confermare sempre import, caller, test e runtime path.

## 4. Correggere documentazione e commenti non veritieri

### 4.1 Commenti platform/database

Controllare e rimuovere i commenti in:

```text
DataServer/cmd/server/bootstrap.go
DataServer/internal/store/sqlite.go
```

che dichiarano `internal/platform/database` assente o la build intenzionalmente rotta, perché il package è presente ed è usato.

La sostituzione corretta deve descrivere lo stato reale, per esempio:

- SQLite è il backend operativo completo;
- Postgres è disponibile a livello connection/repository parziale;
- il cutover Postgres end-to-end non è concluso;
- non esiste una build intenzionalmente rotta.

Aggiungere un test o una guardia che impedisca la ricomparsa delle frasi obsolete:

```bash
! git grep -n 'build is intentionally broken\|build è intenzionalmente rotta'
```

### 4.2 Roadmap distributed rendering

Aggiornare:

```text
docs/architecture/distributed-rendering/README.md
docs/architecture/distributed-rendering/PR-01-TASK-CONTRACTS-OBSERVABILITY.md
docs/architecture/distributed-rendering/PR-03-EXECUTOR-REGISTRY-WORKERS.md
docs/architecture/distributed-rendering/PR-04-SCHEDULER-COST-SHARDING.md
```

La roadmap deve distinguere:

- `LANDED ON MAIN`;
- `PARTIAL`;
- `OPEN`;
- `SUPERSEDED`;
- `REMOVAL PENDING`.

Per ogni voce landed indicare:

- file o package canonico;
- commit di landing;
- test di accettazione;
- cosa manca realmente.

Correggere in particolare le sezioni che dichiarano mancanti package già presenti, come `taskgraph`, `taskattempts`, `observability`, `executor`, `taskrunner` e `costmodel`.

La roadmap non deve usare il nome di un branch come stato operativo dopo che il codice è entrato in `main`.

### 4.3 README principale

Il README deve descrivere soltanto la struttura corrente. Verificare che:

- non elenchi package eliminati;
- non chiami canonico un percorso deprecato;
- distingua runtime master, worker, shared, deploy e frontend;
- chiarisca che la SPA non è parte del renderer distribuito;
- rimandi ai tre runbook operativi.

## 5. Allineare `OWNERSHIP.md` e `CODEOWNERS`

File coinvolti:

```text
docs/architecture/OWNERSHIP.md
.github/CODEOWNERS
scripts/ci/check-single-writer.sh
scripts/ci/check-registry.sh
scripts/ci/check-architecture.sh
```

Aggiungere ownership esplicita per:

```text
DataServer/internal/taskgraph/
DataServer/internal/taskattempts/
DataServer/internal/observability/
DataServer/internal/costmodel/
RemoteCodex/native/worker-agent-go/internal/executor/
RemoteCodex/native/worker-agent-go/internal/taskrunner/
RemoteCodex/native/worker-agent-go/internal/resource/
RemoteCodex/native/worker-agent-go/pkg/cache/
RemoteCodex/native/worker-agent-go/pkg/blob/
```

Rimuovere o marcare come proibiti percorsi non più esistenti, ad esempio `DataServer/internal/obs/`, se il package canonico è `internal/observability`.

Aggiungere una verifica CI che confronti le directory canoniche dichiarate in `OWNERSHIP.md` con una allowlist in `CODEOWNERS`. La verifica può essere semplice e deterministica; non deve tentare di interpretare Markdown genericamente.

Criterio di completamento:

- ogni package canonico ha owner;
- ogni writer ha una regola single-writer;
- ogni registry ha una regola anti-duplicazione;
- nessun percorso morto resta in CODEOWNERS.

## 6. Pulizia dipendenze Go

Moduli:

```text
DataServer
RemoteCodex/native/worker-agent-go
shared
```

Procedura:

```bash
for mod in DataServer RemoteCodex/native/worker-agent-go shared; do
  (cd "$mod" && go mod tidy && go mod verify)
done

git diff -- '*.mod' '*.sum' go.work.sum
```

Verifiche mirate:

1. `github.com/jdkato/prose/v2` deve sparire dal worker se `pkg/nlp` è stato eliminato e non esistono altri import.
2. `github.com/lib/pq` deve sparire dal server se `pgx/v5/stdlib` è l'unico driver PostgreSQL usato.
3. Dipendenze indirect non devono essere rimosse manualmente: lasciare decidere a `go mod tidy`.
4. Non aggiornare versioni per opportunità durante questa pulizia; separare gli upgrade in un altro cambiamento.

Comandi di controllo:

```bash
git grep -n 'github.com/jdkato/prose'
git grep -n 'github.com/lib/pq'
(cd DataServer && go list -deps ./... >/tmp/velox-server-deps.txt)
(cd RemoteCodex/native/worker-agent-go && go list -deps ./... >/tmp/velox-worker-deps.txt)
```

Test:

```bash
make fmt-check
make vet
make verify-fast
```

## 7. Consolidamento delle migrazioni

### 7.1 Target canonico

Il solo layout consentito deve essere:

```text
DataServer/internal/store/migrations/sqlite/*.sql
DataServer/internal/store/migrations/postgres/*.sql
DataServer/internal/store/migrations/runner.go
```

Nuove migrazioni non devono essere aggiunte alla root `migrations/`.

### 7.2 Inventario

```bash
find DataServer/internal/store/migrations -maxdepth 2 -type f -name '*.sql' -print | sort
git grep -n 'go:embed.*\.sql' DataServer/internal/store/migrations DataServer/internal/store
```

Creare una tabella di mapping:

```text
vecchio file root
nuovo file sqlite/postgres
stesso effetto SQL
DB che potrebbe aver applicato il vecchio numero
strategia di compatibilità
rimozione prevista
```

### 7.3 Eliminare tolleranze generiche

Il runner non deve ignorare per sempre:

- `duplicate column` su qualunque `ALTER TABLE ADD COLUMN`;
- `no such column` su qualunque `DROP COLUMN`.

Queste tolleranze possono nascondere errori futuri.

Procedura corretta:

1. identificare esattamente le migrazioni duplicate del cutover;
2. gestire la compatibilità con una migrazione o preflight specifico e nominato;
3. testare database in tre stati:
   - nuovo database vuoto;
   - database che ha applicato il vecchio percorso root;
   - database già sul nuovo percorso dialect-aware;
4. rimuovere la tolleranza generica;
5. far fallire ogni errore SQL non previsto.

Test richiesti:

- checksum invariati per migrazioni applicate;
- nessuna versione duplicata nello stesso dialect;
- upgrade da snapshot legacy;
- bootstrap da database vuoto;
- secondo bootstrap idempotente;
- errore reale su colonna duplicata non autorizzata.

### 7.4 `postMigrationAdjustments`

Ogni `ensureColumn` ancora presente deve essere classificato:

- necessario per compatibilità temporanea;
- migrabile in SQL canonico;
- morto.

Portare le aggiunte di schema nel sistema migrations. Lasciare `postMigrationAdjustments` solo se esiste un motivo operativo documentato e una data di rimozione.

## 8. Configurazione e compatibilità

Cercare alias e fallback:

```bash
git grep -n 'LookupEnv\|Getenv\|fallback\|alias\|deprecated' DataServer/internal/config DataServer/cmd RemoteCodex/native/worker-agent-go
```

Per ogni variabile:

- scegliere il nome canonico;
- documentarlo nell'env example;
- aggiungere validazione centralizzata;
- migrare i caller;
- mantenere eventualmente un alias read-only con warning e scadenza;
- rimuovere l'alias alla scadenza.

È vietato:

- leggere env direttamente in handler;
- avere due variabili con precedenza implicita;
- accettare wildcard o allowlist vuote in produzione;
- usare fallback silenziosi per credenziali, worker identity o storage.

## 9. Separazione del frontend dal core

### 9.1 Decisione target

Velox core resta headless. Il frontend deve diventare un prodotto separato che consuma API del master.

Target:

```text
velox-core repository
  DataServer
  worker-agent
  video-engine
  shared
  deploy

velox-frontend repository
  React/Vite SPA
  test frontend
  build e release statici
```

### 9.2 Preparazione

Prima di spostare file:

1. documentare tutte le route usate dalla SPA;
2. verificare autenticazione, CORS e base URL;
3. produrre un build statico riproducibile;
4. aggiungere una release/versione indipendente;
5. definire dove viene ospitata la SPA.

### 9.3 Modifiche core

Dopo la disponibilità del frontend separato:

- rimuovere `frontend_standalone/` dal core;
- rimuovere `DataServer/internal/app/frontend.go`;
- rimuovere SPA fallback e static file serving;
- mantenere una landing API minimale soltanto se operativamente utile;
- configurare CORS tramite config validata, non con host hardcoded nel router;
- aggiornare Dockerfile master: nessuna dipendenza dal bundle SPA;
- aggiornare README e deploy.

### 9.4 Migrazione sicura

Per una release di transizione è possibile servire il frontend esterno mantenendo il vecchio bundle disabilitato di default. Non mantenere due build pipeline permanenti.

Criteri di completamento:

```bash
test ! -d frontend_standalone
! git grep -n 'VELOX_SPA_DIR\|ServeSPA\|frontend_standalone/web/dist'
make verify
```

## 10. Eliminazione codice morto

Per ogni candidato:

1. verificare zero import e zero caller;
2. verificare eventuale uso via reflection, registry o string ID;
3. rimuovere codice e test dedicati al codice morto;
4. eseguire `go mod tidy`;
5. aggiungere una guardia se il simbolo rappresentava un vecchio percorso architetturale;
6. non mantenere wrapper vuoti “nel caso servano”.

Comandi:

```bash
go list ./... >/tmp/packages.txt
git grep -n '<simbolo>'
make verify-fast
```

Non usare il numero di linee rimosse come criterio di successo. Il criterio è la riduzione di owner e percorsi alternativi.

## 11. Guardie CI da aggiungere o rafforzare

La CI deve fallire quando:

- ricompare `internal/queue`;
- viene introdotto un secondo map/switch di executor;
- un handler esegue SQL diretto;
- nasce una nuova migration SQL fuori dalle directory dialect-aware;
- compare una compatibilità senza owner e data;
- viene committato `node_modules`, `dist`, output o binari;
- un package canonico manca da CODEOWNERS;
- compaiono segreti o identificatori infrastrutturali reali;
- un file generato non è riproducibile;
- una route write usa il vecchio workflow.

Le guardie devono essere specifiche e con messaggi di errore che indicano:

- regola violata;
- file trovato;
- owner canonico da usare;
- comando di correzione.

## 12. Sequenza di cambi consigliata

1. Correzione commenti falsi e roadmap.
2. Allineamento CODEOWNERS/OWNERSHIP.
3. `go mod tidy` dei tre moduli.
4. Inventario e test migrazioni legacy.
5. Rimozione embed/migrazioni duplicate.
6. Rimozione tolleranze SQL generiche.
7. Pulizia config alias.
8. Preparazione split frontend.
9. Rimozione frontend dal core.
10. Scan finale dead code + nuove guardie.

## 13. Definition of done

La pulizia è conclusa quando:

- documentazione e codice descrivono lo stesso stato;
- non esistono commenti che dichiarano assenti package presenti;
- ogni owner canonico è in CODEOWNERS;
- `go mod tidy` non produce diff;
- esiste un solo percorso migrations per dialect;
- non ci sono compatibilità senza owner e data;
- il frontend non è più una dipendenza del core headless;
- non sono versionati output, cache, binari o `node_modules`;
- le guardie CI impediscono la regressione;
- `make verify` passa da checkout pulito.

## 14. Controllo finale

```bash
git status -sb
git diff --check
git grep -n 'intentionally broken\|was dropped' || true
git grep -n 'internal/obs' || true
git grep -n 'frontend_standalone/web/dist' || true
find . -type d -name node_modules -o -name dist
for mod in DataServer RemoteCodex/native/worker-agent-go shared; do
  (cd "$mod" && go mod tidy && git diff --exit-code -- go.mod go.sum)
done
make verify
```

Conservare il log della verifica e il commit SHA come evidenza operativa.