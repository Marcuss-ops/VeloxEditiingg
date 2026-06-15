# JSON → SQLite Migration Inventory

> Data: 2026-06-14  
> Repository: VeloxEditing  
> Branch: `main`  
> Stato: **Tutte le fasi completate**

---

## Principi architetturali

1. **Una entità di dominio, un repository, una fonte di verità.**
2. SQLite è il database predefinito e unico per tutti i dati runtime (`velox.db`, WAL mode).
3. I file JSON possono essere payload, backup o configurazione, **mai** una seconda persistenza concorrente a SQLite.
4. Il migration runner versionato con checksum SHA256 garantisce che ogni istanza abbia lo stesso schema.
5. L'audit (`internal/audit/data_layer.go`) blocca la reintroduzione di file JSON legacy in CI.

---

## Migration Runner

Il migration runner è in `internal/store/migrations/migrations.go`:

- **Tabella `schema_migrations`**: tiene traccia di ogni migration applicata con checksum SHA256.
- **Esecuzione transazionata**: ogni migration è atomica.
- **Checksum**: una migration già applicata **non può essere modificata** — il runner blocca l'avvio.
- **Ordine**: le migration vengono eseguite in ordine di versione numerica.

### Migration eseguite

| # | File | Descrizione |
|---|------|-------------|
| 001 | `001_initial.sql` | Schema iniziale consolidato (jobs, workers, YouTube, calendar, ansible, dark editor) |
| 002 | `002_legacy_imports.sql` | Tabella `legacy_imports`, colonne arricchite su `workers` |
| 003 | `003_youtube_canonical.sql` | Modello YouTube canonico (`youtube_channels`, `youtube_groups_v2`, `youtube_group_channels`, `youtube_tracked_niches`) |
| 004 | `004_ansible.sql` | Tabelle Ansible strutturate (`ansible_hosts`, `ansible_runs`, `ansible_run_hosts`) |
| 005 | `005_legacy_cleanup.sql` | Migration soft (solo documentazione — nessun DROP) |
| 006 | `006_drive_links_source_of_truth.sql` | Drive links SQLite fonte di verità, `drive_master_folders` |
| 007 | `007_queue_persistence.sql` | Coda persistente (orchestrator_jobs, dlq_jobs, job_events) |
| 008 | `008_drop_legacy_tables.sql` | **Data copy SAFE** — INSERT OR IGNORE da tabelle legacy → canoniche. Nessun DROP. + CI guard `legacy_json_registry` |
| 009 | `009_drop_legacy_tables.sql` | **DROP IRREVERSIBILE** delle tabelle legacy dopo verifica dati. Applicare solo dopo 008. |

---

## Inventario JSON — Stato di ogni file

### ✅ Migrati completamente (SQLite è l'unica fonte)

| File JSON originale | Nuova tabella SQLite | Cosa contiene | Note |
|---|---|---|---|
| `workers.json` | `workers` + `worker_flags` | Registro worker, stato, heartbeat, flag revoked | `NewWithPersistence` rimosso. `Registry` usa solo SQLite. `ReplaceWorkers()` → `UpsertWorker()` individuale. |
| `channels.json` (youtube) | `youtube_channels` (canonical) + `oauth token` files | Metadati canali, lingue | Unito con `ChannelsSaved.json` nel modello canonico. Token OAuth restano su file protetto. |
| `groups.json` (youtube) | `youtube_groups_v2` + `youtube_group_channels` | Gruppi upload con membership | Unito con i gruppi Storage nel modello canonico. |
| `ChannelsSaved.json` | `youtube_channels` + `youtube_group_channels` | Gruppi e canali YouTube Manager | Unito nel modello canonico. `storage.go` ora usa SQLite. |
| `ansible_computers.json` | `ansible_hosts` | Inventario computer Ansible | Strutturato con colonne. `secret_ref` al posto di `SSHPassword` in chiaro. |
| `ansible_runs.json` | `ansible_runs` + `ansible_run_hosts` | Cronologia esecuzioni Ansible | Memorizzato con host associati in tabella separata. |
| `youtube_api_cache.json` | `youtube_api_cache` (SQLite) | Cache risposte API YouTube | TTL gestito via timestamp. Get con fallback SQLite. |
| `analytics_cache.json` | `analytics_cache` (SQLite) | Cache analytics canali | Solo lettura legacy in `channel_handlers.go` per backward compat. |

### ✅ Eliminati (nessuna persistenza)

| File | Cosa era | Motivo |
|---|---|---|
| `feed_cache.json` | Cache feed video (10h TTL) | Ora solo in-memory (`FeedCache` struct senza file). |
| `upload_history.json` | Storico upload YouTube | Non più usato dalla codebase. |
| `analytics_realtime_cache.json` | Cache analytics realtime | Non più scritto da `DataRetentionCleanup`. |

### 📁 Mantenuti come file (non runtime)

| File | Motivo |
|---|---|
| `manifest_v2.json` | Manifest delle build worker — generato, non runtime. |
| `BUILD_INFO.json` | Info di build — configurazione, non dati. |
| Token OAuth YouTube (`account_*.json`) | Secret storage — non devono stare nel DB applicativo. |
| Playbook Ansible (`*.yml`) | Script di deploy — devono restare file. |

---

## Architettura per dominio

### 1. Worker

**Prima (JSON):** `workers.json` era la fonte principale, SQLite era un dual-write "best effort".

**Dopo (SQLite):**
- `NewWithPersistence` rimosso. Unico costruttore: `New(rdb, useRedis, dbStore)`.
- `ReplaceWorkers()` (DELETE + re-INSERT) rimosso. Usa `UpsertWorker()` (UPSERT individuale).
- `RevokeWorker()` usa `SetWorkerRevoked()`.
- `Save()` è diventato no-op — ogni mutazione scrive direttamente in SQLite.

**Interfacce:**
```go
// internal/store/workers_repo.go
type WorkersRepository interface {
    ListWorkers() ([]map[string]any, error)
    GetWorker(workerID string) (map[string]any, error)
    UpsertWorker(raw []byte) error
    DeleteWorker(workerID string) error
    SetRevoked(workerID string, revoked bool) error
    GetRevoked() ([]string, error)
}
```

### 2. YouTube — Modello Canonico

**Prima (4 tabelle + 3 JSON):**
- `youtube_channel_metadata` — Service-level channel metadata
- `youtube_groups` (old) — Service-level upload groups (channels serializzati come JSON)
- `youtube_manager_channels` — Storage-level channels (da ChannelsSaved.json)
- `youtube_manager_groups` — Storage-level groups (da ChannelsSaved.json)
- Due istanze indipendenti di `youtube.Service` (in `module.go` e in `NewYouTubeHandlers`)

**Dopo (3 tabelle + 1 Service):**
- `youtube_channels` — Singola fonte di verità per metadati canale
- `youtube_groups_v2` — Gruppi con `UNIQUE(name, group_type)` per differenziare upload/manager
- `youtube_group_channels` — Membership many-to-many con foreign keys e CASCADE DELETE
- `youtube_tracked_niches` — Niche keywords tracciate
- Una sola istanza di `youtube.Service` passata da `module.go` a `NewYouTubeHandlers`

### 3. Ansible

**Prima (2 JSON):**
- `ansible_computers.json` — Inventario (con SSHPassword in chiaro)
- `ansible_runs.json` — Cronologia esecuzioni

**Dopo (3 tabelle):**
- `ansible_hosts` — Inventario strutturato con `secret_ref` (no SSHPassword)
- `ansible_runs` — Esecuzioni con colonne strutturate
- `ansible_run_hosts` — Relazione many-to-many run↔host con CASCADE DELETE

**SSHPassword:** Migrato in file segreti `secrets/ansible/ssh_host_*` (0600) durante l'import. `secret_ref` referenzia il file. Nessuna password in chiaro nel database.

### 4. Cache

**Prima:** `youtube_api_cache.json` (file), `feed_cache.json` (file), `analytics_cache.json` (file)

**Dopo:**
- `youtube_api_cache` (SQLite) — Cache API YouTube con TTL, Get con fallback SQLite
- `FeedCache` — Solo in-memory, nessuna persistenza su file
- `analytics_cache` (SQLite) — Cache analytics

### 5. Legacy Imports Tracking

La tabella `legacy_imports` traccia ogni operazione di import JSON→SQLite:

```sql
CREATE TABLE legacy_imports (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name      TEXT NOT NULL,
    source_path      TEXT NOT NULL,
    source_sha256    TEXT NOT NULL,
    importer_version INTEGER NOT NULL DEFAULT 1,
    status           TEXT NOT NULL DEFAULT 'applied',
    imported_rows    INTEGER NOT NULL DEFAULT 0,
    rejected_rows    INTEGER NOT NULL DEFAULT 0,
    conflict_rows    INTEGER NOT NULL DEFAULT 0,
    report_path      TEXT,
    error_message    TEXT,
    imported_at      TEXT NOT NULL,
    UNIQUE(source_name, source_sha256, importer_version)
);
```

Questo rende ogni importazione idempotente, verificabile e auditabile.

---

## Two-step legacy cleanup: migration 008 (data copy) + 009 (DROP)

Per garantire un upgrade **production-safe**, la rimozione delle tabelle legacy è stata suddivisa in due migrazioni distinte:

### Migration 008 — Data copy (SAFE, rollbackabile)

`008_drop_legacy_tables.sql` esegue solo INSERT OR IGNORE dalle tabelle legacy in quelle canoniche. **Non droppa nulla.**

| Fase | Operazione |
|------|-----------|
| 1-4 | INSERT OR IGNORE da `youtube_channel_metadata`, `youtube_groups`, `youtube_manager_channels`, `youtube_manager_groups` → `youtube_channels`, `youtube_groups_v2`, `youtube_group_channels` |
| 5 | INSERT OR IGNORE da `ansible_computers` → `ansible_hosts` |
| CI guard | Crea `legacy_json_registry` con tutti i path JSON legacy da bannare |

**Rollback:** Cancellare i record inseriti dalle tabelle canoniche. Le tabelle legacy sono intatte.

### Migration 009 — DROP (IRREVERSIBILE)

`009_drop_legacy_tables.sql` va applicata **solo dopo aver verificato** che i dati siano stati copiati correttamente. Droppa 5 tabelle legacy:

| Tabella legacy | Sostituita da | Note |
|---|---|---|
| `ansible_computers` | `ansible_hosts` (004) | Dati migrati via 008 Phase 5 |
| `youtube_channel_metadata` | `youtube_channels` (003) | Dati migrati via 008 Phase 1 |
| `youtube_groups` (old) | `youtube_groups_v2` (003) | Canali linkati via json_each in youtube_group_channels — 008 Phase 2 |
| `youtube_manager_channels` | `youtube_channels` + `youtube_group_channels` (003) | Canali e gruppi manager uniti nel modello canonico — 008 Phase 3 |
| `youtube_manager_groups` | `youtube_groups_v2` (003) | Gruppi manager migrati con group_type='manager' — 008 Phase 4 |

**Verifica pre-009:**
```sql
-- Verificare che i conteggi corrispondano
SELECT 'legacy_channels', COUNT(*) FROM youtube_channel_metadata
UNION ALL
SELECT 'canonical_channels', COUNT(*) FROM youtube_channels;

SELECT 'legacy_groups', COUNT(*) FROM youtube_groups
UNION ALL
SELECT 'canonical_groups', COUNT(*) FROM youtube_groups_v2 WHERE group_type='upload';
```

**Nessun codice produttivo:** Tutti i fallback legacy sono stati rimossi dal codice. Le interfacce `YouTubeStore` e `AnsibleComputerStore` espongono solo metodi canonici. I test `TestMigration008_UpgradeEndToEnd` verificano zero perdita dati in entrambi gli step (008 preserva le tabelle legacy, 009 le droppa).

---

## CI Guard

Il file `internal/audit/data_layer.go` implementa un audit che:

1. **Blocca la reintroduzione** di file JSON legacy: `youtube_manager.json`, `groups.json`, `channels.json`, `ChannelsSaved.json`, `workers.json`, `ansible_runs.json`, `feed_cache.json`, `upload_history.json`, ecc.
2. **Rileva sorgenti duplicate** di dati (es. `workers.json` in root + `workers/workers.json`).
3. **Verifica consistenza** dei path (es. `credentials` vs `Credentials`).

L'audit viene eseguito all'avvio del server e può essere usato in CI tramite `FailOnError()`:

```go
if err := auditor.Audit().FailOnError(); err != nil {
    log.Fatalf("[AUDIT] %v", err)
}
```

---

## Riepilogo delle PR consigliate

| PR | Cosa contiene | Dipende da |
|---|---|---|
| 1 | Migration infrastructure (runner, `001_initial.sql`) | — |
| 2 | Worker source of truth (`002_legacy_imports`, `WorkerRepository`, Registry cleanup) | PR 1 |
| 3 | YouTube canonical model (`003_youtube_canonical.sql`, doppio Service eliminato) | PR 1 |
| 4 | Ansible persistence (`004_ansible.sql`, `ansible_hosts/runs/run_hosts`, `secret_ref`) | PR 1 |
| 5 | Cache + legacy removal (FeedCache in-memory, dual-write rimosso, CI guard, audit) | PR 2, 3, 4 |

---

## Procedura di rollback

Se una migration causa problemi:

1. Fermare il server.
2. Eliminare l'entry in `schema_migrations` per la migration problematica:
   ```sql
   DELETE FROM schema_migrations WHERE version = N;
   ```
3. Eseguire manualmente il reverse SQL.
4. Riavviare il server.

**Nota:** Le migration 003 e 004 sono additive (CREATE TABLE IF NOT EXISTS) e non hanno drop — il rollback è sicuro.

---

## Definition of Done

- [x] Esiste un unico migration runner versionato con `schema_migrations` e checksum.
- [x] `go build ./...` passa.
- [x] `go test ./...` passa.
- [x] Nessun endpoint produttivo legge JSON legacy.
- [x] Nessun endpoint produttivo scrive JSON legacy.
- [x] L'audit blocca la reintroduzione di file JSON legacy.
- [x] I token OAuth YouTube non sono in tabelle normali (restano su file protetto).
- [x] SSHPassword non è in chiaro nel database (solo `secret_ref`).
- [x] Tutte le migrazioni sono idempotenti (CREATE TABLE IF NOT EXISTS, UPSERT).
