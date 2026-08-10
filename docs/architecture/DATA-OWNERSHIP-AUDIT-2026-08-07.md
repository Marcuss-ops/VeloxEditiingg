# Data ownership audit — InstaEdit source of truth / Velox editor-only

**Data:** 2026-08-07
**Azione:** Piano disaccoppiamento InstaEdit ↔ Velox, Azione 1
**Esito:** ✅ InstaEdit DB è l'unica source of truth per utenti, workspace,
gruppi, canali e video. Velox DB non possiede copie operative di gruppi o
canali. Una copia di drop non applicabile (postgres) è stata riparata.

---

## 1. InstaEdit DB = source of truth (verificato)

`InstaeditLogin/internal/database/migrations/` — schema Postgres canonico:

| Dominio | Tabella | Migrazione |
| --- | --- | --- |
| Utenti | `users` | `001_init.sql` |
| Account di piattaforma | `platform_accounts` | `001_init.sql` |
| Workspace | `workspaces`, `workspace_members` | `003`, `028` |
| Gruppi | `groups`, `group_accounts` | `041_groups.sql` |
| Canali per workspace | `workspace_channels` | `044_workspace_channels.sql` |
| Video / contenuti | `posts`, `post_targets`, `upload_jobs` | `003`, `036` |
| Sessioni editor video | `youtube_video_edits` (velox_project_id) | `065` |
| Progetti editor | `thumbnail_projects` + revisions/assets/exports | `094` |
| Mapping project↔project | `velox_project_bridges` | `112`, `114` |

- Nessuna tabella `velox_*` speculare oltre `velox_project_bridges`
  (mapping `InstaEdit project_id ↔ velox_project_id`, unico collegamento).
- Nessun `SyncGroupsToVelox`, `SyncChannelsFromVelox`,
  `MirrorGroupMemberships` presente nel codice (verificato con ricerca
  globale; compare solo come vincolo negativo in
  `docs/VELOX-FRONTEND-GROUPS-BOUNDARY.md`).

## 2. Velox DB = solo dominio editor/render (verificato)

`VeloxEditiingg/DataServer/internal/store/migrations/sqlite/` — la chain
cumulativa termina senza alcun dominio Groups/Channels:

| Dominio rimosso | Migrazione di drop | Stato |
| --- | --- | --- |
| YouTube legacy (metadata, manager channels/groups) | `008`/`009` | ✅ |
| YouTube canonico (`youtube_channels`, `youtube_groups`, `youtube_group_channels`, `youtube_oauth_tokens`, metriche, cache, niches) | `090_drop_youtube_domain.sql` | ✅ |
| Colonne `youtube_group` / `youtube_links_json` su `calendar_events` | `090` | ✅ |
| Dark Editor (`dark_editor_projects`, `folders`, `assets`, `templates`, `generations`, `temp_files`) | `128_drop_dark_editor_domain.sql` | ✅ |

Le tabelle che restano in Velox (jobs, taskgraph, task_attempts, artifacts,
deliveries, workers, outbox, drive_links, instaedit_delivery_events, …)
sono esclusivamente stato editor/render/execution — nessun catalogo
utenti/gruppi/canali.

**Verifica automatica:** `migrations_schema_test.go`
(`TestMigration090_YouTubeDomainDropped`,
`TestMigration128_DarkEditorDomainDropped`,
`TestMigration128_UpgradeFromPreDropState`) applica l'intera chain e
pinna l'assenza di ogni tabella/colonna del dominio. Suite verde.

## 3. Difetto trovato e riparato — drop YouTube postgres irraggiungibile

`VeloxEditiingg/DataServer/internal/store/migrations/postgres/` conteneva
**due file con versione 010**:

```
010_drive.sql                   (versione 10)
010_drop_youtube_domain.sql     (versione 10)  ← collisione
```

`discoverMigrations` (runner) **fallisce chiuso** su versioni duplicate
(`duplicate migration version 010`). Conseguenza: la chain postgres non
poteva mai essere scoperta → la migrazione che elimina gruppi/canali
YouTube dal DB postgres era **irraggiungibile**.

**Fix:** rinomina `010_drop_youtube_domain.sql` → `023_drop_youtube_domain.sql`
(versione libera, dopo `022_media_probe_jobs.sql`). La chain postgres è test-only oggi: `Config.Validate()` rifiuta
`VELOX_DB_DRIVER=postgres` prima di `database.Open`, mentre il runtime master
resta SQLite-only. Nessun `schema_migrations` applicato registrava la versione
10 con questo contenuto: il rinomino è sicuro e non tocca checksum applicati.

**Regression test aggiunto:**
`TestDiscoverPostgresMigrations_NoDuplicateVersions`
(`migrations_discovery_test.go`) — scopre la chain postgres reale e pinna:
- nessuna versione duplicata;
- `drop_youtube_domain` presente e ordinata dopo `009_youtube`.

## 4. Riferimenti aggiornati

- `DataServer/internal/store/migrations/README.md` (layout drop postgres)
- `docs/SOCIAL_API_MIGRATION_RUNBOOK.md`
- `deploy/scripts/audit-no-youtube-residuals.sh` (commento)
- header del file `023_drop_youtube_domain.sql`

## 5. Conclusioni per la DoD

- ✅ InstaEdit DB = unica source of truth per utenti, workspace, gruppi,
  canali, video.
- ✅ Velox DB non possiede gruppi/canali operativi (SQLite drop applicati;
  postgres drop ora raggiungibile).
- ✅ Mapping solo `InstaEditProject ↔ VeloxProject` (`velox_project_bridges`
  + `youtube_video_edits.velox_project_id`).
- ✅ Nessun sync bidirezionale, nessun DB condiviso.
- ⏭ Da fare nelle azioni successive: frontend Velox (rimozione pagine
  Groups/Channels — repo `VeloxFrontend`), letture InstaEdit senza
  `VELOX_CONTROL_URL`, Test A/B/C/D.
