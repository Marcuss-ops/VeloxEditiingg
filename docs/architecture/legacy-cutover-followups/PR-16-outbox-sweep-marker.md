# PR-16 — Outbox sweep marker & schema-version tagging

> **Audit anchor:** [§P1.6](../LEGACY_SSOT_AUDIT.md#p16--drain-outbox-troppo-ampio) — il drain È mirato, ma manca il marker di cutover completato e il filtro per `schema_version`.
> **Target milestone:** post-cutover P1.
> **Branch:** `cutover/pr-16-outbox-sweep-marker`
> **Dipendenze:** PR-11; PR-10 benefica.

## Contesto

L'audit P1.6 rivendica che il drain outbox è "troppo ampio a ogni
boot" e che servirebbe un filtro per `schema_version` o un marker.

L'analisi empirica mostra che il drain NON è generico:
`DataServer/cmd/server/bootstrap_assets.go:103` chiama
`p.Outbox.DrainLegacyEvents(ctx, legacyTypes)` con una `legacyTypes`
lista specifica (cfr. `DataServer/internal/outbox/store.go:334`).

Ciononostante, il pattern attuale ha due lati deboli:

1. il drain viene invocato **a ogni boot** finché qualcuno non
   rimuove la chiamata — non c'è un marker persistente che ne
   dichiari la conclusione,
2. la lista `legacyTypes` deve essere mantenuta manualmente in due
   punti (`bootstrap_assets.go` e altrove). Un filtro per
   `schema_version` renderebbe il sistema self-describing.

PR-16 chiude entrambi senza riscrivere la logica di drain.

## Scope

- Aggiungere una colonna `schema_version INTEGER NOT NULL DEFAULT 1`
  alla tabella `outbox_events` (migration `049_outbox_schema_version.sql`
  o successiva).
- Aggiungere una tabella di config
  `app_config(key TEXT PRIMARY KEY, value TEXT)` con bootstrap
  marker: `legacy_outbox_cutover_completed BOOLEAN`.
- Modificare `DrainLegacyEvents` per:
  - accettare un filtro opzionale `legacySchemaVersions []int`,
  - marcare automaticamente `legacy_outbox_cutover_completed=true`
    quando il drain ritorna 0 record dopo N boot consecutivi (o
    dopo che `bootstrap_assets.go` lo richiede esplicitamente
    la prima volta).
- Modificare `bootstrap_assets.go`:
  - leggere il marker,
  - se marker presente → SKIP del drain con log
    `legacy_outbox_drain_skipped`,
  - se marker assente → drain + scrittura marker.

## Files to touch

```text
DataServer/internal/store/migrations/sqlite/049_outbox_schema_version.sql
DataServer/internal/store/migrations/sqlite/050_app_config.sql
DataServer/internal/outbox/store.go                       # DrainLegacyEvents + marker
DataServer/internal/outbox/marker.go (nuovo)
DataServer/cmd/server/bootstrap_assets.go                 # gate + write marker
DataServer/internal/app/app_config*.go (se esiste)
DataServer/internal/outbox/outbox_test.go                  # marker + schema_version
```

## Sequenza operativa

```text
1. Migration 049: ALTER TABLE outbox_events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1.
2. Migration 050: CREATE TABLE IF NOT EXISTS app_config (key TEXT PRIMARY KEY, value TEXT).
3. In outbox/store.go:
   - Aggiungere `LegacyOutboxSchemaVersion int = 1` come costante.
   - DrainLegacyEvents legge schema_version e applica:
       WHERE status='PENDING' AND schema_version IN (?).
4. In outbox/marker.go (nuovo):
   - MarkerStore.Set(key,value) / Get(key)
   - Set solo se transazione atomica con drain.
5. In bootstrap_assets.go:
   - if MarkerExists("legacy_outbox_cutover_completed") { skip + log }
   - else { drain(); Set("legacy_outbox_cutover_completed","true") }
6. Test integration:
   - Primo boot con legacy events → drain avviene + marker scritto.
   - Secondo boot → drain SKIP + log "skipped".
   - Inserimento di un nuovo evento schema_version=2 → non viene drainato
     anche in assenza di marker.
```

## Acceptance criteria

- [ ] Migration 049/050 applicabili senza errori su DB esistente.
- [ ] Dopo 1 boot con legacy events, marker `legacy_outbox_cutover_completed`
      è `true` in `app_config`.
- [ ] Secondo boot: nessuna query di drain outbox eseguita su
      eventi schema_version=1.
- [ ] Eventi schema_version=2 NON sono mai toccati dal drain anche
      in assenza di marker.
- [ ] golden E2E verde: nessun errore di regressione.

## Test

- **Unit:**
  - `outbox_marker_test.go`: Set/Get round-trip.
  - `outbox/store_test.go`: `DrainLegacyEvents` con filtro schema_version.
- **Integration:** golden E2E che includa un boot con legacy events
  seguiti da un nuovo boot pulito.
- **Regression:** golden E2E workload normale rimane invariato.

## CI guards introdotti

In `check-migrations.sh`:

```bash
# Dopo aver applicato fino al 049:
# - outbox_events.schema_version esiste
# - app_config esiste
```

In `check-architecture.sh`:

```bash
# Vietata la mancanza della lettura del marker legacy_outbox_cutover_completed
# in qualunque punto di bootstrap che invochi DrainLegacyEvents.
```

## Rischi

- DB esistenti senza la colonna `schema_version` devono essere
  portati correttamente a default `1` (era il comportamento
  storico retrocompatibile).
- Concurrent boot di due pod: solo uno deve impostare il marker.
  Risolvere con `INSERT OR IGNORE` in SQLite o `ON CONFLICT DO NOTHING`
  in PG, atomico.

## Out of scope

- Modificare la semantica del drain stesso: PR-16 non cambia il
  comportamento di drain, lo rende idempotente.
- Pulizia completa di `app_config` adozione (potrebbe essere
  usato anche da altri componenti futuri).
