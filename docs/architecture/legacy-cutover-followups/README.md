# Legacy Cutover Followups

> Tracking dei followup del documento
> [`docs/architecture/LEGACY_SSOT_AUDIT.md`](../LEGACY_SSOT_AUDIT.md)
> al commit `239007117a4319f84626f8b00ac88cc19e953a21`.

Questa cartella raccoglie le **design-doc delle Pull Request** che
chiudono i problemi P0 e P1 identificati dall'audit, **rivedute
dopo l'analisi empirica del codice**.

Le 16 design-doc coprono:
- una PR puramente documentale di **riconciliazione** (PR-11),
- i sei problemi P0 dell'audit (PR-01, -02, -03, -04, -05, -13),
- i sette problemi P1 (PR-06, -07, -08, -14, -15, -16, più PR-04),
- le guard CI consolidate (PR-10, PR-12).

Ogni file è auto-contenuto, elenca scope, file da toccare,
sequenza operativa, criteri di accettazione, test, CI guards introdotti,
rischi e out-of-scope.

> **Importante.** L'audit originale del 22 giugno 2026 conteneva
> alcune inesattezze (claim P0.3 confutata dal codice reale,
> claim P1.3 invertita: `parameters` è canonicale non un mirror,
> claim P1.4 già risolta via typed TaskResult, claim P1.6 già mirata).
>
> La matrice di copertura corretta è registrata in **PR-11** e
> rappresenta il prerequisito documentale per l'apertura delle altre PR.

## Roadmap rivista

| PR | Chiude claim | Titolo | Dipendenze | Stato post-analisi |
|---|---|---|---|---|
| [PR-11](./PR-11-pre-flight-empirical-reconciliation.md) | matrice complessiva | Pre-flight empirical reconciliation | — | ⬜ design pronto |
| [PR-01](./PR-01-fix-artifact-finalization-048.md) | [P0.1](../LEGACY_SSOT_AUDIT.md#p01--finalizzazione-artifact-incompatibile-con-migration-048) | Fix artifact finalization post-048 | PR-11 | ✅ merged (sqlite_finalization_repository.go identity-free jobs CAS + TestArtifactFinalize_Post048SchemaIdempotent) |
| [PR-02](./PR-02-single-succeeded-writer.md) | [P0.2](../LEGACY_SSOT_AUDIT.md#p02--due-writer-di-jobsstatus--succeeded) | Restore single SUCCEEDED writer | PR-11, PR-12 | ⬜ design pronto |
| [PR-12](./PR-12-expand-single-writer-ci-guard.md) | §9.1 + P0.2 | Expand single-writer CI guard (Go-native) | PR-11, parallelo PR-02 | ⬜ design pronto |
| [PR-03](./PR-03-canonical-attempt-identity.md) | [P0.3](../LEGACY_SSOT_AUDIT.md#p03--attempt-id-doppio) | Canonical Attempt identity | PR-11 | ✅ closed (no-op di verifica, doc-only) — vedere PR-11 Appendice A |
| [PR-04](./PR-04-atomic-task-acceptance.md) | [P1.5](../LEGACY_SSOT_AUDIT.md#p15--start-task-e-creazione-attempt-non-atomici) | Atomic task acceptance | PR-11 | ⬜ design pronto |
| [PR-13](./PR-13-verify-job-reaper-post-048.md) | [P0.4](../LEGACY_SSOT_AUDIT.md#p04--lease-task-senza-expiry-persistita) effetti 048 | Verify & fix Job-side reaper post-048 (DEPRECATED by PR-05 follow-up) | PR-11, prima di PR-05 | ✅ merged + deprecated (VELOX_DISABLE_JOB_REAPER is now a no-op; TaskLeaseReaper is the canonical master-side lease enforcer; DisableReaper emits one-time DEPRECATED log) |
| [PR-05](./PR-05-task-lease-expiry-reaper.md) | [P0.4](../LEGACY_SSOT_AUDIT.md#p04--lease-task-senza-expiry-persistita) | Task lease expiry + reaper | PR-04, PR-13 | ✅ merged (migration 049 (column add) + 050 (UPDATE backfill) + LifecycleService.RequeueExpiredLeases + TaskLeaseReaper extracted as separate supervisor runner + RenewLease on taskgraph.Repository) |
| [PR-06](./PR-06-task-report-ingestion-service.md) | [P1.4](../LEGACY_SSOT_AUDIT.md#p14--taskresult-non-ingerito-completamente) | `TaskReportIngestionService` | PR-01..PR-04 | ✅ closed (no-op di verifica, doc-only) — vedere PR-11 Appendice A |
| [PR-07](./PR-07-remove-job-protocol-compat.md) | [P1.1](../LEGACY_SSOT_AUDIT.md#p11--protocollo-job-e-task-attivi-contemporaneamente) | Remove Job protocol compatibility | PR-05 | ⬜ design pronto |
| [PR-08](./PR-08-simplify-jobs-repository.md) | [P1.2](../LEGACY_SSOT_AUDIT.md#p12--repository-job-mantiene-api-runtime) | Simplify `jobs.Writer` (SQLite) | PR-02, PR-05 | ⬜ design pronto |
| [PR-14](./PR-14-postgres-jobs-repository-cleanup.md) | [P1.2](../LEGACY_SSOT_AUDIT.md#p12--repository-job-mantiene-api-runtime) mirror PG | PostgresJobsRepository cleanup | PR-08 | ⬜ design pronto |
| [PR-09](./PR-09-payload-v2-single-shape.md) | [P1.3](../LEGACY_SSOT_AUDIT.md#p13--payload-duplicato-top-level-e-parameters) | Payload V2 single shape | PR-02 | ⚠️ claim invertita — sostituita da PR-15 |
| [PR-15](./PR-15-parameters-canonicalization.md) | [P1.3](../LEGACY_SSOT_AUDIT.md#p13--payload-duplicato-top-level-e-parameters) reale | Parameters canonicalization (V2 envelope) | PR-11; **sostituisce** PR-09 | ⬜ design pronto |
| [PR-16](./PR-16-outbox-sweep-marker.md) | [P1.6](../LEGACY_SSOT_AUDIT.md#p16--drain-outbox-troppo-ampio) marker | Outbox sweep marker & schema_version | PR-11, PR-10 | ⬜ design pronto |
| [PR-10](./PR-10-docs-and-ci-hardening.md) | [P1.7](../LEGACY_SSOT_AUDIT.md#p17--documentazione-non-sincronizzata) + §9 tutti | Documentation + CI hardening | ultima dopo le altre | ⬜ design pronto |

### Dipendenze critiche

```text
PR-11 (reconciliation)
   ├─► PR-01
   ├─► PR-02 + PR-12 (parallelo)
   ├─► PR-03 (collassabile)
   ├─► PR-04
   │      └─► PR-05  (PR-13 entra qui tra PR-04 e PR-05)
   │             └─► PR-07
   ├─► PR-06 (collassabile)
   ├─► PR-08 ─► PR-14
   ├─► PR-15 (sostituisce PR-09)
   ├─► PR-16
   └─► chiusura con PR-10
```

### Legenda stati

- ⬜ design pronto: design doc scritta, PR non ancora aperta.
- ⚠️ claim confutata: l'analisi empirica mostra che la claim
  dell'audit è imprecisa; la PR collassa a **no-op di verifica**
  (apre e chiude subito con un commit di documentazione).
- 🟡 in corso
- ✅ merged

## Convenzioni di queste design-doc

Tutti i file `PR-NN-*.md` seguono lo stesso template:

1. Header — titolo, audit anchor, branching, dipendenze.
2. Contesto — quale P0/P1 risolve e perché, con riferimenti ai
   file reali coinvolti.
3. Scope — cosa entra nella PR (e cosa è esplicitamente escluso).
4. Files to touch — percorsi (`DataServer/...`,
   `RemoteCodex/`, `proto/`, `scripts/ci/`, `docs/`, ecc.)
   previsti. Tutti i percorsi sono derivabili dall'audit E
   dall'analisi empirica.
5. Sequenza operativa — passo-passo, con note su transazioni e
   idempotenza.
6. Acceptance criteria — checklist binaria.
7. Test — unit, integration, race, invariant.
8. CI guards introdotti — quali pattern vengono aggiunti a
   `scripts/ci/` (sotto §9 dell'audit) oppure a `scan_test.go`
   (PR-12).
9. Rischi — blast radius, rollback, monitoring.
10. Out of scope — rinviato a PR successiva, con numero PR-NN.

## Definition of Done

La migrazione completa è quella della §10 dell'audit. Alcune voci
risultano già coperte dal codice attuale (claim collassate dopo
PR-11): le design-doc PR-03 e PR-06 esistono comunque come record
di non-regressione, non come implementazione.

Quando una PR viene aperta, sostituire `⬜ design pronto` con
`🟡 in corso` e successivamente `✅ merged`. Solo dopo che tutti i
PR sono `✅`, la sezione §11 dell'audit può essere aggiornata a
**"cutover completato"** (vedi PR-11 appendice "Matrice di
copertura effettiva").
