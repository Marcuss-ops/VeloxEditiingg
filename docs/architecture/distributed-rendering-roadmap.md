# Distributed rendering roadmap — Piano di scalatura a DAG distribuito

**Capitolo del perimetro architetturale Velox** — corrisponde alla sezione **P2** (§33) del piano di intervento nel documento indice [`CURRENT-TO-TARGET-ARCHITECTURE.md`](./CURRENT-TO-TARGET-ARCHITECTURE.md).  
**Stato:** roadmap. La fase P2 parte **dopo** che le fasi P0 (runtime/CI) sono verdi su `main`.

> Le specifiche del **Multi-Task DAG** (§18) e dello **Scheduler target** (§19) a cui questa roadmap si riferisce sono in [`target-architecture.md`](./target-architecture.md).

---

## 33. P2 — RenderPlan, DAG e scala

Questa fase parte dopo i P0 runtime/CI.

### Pre-condizioni

P2 non può partire finché:

- P0-01 (baseline obbligatoria) è verde;
- P0-02 (false-success forwarding) è chiuso;
- P0-03 (supervisor/readiness) è verde;
- P0-04 (progress/conflict) è verde;
- P0-05 (finalizzazione blindata) è verde;
- P0-06 (workload E2E reale) è verde;
- P1-03 (suite recovery) è verde.

### Ordine di implementazione

1. RenderPlan schema;
2. compiler registry;
3. persistenza plan;
4. multi-Task DAG;
5. executor granulari;
6. intermediate artifact contract;
7. cache key deterministica;
8. late composition;
9. locality scoring;
10. temporal sharding;
11. benchmark CPU;
12. soak distribuito.

### Vincoli

Non implementare sharding prima di:

- artifact intermedi deterministici;
- frame/timebase contract;
- concat/mux validation;
- retry per shard;
- confronto sharded/non-sharded.

### Accettazione di fase

La fase P2 è considerata completata quando, oltre ai vincoli di cui sopra:

- un Job video si compila in un DAG di almeno 8 Task granulari con dipendenze reali;
- il placement utilizza locality, slot e bandwidth come descritto in [`target-architecture.md §19`](./target-architecture.md#19-scheduler-target);
- gli artifact intermedi sono registrati, verificati e riusabili via cache deterministica;
- un benchmark CPU mostra speedup > 1.5x su un worker multi-slot rispetto al percorso monolitico;
- un soak distribuito di 24h su staging non perde Job né genera artifact orfani.
