# Velox — osservabilità nativa: audit metriche / log / tracing (2026-08-13)

Audit dei tre pilastri dell'osservabilità sui path critici del DataServer,
con i gap misurati (non stimati) e l'ordine di intervento. Integra
`REFACTOR-COMPLEXITY-2026-08-13.md`: qui il metro è "il path critico è
visibile in `/metrics`, nei log strutturati e nei trace?".

## 0. Infrastruttura esistente (sana, da non riscrivere)

| Pilastro | Stato | Dettagli |
|---|---|---|
| **Metriche** | ✅ ricco | `internal/metrics.Collector` con ~50 family Prometheus (`velox_*`) + `OperationalTelemetry` (delivery/DB/cache). Cardinalità disciplinata (niente `job_id`/`task_id` come label). Wire a `GET /metrics` via `registry.Handler()`. |
| **Log strutturati** | ⚠️ presente ma quasi inutilizzato | `internal/logging` con `Event{timestamp, level, code, component, fields}` + output JSON/human + throttling. Solo **3** `NewLogger` e **4** call-site strutturate. |
| **Tracing** | ⚠️ plumbed ma minimale | OTel SDK (no-op/stdout/otlp), W3C TraceContext, `otelgrpc` sul server gRPC, `telemetry.StartSpan`/`SpanFromContext`/`TraceIDFromContext`. Solo **4** span espliciti. |

## 1. GAP 1 — metriche del forwarding runner orfane (P1)

**Stato: ✅ RISOLTO** — `a09e384d` (`feat(forwarding): wire runner metrics onto the Prometheus registry`): family `velox_forwarding_*` registrate su `Registry` e iniettate nel runner via `WithTelemetry`.

Il `CreatorForwardingRunner` — lo stesso `processLease` a complessità 82
dell'audit complessità, cioè un hotspot — possiede un `RunnerMetrics`
(`Claimed/Forwarded/Failed/Retried/QueueDepth/OldestPending`, `atomic.Int64`),
ma:

- `Snapshot()` restituisce `map[string]int64` e **nessun** chiamante di
  produzione consuma `runner.Metrics()` (`grep` su `internal/` + `cmd/` → 0).
- I nomi `forwarding_claimed/forwarded/failed/retried/…` **non sono
  registrati** sul `metrics.Registry` Prometheus.

Conseguenza: il path di forwarding è visibile **solo via `log.Printf`**, non
in `/metrics`. Confronto: `deliveries` ha `Telemetry` (interfaccia) cablata a
`OperationalTelemetry` (`bootstrap_composition.go:205`) e `completion` ha
`ConflictBudget.WithMetricsSink` → `metrics.Collector`. Il forwarding è
l'unico dei tre runner critici senza sink Prometheus.

**Fix:** una `ForwardingTelemetry` con family `velox_forwarding_{claimed,
forwarded,failed,retried,total,queue_depth,oldest_pending}` registrate su
`Registry`, iniettata nel runner come fa `deliveries.WithTelemetry`. Basso
rischio, puro add-on.

## 2. GAP 2 — copertura tracing: 4 span su ~7 path critici (P1)

**Stato: ✅ RISOLTO** — `a751f473` (`feat(observability): open tracing spans on the 5 critical gap-2 paths`): `forward_tick`/`forward_lease`/`deliver_lease`/`complete_upload`/`resolve_forwarding`/`outbox_dispatch`.

Span espliciti oggi:

| Span | File |
|---|---|
| `enqueue`, `schedule_task` | jobs/enqueue/enqueue.go |
| `claim_task` | grpcserver/handler_accept.go |
| `ingest_result` | grpcserver/handler_result.go |

`otelgrpc` fornisce lo span gRPC esterno, ma **nessuna annidamento interno**
per la business logic. Path critici senza span:

- `forwarding` (`tick`, `processLease`, poll remote)
- `deliveries` (`processLease`, `runPublicationPhases`)
- `completion` (`CompleteUpload`, reconcile supervisor)
- `creatorflow.Resolver.Resolve`
- `outbox.Dispatcher.Run`

**Fix:** aprire `StartSpan(ctx, "forward_lease")` / `"deliver_lease"` /
`"complete_upload"` / `"resolve_forwarding"` / `"outbox_dispatch"` con
attributi low-cardinality (provider, error_class, phase). Nessun `job_id`
come nome span (già codificato nella policy del package telemetry).

## 3. GAP 3 — log strutturati quasi assenti (P2)

**Stato: ✅ RISOLTO** — `2f544de6` (`feat(observability): structured logging on forwarding/deliveries/completion runners`): 43 `log.Printf` → `logging.Info/Warn/Error` con codici `FORWARDING_*`/`DELIVERY_*`/`COMPLETION_RECONCILE_*`.

Conteggio reale nei path di produzione (`internal/` + `cmd/`, esclusi test):

| Forma | Call-site |
|---|---|
| `log.Printf/Println/Print` non strutturati | **565** |
| `logging.Info/Warn/Error/…` strutturati | **4** |

La convenzione `[PREFIX] key=value` (es. `[FORWARDING] forwarded
forwarding=%s → job=%s`) è semi-strutturata ma **non**:
1. query-abile come JSON in produzione (i campi vivono dentro `Message`);
2. classificata con `code`/`component` (la tassonomia di `logging/codes.go`);
3. correlata al trace.

Top package non strutturati: `grpcserver` (141), `cmd/server` (114),
`fleet` (27), `artifacts` (24), `deliveries` (20).

**Fix:** adozione incrementale sui **nuovi** punti di log + i path critici;
non una migrazione bulk dei 565. I runner (`forwarding`, `deliveries`,
`completion`) prendono un `*logging.Logger(component)` e loggano
`logger.Info(code, fields)` con i campi già presenti nelle stringhe.

## 4. GAP 4 — nessuna correlazione trace ↔ log (P2)

**Stato: ✅ RISOLTO** — `c1249bba` (`feat(observability): inject trace_id/span_id into structured logs (GAP 4)`): varianti `*Context` di `logging` + threading `ctx` nei runner.

`telemetry.TraceIDFromContext`/`SpanIDFromContext` esistono ma **non sono
usati** (`grep` → 0 call-site; l'unico `TraceID` presente è il campo dati
del modello `taskattempts`/`audittrail`, non l'iniezione nei log).

Conseguenza: i 4 span e i 565 log non sono collegabili — impossibile
ricostruire il percorso di un singolo `forwarding_id` attraverso i servizi.

**Fix:** in `logging.log`, quando è disponibile, iniettare `trace_id`/`span_id`
negli `Event.Fields` (o campi dedicati) leggendoli da `context.Context`; i
call-site critici passano già `ctx`.

## 5. Stato di chiusura (2026-08-13)

Tutti i gap dell'audit sono chiusi su `main`, nell'ordine: GAP 1 → GAP 2 → GAP 3 → GAP 4.

| Gap | Commit | Cosa |
|---|---|---|
| GAP 1 (P1) | `a09e384d` | metriche forwarding `velox_forwarding_*` sul `Registry` + iniezione nel runner |
| GAP 2 (P1) | `a751f473` | span `forward_tick`/`forward_lease`/`deliver_lease`/`complete_upload`/`resolve_forwarding`/`outbox_dispatch` |
| GAP 3 (P2) | `2f544de6` | `log.Printf` → `logging.Info/Warn/Error` sui 3 runner critici |
| GAP 4 (P2) | `c1249bba` | iniezione `trace_id`/`span_id` nei log via varianti `*Context` |

## Vincoli operativi

- Cardinalità: niente `job_id`/`task_id`/`attempt_id`/`hash` come label o nome
  span (già policy in `metrics.safeLabelKey` e nel doc di `telemetry`).
- Ogni blocco = commit atomico + push `main`.
- Nessun nuovo stato globale mutabile: i sink vanno iniettati via costruttore
  (allinea il lavoro precedente su `globalMetrics`/logging `atomic.Pointer`).
