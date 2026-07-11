# Dashboards — Grafana panels for the Project Performance Scorecard

This directory holds the canonical Grafana dashboard JSON for the
Velox master's Prometheus exposition surface. Every dashboard here
MUST consume metrics from the spec §14 single-family shape:

  `velox_compute_seconds_total{outcome=...}`

and the sibling family for failure-reason attribution:

  `velox_compute_failure_reasons_total{reason=...}`

The 4 retired split-family names are FORBIDDEN:

  - `velox_compute_seconds_total_failed`
  - `velox_compute_seconds_total_cancelled`
  - `velox_compute_seconds_total_stale`
  - `velox_compute_seconds_total_useful`

`scripts/ci/check-compute-outcome-labels.sh` fails CI on any of
those substrings appearing in this directory.

## Files

- `compute-outcomes.json` — Grafana panel set for compute outcomes.
- `failure-reasons.json` — Grafana panel for the failure-reason
  sibling family (top-N reasons).
- `placement-rejections.json` — Grafana panel set for placement
  rejections: per-reason rate, total rejections (1h), the
  unsupported_executor capability-drift signal, and a stacked-by-
  reason overview. Reads `velox_placement_rejections_total{reason=...}`.
- `conflict-budget.json` — Grafana panel set for the ConflictBudget
  consecutive-err transition surface (spec §14 Blocco 5):
  escalation rate (deadlock signal), 1h reset count (transition
  density), under-threshold observation rate (CAS noise), and
  streak-length quantiles p50/p95/p99 over the histogram. Reads
  `velox_conflict_escalations_total`,
  `velox_conflict_streak_reset_total`,
  `velox_conflict_stayed_under_threshold_total`, and
  `velox_conflict_streak_length_bucket` (the histogram bucket
  family whose boundary quantiles are computed via `histogram_quantile`).
  Cardinality discipline: NO labels on any of the four families
  (no host, no per-reason dim) — the streak length is captured as
  a histogram observation rather than as a label series.

## How to import

1. Grafana → Dashboards → Import → Upload JSON file.
2. Select the Velox Prometheus datasource.
3. Save under the `Velox / Scorecard` folder.

## Add a new dashboard

1. Build the panel / variable set in the Grafana UI.
2. Export → "Export for sharing externally" → save the JSON here.
3. Run `scripts/ci/check-compute-outcome-labels.sh` locally.
4. Open a PR.
