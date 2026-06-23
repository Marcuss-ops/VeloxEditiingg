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

## How to import

1. Grafana → Dashboards → Import → Upload JSON file.
2. Select the Velox Prometheus datasource.
3. Save under the `Velox / Scorecard` folder.

## Add a new dashboard

1. Build the panel / variable set in the Grafana UI.
2. Export → "Export for sharing externally" → save the JSON here.
3. Run `scripts/ci/check-compute-outcome-labels.sh` locally.
4. Open a PR.
