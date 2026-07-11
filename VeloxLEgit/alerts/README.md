# Alerts — Prometheus alerting rules for the Project Performance Scorecard

Prometheus alerting rule files for the Velox master's metrics
surface. Every rule file in this directory MUST consume metrics
from the spec §14 single-family shape:

  `velox_compute_seconds_total{outcome=...}`

and the sibling family for failure-reason attribution:

  `velox_compute_failure_reasons_total{reason=...}`

The 4 retired split-family names are FORBIDDEN
(`scripts/ci/check-compute-outcome-labels.sh` enforces this):

  - `velox_compute_seconds_total_failed`
  - `velox_compute_seconds_total_cancelled`
  - `velox_compute_seconds_total_stale`
  - `velox_compute_seconds_total_useful`

## Files

- `spec-14-compute-outcomes.yml` — canonical alert set for compute
  outcome SLO regressions and the failure-reason tail.
- `spec-15-placement-rejections.yml` — placement-rejection alerting
  for the single-label family `velox_placement_rejections_total{reason=...}`
  (rate anomalies, capability drift on `unsupported_executor`,
  fleet saturation on `capacity_full`, single-reason dominance).

## ConflictBudget metrics reference (no alert file yet)

The conflict-budget instrumentation ships as metrics and a Grafana
dashboard (see `dashboards/conflict-budget.json`) but **does not yet
have a dedicated alert rule file**. The expected upcoming
`spec-16-conflict-budget.yml` will cover:

- `velox_conflict_escalations_total` rate > 0 (warning): deadlock signal.
- `velox_conflict_streak_reset_total` rate near zero AND
  `velox_conflict_stayed_under_threshold_total` rate high (warning):
  the budget is accumulating under-threshold conflicts without a
  reset cycle — leading indicator for an upcoming escalation.
- `histogram_quantile(0.95, ...velox_conflict_streak_length_bucket... ) > 2`
  for 30m (info): the runup distribution is climbing toward the
  threshold boundary.

Mint `spec-16-conflict-budget.yml` alongside the metrics spec when
alerting becomes an explicit operator requirement; do not stub the
file just to fill the directory.

## Adding new alert rules

1. Build the new alert in the scoring team's local Prometheus.
2. Validate in `promtool check rules` (bundled with Prometheus).
3. Export → save the YAML here.
4. Run `scripts/ci/check-compute-outcome-labels.sh` locally.
5. Open a PR.

## Loading

Prometheus picks up rules from `--rules-file` glob. In the
Velox production compose file the rule directory is mounted at:

  /etc/prometheus/rules/

so a reload picks up new files without a config push.
