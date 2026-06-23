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
