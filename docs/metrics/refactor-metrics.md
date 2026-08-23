# Reproducible Refactoring Metrics

The canonical analyzer is [`scripts/metrics/refactor_metrics.py`](../../scripts/metrics/refactor_metrics.py). It has no third-party Python dependencies and compares two Git refs into one versioned JSON schema plus a Markdown summary.

## Run locally

```bash
BASE_REF=main make refactor-metrics
```

Override the output directory or head ref when needed:

```bash
BASE_REF=main HEAD_REF=HEAD METRICS_OUT=/tmp/velox-metrics make refactor-metrics
```

The output files are `refactor-metrics.json` and `refactor-metrics.md`.

## CI behavior

`.github/workflows/refactor-metrics.yml` runs on pull requests, pushes to `main`, and manual dispatch. The base ref is selected as follows:

- pull request: `github.event.pull_request.base.sha`;
- push to `main`: `github.event.before`;
- initial/manual run without a usable parent: the first parent or root commit.

CI uploads the JSON, Markdown, and text summary as `refactor-metrics-<sha>` for 90 days and writes the Markdown report to the job summary. The workflow is reporting-only; it does not fail a refactor because a metric increased. Threshold policy belongs in the existing LOC gate or a later explicit policy change.

## Metric definitions

- **LOC:** physical lines, with blank, comment, and code lines reported separately. Source files use the deterministic extension allowlist in the script. Generated protobuf files and build/vendor/cache directories are excluded.
- **Complexity:** `1 +` decision tokens. For Go/C/C++/Python this counts `if`, `for`, `case`, `catch`, `switch`, `&&`, `||`, and `?`; shell additionally counts `elif` and `until`. Comments and string contents are removed before counting. This is a stable comparative proxy, not a replacement for a language-specific cyclomatic analyzer.
- **Duplication:** normalized six-line non-empty code windows. Whitespace and numeric literals are normalized, comments/strings are removed, and repeated windows are counted after their first occurrence. The report includes a deterministic sample of duplicate hashes and locations.
- **Responsibility:** stable category matches against the repository-relative path and the first 100,000 source bytes. Categories include artifact, configuration, conversion, media, orchestration, persistence, security, telemetry, transport, validation, and testing. A file with at least three non-testing categories is reported as mixed responsibility.

Every JSON report includes `schema_version`, `metric_definition`, `base_ref`, `head_ref`, full base/head summaries, and file-level deltas. Reports from different refactoring commits are therefore comparable without relying on the local state of external tools.

## Interpretation

Use total deltas for repository-wide movement and `top_loc_reductions`/`top_complexity_reductions` to confirm that a split reduced the coordinator rather than only moving lines. Use `mixed_responsibility_files`, responsibility counts, and duplicate samples to identify whether a refactor produced focused modules or merely redistributed responsibilities and repeated blocks.
