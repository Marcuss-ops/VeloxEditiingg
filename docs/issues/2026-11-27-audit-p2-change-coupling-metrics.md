# P2 — Measure complexity, churn, and change coupling for refactor priorities

**Status:** Backlog
**Area:** Repository-wide, starting with audit hotspots
**Source:** Preliminary audit noted that priorities were not ranked using
historical change frequency.

## Problem

The audit identifies large packages and complex files but does not establish
which ones create the most maintenance cost. The repository already has
`scripts/metrics/refactor_metrics.py` for reproducible LOC, complexity proxy,
duplication, responsibility, and ref comparison. The missing evidence is
primarily historical churn and change coupling, not another static-analysis
framework.

## Work plan

1. Read `docs/metrics/refactor-metrics.md` and use the existing analyzer with
   an explicit baseline/head ref. Save or summarize its schema-versioned
   output.
2. Choose and document a representative history window that excludes
   generated/vendor/build artifacts and does not combine unrelated periods.
3. Measure per-file change frequency and co-change pairs for the listed
   hotspots: completion/publication/delivery flow, `internal/store`, worker,
   pipeline, metrics/telemetry, grpcserver, fleet, API, shared contract, and
   server bootstrap.
4. Distinguish mechanical co-change (rename, generated update, broad format)
   from semantic coupling by sampling commit diffs and commit messages.
5. Rank candidate work by confirmed semantic risk, churn, coupling, and
   estimated change size. Keep the metric proxy and its limitations visible.
6. Produce a short reproducible report or update the existing metrics docs;
   do not add a new dashboard or scheduled job unless a separate need is shown.

## Acceptance criteria

- Baseline/head refs, date window, file filters, and commands are recorded.
- Existing metrics output is interpreted according to its documented
  definitions; approximate complexity is not described as cyclomatic truth.
- Change-coupling candidates include sampled commit evidence and exclude
  obvious mechanical noise.
- Refactor priority is updated only where measurements change the decision;
  raw counts alone do not trigger code movement.
- The resulting report is concise and repeatable using repository tooling.

## Verification

Run the documented `make refactor-metrics` workflow with explicit refs and
inspect the generated JSON/Markdown. Verify historical scripts against a small
sample of commits before relying on conclusions. No production tests are
needed for metrics-only work.
