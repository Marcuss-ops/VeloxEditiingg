# Velox Changelog Archive — 2026-07-27 and Earlier, Part 3

> Continuation of [part 2](CHANGELOG-2026-07-27-and-earlier-part2.md).

### PR-15.16 — no-youtube-regression CI guard workflow

A dedicated GitHub Actions workflow now forbids re-introduction of any
direct Velox-side YouTube integration after the YouTube → Social API
closure (PR-15.8 / PR-15.9 / PR-15.10 / PR-15.11 / PR-15.12 / PR-15.13
/ PR-15.14 + Residuo 2 / 3 / 4 chain). Migrations 090 / 091 / 092 / 093
+ the typed model + validator + runner + socialclient + provider
layers already CLOSED the domain runtime; this workflow exists to keep
it closed at CI time.

**Added**:

- `.github/workflows/no-youtube-regression.yml` (commit
  `59a91f7 ci(workflow): add no-youtube-regression guard`). Single
  job `audit` (`YouTube regression guard` step) runs on
  `ubuntu-latest`, `timeout-minutes: 5`,
  `permissions: contents: read` (least-privilege). Concurrency
  group `no-youtube-regression-${ref}` cancels in-progress for
  `pull_request` events so successive PR updates do not pile up.
  `actions/checkout@v4` is invoked with `fetch-depth: 0` so the
  full history is searchable — future maintainers can `git blame`
  any match the audit surfaces.

**Triggers**:

- `push` to `main` — immediate fail-fast on regression re-introduction.
- `pull_request` to `main` — pre-merge gating. NO `paths-ignore`
  (every PR runs the audit; even a doc-only edit that introduces a
  YouTube pattern cannot slip through silently).
- `schedule: cron`: `'0 6 * * 1'` — weekly Monday 06:00 UTC drift
  detector (catches newly-disclosed YouTube patterns in PRs that
  somehow bypass direct CI).
- `workflow_dispatch` — manual re-run.

**Validator runner (single regex + 10 carve-out categories)**:

The runner script computes `git grep -nE "$REGEX"` over the full
history and pipes the results through a 12-line pathspec exclusion
set (10 distinct carve-out categories). On any non-empty match the
script prints the disjunction caught + per-disjunct remediation
hints and `exit 1`. On clean it prints
`✅ No YouTube regression found — clean.`

The single regex (verbatim from the workflow's `REGEX` env var):

```text
google\.golang\.org/api/youtube|youtubeanalytics|VELOX_YOUTUBE|youtube_oauth|internal/integrations/youtube|handlers/server/youtube|providers\.NewYouTubeProvider
```

Covers the 7 forbidden disjuncts (direct Go imports, legacy env var
prefix, OAuth subdomain, legacy integration / handler directories,
legacy provider constructor).

**Pathspec carve-outs (10 categories, 12 pathspec lines)**:

Each exclusion is documented inline in the workflow header with
its rationale. The full set:

1. `.github/workflows/no-youtube-regression.yml`
   **SELF-EXCLUSION** — the workflow file's header enumerates the
   forbidden disjuncts verbatim in the `REGEX` env var + the
   per-disjunct `Hints` comment block. Without this exclusion the
   audit would self-trip on the very file that defines it.

2. `**/migrations/**`
   Forward-only SQL migrations carry residual YouTube references
   under the
   `003_youtube_*.sql / 011_youtube_oauth_tokens.sql / 012_youtube_groups_rename.sql`
   chain. Editing them would violate the **forward-only invariant**
   documented in `DataServer/internal/store/migrations/README.md`
   (which is the same precedent as `001_initial.sql` from Residuo 1).

3. `**/testdata/**`
   Byte-mirror fixtures + snapshot data referenced by the migration
   runner's test suite (`applyMigration` reads from `testdata/*.sql`
   to satisfy idempotency repros).

4. `**/*_test.go`
   Go test files legitimately assert YouTube as a FORBIDDEN
   contract surface (e.g.
   `delivery_destination_opaque_test.go` pins "no youtube-prefixed
   field in `Destination` struct"; the socialclient wire-shape
   tests pin "legacy keys never present" with `youtube` constantly
   neighbouring the assertions). The audit MUST NOT trip on
   negative-pinning tests.

5. `**/*.example`
   Operator-facing templates that intentionally warn against
   re-introduction (Ansible vault, env templates, secrets examples).
   The `deploy/group_vars/vault.yml.example` file is the canonical
   case — it documents the historical `vault_velox_youtube_*`
   secrets as RETIRED.

6. `**/*.md`
   Nested documentation across `docs/**` and any future
   subdirectories cites removed artefacts as historical
   context ("`internal/integrations/youtube`";
   "`providers.NewYouTubeProvider`"). Audit MUST NOT trip on
   documented history.

7. `CHANGELOG.md`
   Root-level historical change record. The `**/*.md` pathspec
   matches `path/file.md` but does NOT match `file.md` at repo root
   (git pathspec semantics: at least one path component required).
   Added explicitly to cover the root-level case.

8. `**/socialcontract/**` + `**/social_contract/**`
   Forward-looking carve-outs for `social_repo` boundary tests
   that may pin YouTube as FORBIDDEN contract markers. Zero-cost
   on current `main` (no matching directories yet); reserved for
   the integration suite landing under
   `DataServer/internal/integration_test/`.

10. `DataServer/internal/jobs/enqueue/delivery_plan_validator.go`
    + `DataServer/internal/store/delivery_plan_payload.go`
    Both files carry a NOTE block documenting the canonical
    YouTube → Delivery rename intent (`YouTubeGroup` →
    `DestinationGroupID`, `YouTubeChannelID` →
    `ExternalDestinationID`, `YouTubeVideoID` → `RemoteMediaID`,
    `YouTubeURL` → `RemoteURL`, `YouTubeStatus` → `DeliveryStatus`).
    The literal `youtube_oauth_tokens` is cited as a DROPPED
    legacy table ("Velox no longer SELECTs `youtube_channels`,
    `youtube_oauth_tokens`, or `youtube_groups`"). The carve-out
    is intentional — the NOTE is documentation for future
    contributors.
    **When (and only when) a future contributor removes those NOTE
    blocks, they MUST also delete the corresponding carve-out
    exclusions inline below** — leaving either an unused carve-out
    or the NOTE alone both regress this audit's surface-area
    contract.

**Operator-facing: what to do if the workflow fails on a PR**:

The workflow's `exit 1` path prints the matched line(s) AND the
per-disjunct remediation hints inline (verbatim in the runner step).
Operator playbook for the 7 forbidden disjuncts:

- **`google.golang.org/api/youtube` or `youtubeanalytics`** (direct
  Go imports) — run `cd DataServer && go mod tidy` and remove the
  dependency. If a reintroduction is genuinely needed for a Social
  API call, route through `DataServer/internal/socialclient/`
  instead — never import the upstream SDK directly.

- **`VELOX_YOUTUBE`** (legacy env var prefix) — migrate to canonical
  `SOCIAL_API_*` names per PR-15.10 closure contract. See
  `deploy/velox-server.env.example` +
  `deploy/group_vars/vault.yml.example` for the canonical mapping.
  The legacy `SOCIAL_GATEWAY_*` aliases are also retired alongside.

- **`youtube_oauth`** (OAuth subdomain) — the OAuth closures live
  in PR-15.8 + PR-15.9. Reintroduction requires a NEW explicit PR
  with rationale and **does NOT** silently merge: the workflow's
  purpose is to make such reintroduction a deliberate architectural
  decision, not an accidental copy-paste.

- **`internal/integrations/youtube`** or **`handlers/server/youtube`**
  (legacy directories) — closures live in PR-15.8. The migration
  is delegated to the external Social API repo. A community
  contributor who wants to revive the YouTube path must do so in a
  NEW repo, out of scope for Velox.

- **`providers.NewYouTubeProvider`** (legacy provider constructor) —
  use `social_gateway` (the thin adapter wrapping
  `socialclient.Client`). The provider registry is keyed on
  canonical names; registered providers live in
  `DataServer/internal/deliveries/providers/`.

**General operator checklist for any workflow failure**:

1. **Localize the offending line** with `git grep -nE "<REGEX>" --`
   against the local working tree, applying the same 12 pathspec
   carve-outs as the runner. Confirm the line is in ACTIVE code
   (NOT in CHANGELOG / docs / migrations / test fixtures).

2. **If the line is in active code**: pick the canonical replacement
   per the 7-disjunct playbook above. **DO NOT** add an inline
   `':!...'` carve-out to silence the audit — that's a regression-
   guard smoking-gun and must not be merged without an explicit PR
   justifying the carve-out and auditing the new exclusion with
   the same scrutiny as the original 10.

3. **If the line is correctly in an excluded path**: the carve-out
   list may need follow-up expansion (e.g., a NEW fixture file
   format or documentation subdirectory). Open a follow-up PR that
   references this entry, and add the new exclusion inline below
   the existing 10 with explicit rationale. The review checklist
   for such PRs: (a) the new exclusion is necessary, not
   convenient; (b) the new exclusion is documented inline; (c) the
   new exclusion does NOT weaken the audit's coverage of the 7
   forbidden disjuncts.

4. **If the carve-out removal is needed** (NOTE-block contributors
   removing the documentation comments that necessitate
   category 10): commit must delete the corresponding carve-out in
   the SAME atomic commit. Leaving either an unused carve-out (false
   freedom) or the NOTE alone (regression) both regress this
   audit's surface-area contract.

**Commit chain (1 commit on `main`, NO branches)**:

| Hash     | Subject                                |
| ---      | ---                                    |
| `59a91f7` | `ci(workflow): add no-youtube-regression guard` |

**Verification**:

- `cat .github/workflows/no-youtube-regression.yml` renders the
  workflow verbatim as documented above. The `REGEX` env var is
  the single source of truth for the audit pattern.

- `git log --oneline -1 -- .github/workflows/no-youtube-regression.yml`:
  `59a91f7 ci(workflow): add no-youtube-regression guard`.

- Local reproduction of the runner matrix (same pathspec
  exclusions as the workflow applies):
  ```bash
  git grep -nE 'google\.golang\.org/api/youtube|youtubeanalytics|VELOX_YOUTUBE|youtube_oauth|internal/integrations/youtube|handlers/server/youtube|providers\.NewYouTubeProvider' \
    -- ':!.github/workflows/no-youtube-regression.yml' \
    ':!**/migrations/**' ':!**/testdata/**' ':!**/*_test.go' \
    ':!**/*.example' ':!**/*.md' ':!CHANGELOG.md' \
    ':!**/socialcontract/**' ':!**/social_contract/**' \
    ':!DataServer/internal/jobs/enqueue/delivery_plan_validator.go' \
    ':!DataServer/internal/store/delivery_plan_payload.go'
  # expect: empty (0 matches on post-PR-15.x main)

- Manual `workflow_dispatch` against the workflow on `main` re-reports
  `✅ No YouTube regression found — clean.` at the gate tier. The
  canonical CI (`make verify`) does NOT duplicate this audit, so
  this workflow is the single source of truth for the regex match.

- `go test ./...`, `go vet ./...`, `go build ./...`: PASS.
  Workflow change is YAML-pure; no Go compile impact.

**Refs**:

- `.github/workflows/no-youtube-regression.yml` — workflow source-of-truth.
- `DataServer/internal/store/migrations/README.md` — forward-only
  migration invariant referenced by the `**/migrations/**` carve-out.
- `docs/SOCIAL_API_MIGRATION_RUNBOOK.md` — operator-facing closure
  context (closure backfill procedure for `social_destination_id` /
  `external_destination_id` legacy rows + audit procedure post-deploy).
- `PR-15.8 / PR-15.9 / PR-15.10 / PR-15.11 / PR-15.12 / PR-15.13 /
  PR-15.14` — the canonical closure chain this workflow guards.

<a id="pr-1514-residuo-4-closure-externaldestinationid-canonical-rename"></a>
