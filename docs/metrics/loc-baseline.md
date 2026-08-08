# LOC Baseline — VeloxEditingg

> Document set: **Part 1 — Baseline maps** · [Part 2 — Hotspots, policy and methodology](loc-baseline-policy.md) · [Part 3 — Refactor history](loc-refactor-history.md)

> **Snapshot:** initial measurement of the repository
> **Scope:** source-tracked files in `DataServer/`, `RemoteCodex/native/worker-agent-go/`, `scripts/`, `docs/`, `shared/`, `deploy/`, `Pipeline/`
> **Exclusions:** `.git/`, generated vendored code, build artifacts, `node_modules`, virtualenvs
> **Purpose:** establish a measurable baseline of code size per top-level area and per package, identify refactor hotspots, kick off the Long-File Reduction plan

---

## 1. Executive summary

| KPI | Value |
| --- | --- |
| Total measured LOC *(top-level areas, initial snapshot)* | **198 439** |
| `.go` LOC total | **161 379** |
| `.sh` LOC total | **20 754** |
| `.md` LOC total | **20 739** |
| `.yml` LOC total | **5 859** |
| `.json` LOC total | **1 695** |
| `.proto` LOC total | **645** |
| `.py` LOC total | **590** |
| Longest non-generated file (prod Go) | `DataServer/internal/store/sqlite_task_repository.go` (2 045 LOC) |
| Longest test file | `DataServer/internal/store/sqlite_task_atomic_test.go` (1 521 LOC) |
| Hottest package | `DataServer/internal/store` (27 259 LOC) |
| Generated-proto dominance | `shared/controltransport/pb/worker_control.pb.go` (4 460 LOC) |

> Pipeline/ is currently empty in this snapshot (0 LOC tracked). If reintroduced it must be re-measured.

> **Note on totals.** "Total measured LOC (top-level areas, initial snapshot) = 198 439" is the sum of the per-area rows in §2 (each row measured by file extensions within its top-level directory). These figures are retained as the historical starting point for the refactor plan. The reproducible §12 methodology must be rerun to publish a new baseline after structural removals; this document does not infer current totals by subtracting retired code manually.

---

## 2. Heatmap — LOC per top-level area

Bar width is scaled to the largest area (`DataServer`).

```
DataServer                          124741  | ##################################################
RemoteCodex/.../worker-agent-go      31963  | ##############
docs                                 18817  | ########
scripts                               9865  | ####
shared                                9061  | ####
deploy                                3992  | ##
Pipeline                                 0  |
```

| Area | LOC | Share of total |
| --- | ---: | ---: |
| DataServer | 124 741 | 62.9 % |
| RemoteCodex/native/worker-agent-go | 31 963 | 16.1 % |
| docs | 18 817 | 9.5 % |
| scripts | 9 865 | 5.0 % |
| shared | 9 061 | 4.6 % |
| deploy | 3 992 | 2.0 % |
| Pipeline | 0 | 0.0 % |

---

## 3. Heatmap — DataServer/internal per package

Per top-level sub-package inside `DataServer/internal` in the initial snapshot. Bar scaled to max (`store` = 27 259). The ASCII block above shows the 15 largest sub-packages for visual density; the table below is exhaustive (every direct-child sub-package found by `DataServer/internal/*/`). The retired editor code is retained in this historical package total; the current-scope handlers breakdown is reported separately in §3a.

```
store                27259  | ##################################################
handlers             21235  | ########################################
jobs                  7229  | ##############
integrations          6517  | ############
artifacts             6516  | ############
completion            6211  | ############
metrics               5145  | ##########
grpcserver            5044  | ##########
workers               2586  | #####
services              2118  | ####
supervisor            2081  | ####
outbox                1942  | ###
deliveries            1884  | ###
taskgraph             1874  | ###
assets                1641  | ###
```

| Package | LOC |
| --- | ---: |
| store | 27 259 |
| handlers | 21 235 |
| jobs | 7 229 |
| integrations | 6 517 |
| artifacts | 6 516 |
| completion | 6 211 |
| metrics | 5 145 |
| grpcserver | 5 044 |
| workers | 2 586 |
| services | 2 118 |
| supervisor | 2 081 |
| outbox | 1 942 |
| deliveries | 1 884 |
| taskgraph | 1 874 |
| assets | 1 641 |
| creatorflow | 1 589 |
| observability | 1 502 |
| ingest | 1 470 |
| forwarding | 1 440 |
| config | 1 335 |
| app | 1 109 |
| alertengine | 1 051 |
| costmodel | 941 |
| taskattempts | 875 |
| logging | 873 |
| platform | 817 |
| audit | 791 |
| placement | 609 |
| remoteengine | 507 |
| registry | 463 |
| secrets | 410 |
| routing | 345 |
| alerts | 308 |
| telemetry | 204 |
| dbutil | 148 |
| taskoutput_artifacts | 67 |
| performance | 49 |
| identity | 44 |
| metricscenter | 37 |

### 3a. Heatmap — DataServer/internal/handlers (sottopackage)

Every Go file path encountered by `os.walk` under `DataServer/internal/handlers` is bucketed into its deepest file-bearing directory. Rows are listed top-down (parents first, then leaves). This current-scope presentation excludes the retired editor subtree and totals **18 392 LOC**. The initial snapshot total was **21 235 LOC**, including **2 843 LOC** of retired editor code; rerun §12 before using any section as a new repository-wide baseline. Bar scaled to max (`server/youtube` = 2 811).

```
server/youtube                  2811  | ##################################################
remote/ansible                  2049  | ####################################
server/drive                    1485  | ##########################
server/api                      1466  | ##########################
remote/workers                  1413  | ##########################
remote/workers/lifecycle         949  | ##################
remote/workers/uploads           741  | #############
server/calendar                 1399  | ########################
server/pipeline                  994  | ##################
server/script                    922  | ################
server/youtube/creative          700  | ############
server/youtube/videos            584  | ##########
remote/livestream                352  | ######
server/audit                     305  | #####
server/smoke                     255  | ####
remote/workers/validation        248  | ####
remote/install                   240  | ####
web/proxy                        233  | ####
remote/workers/management        219  | ###
remote/workers/assets            208  | ###
server/groups                    174  | ###
remote/workers/sse               147  | ##
<root> (orphan .go files in handlers/)  144  | ##
web/explorer                    138  | ##
server/jobs                      137  | ##
web/spa                          68  | ##
server/health                    11  | ##
```

| Sub-package | LOC |
| --- | ---: |
| server/youtube | 2 811 |
| server/youtube/creative | 700 |
| server/youtube/videos | 584 |
| remote/ansible | 2 049 |
| server/drive | 1 485 |
| server/api | 1 466 |
| remote/workers | 1 413 |
| remote/workers/lifecycle | 949 |
| remote/workers/uploads | 741 |
| remote/workers/validation | 248 |
| remote/workers/management | 219 |
| remote/workers/assets | 208 |
| remote/workers/sse | 147 |
| server/calendar | 1 399 |
| server/pipeline | 994 |
| server/script | 922 |
| remote/livestream | 352 |
| server/audit | 305 |
| server/smoke | 255 |
| remote/install | 240 |
| web/proxy | 233 |
| server/groups | 174 |
| `<root>` (orphan `.go` files in `handlers/`) | 144 |
| web/explorer | 138 |
| server/jobs | 137 |
| web/spa | 68 |
| server/health | 11 |
| **Current-scope total** | **18 392** |

> Notes:
> * The `<root>` row is `.go` files that live directly in `DataServer/internal/handlers/` (no subpackage).
> * Sub-directories that contain their own `.go` files are listed as independent rows; they are **not** already summed into the parent row above them.
> * Earlier draft of this section used `find -maxdepth` and under-counted intermediate leaves. This version uses `os.walk` (any-depth) and records the initial snapshot only.
> * Retired editor packages are intentionally omitted from this current-scope presentation. The initial snapshot total is retained in the section text for comparison; re-run §12 to publish a fully reconciled repository-wide baseline.

### 3b. Heatmap — DataServer/cmd

| Path | LOC |
| --- | ---: |
| `cmd/dev-hello-client/main.go` | 654 |
| `cmd/server/bootstrap_composition.go` | 488 |
| `cmd/server/bootstrap_hardening_test.go` | 413 |
| `cmd/worker/recover_output.go` | 357 |
| `cmd/dev-hello-client/shutdown_test.go` | 324 |
| `cmd/server/router.go` | 278 |
| `cmd/server/bootstrap_modules.go` | 273 |
| `cmd/server/bootstrap_grpconfig_test.go` | 254 |
| `cmd/server/bootstrap.go` | 212 |
| `cmd/server/bootstrap_test.go` | 204 |
| `cmd/velox-bundler/main.go` | 183 |
| `cmd/server/bootstrap_readiness.go` | 164 |
| `cmd/server/bootstrap_transport.go` | 155 |
| `cmd/server/bootstrap_assets.go` | 145 |
| `cmd/server/bootstrap_persistence.go` | 113 |
| `cmd/server/router_script_routes_test.go` | 105 |
| `cmd/server/bootstrap_tasks.go` | 83 |
| `cmd/server/bootstrap_grpc.go` | 79 |
| `cmd/server/bootstrap_grpc_test.go` | 75 |
| `cmd/server/bootstrap_alerts.go` | 67 |
| `cmd/server/bootstrap_test_helpers_test.go` | 65 |
| `cmd/seed-velox-db-fixture/main.go` | 60 |
| `cmd/server/main.go` | 58 |
| `cmd/server/bootstrap_workers.go` | 50 |
| `cmd/server/bootstrap_middleware.go` | 39 |
| `cmd/server/bootstrap_jobs.go` | 38 |
| `cmd/server/shutdown.go` | 37 |
| `cmd/server/bootstrap_audit.go` | 35 |
| `cmd/server/bootstrap_config.go` | 26 |

> The `cmd/server/bootstrap_*.go` family is a candidate for `cmd/server/bootstrap/` sub-package split.

---

## 4. Heatmap — RemoteCodex/native/worker-agent-go/internal per package

```
worker        5757  | ##################################################
telemetry     4783  | ##########################################
taskrunner    2558  | #######################
publisher     2270  | ####################
transport     1418  | ############
spool         1139  | ##########
executor      1093  | ##########
oteltrace      108  | ##
```

| Package | LOC |
| --- | ---: |
| worker | 5 757 |
| telemetry | 4 783 |
| taskrunner | 2 558 |
| publisher | 2 270 |
| transport | 1 418 |
| spool | 1 139 |
| executor | 1 093 |
| oteltrace | 108 |

## 5. Heatmap — RemoteCodex/native/worker-agent-go/pkg per package

```
video         2947  | ##################################################
config        1933  | #################################
doctor        1342  | #######################
bootstrap     1295  | #######################
api           1272  | #######################
cache          619  | ###########
logger         487  | ########
blob           385  | ######
resilience     309  | #####
bundle         250  | ####
binaryresolver 164  | ##
validation     137  | ##
```

| Package | LOC |
| --- | ---: |
| video | 2 947 |
| config | 1 933 |
| doctor | 1 342 |
| bootstrap | 1 295 |
| api | 1 272 |
| cache | 619 |
| logger | 487 |
| blob | 385 |
| resilience | 309 |
| bundle | 250 |
| binaryresolver | 164 |
| validation | 137 |

---

## 6. Heatmap — scripts per subdir

```
ci          5035  | ##################################################
cert        2948  | ##############################
operator      91  | ##
```

| Subdir | LOC |
| --- | ---: |
| ci | 5 035 |
| cert | 2 948 |
| operator | 91 |

> `scripts/cert/certify-worker-2c-2d.sh` (794 LOC) is the single longest script and a refactor candidate.

## 7. Heatmap — docs per subdir

```
architecture       5096  | ##################################################
100-percent-plan   2920  | #############################
rw-prod            2077  | #######################
operations         1998  | #######################
roadmap            1993  | #######################
archive             765  | #######
api                 526  | #####
pr                  260  | ##
audit                88  | ##
```

| Subdir | LOC |
| --- | ---: |
| architecture | 5 096 |
| 100-percent-plan | 2 920 |
| rw-prod | 2 077 |
| operations | 1 998 |
| roadmap | 1 993 |
| archive | 765 |
| api | 526 |
| pr | 260 |
| audit | 88 |

> `docs/architecture/CURRENT-TO-TARGET-ARCHITECTURE.md` (1 492 LOC) is the single longest doc — target for splitting per chapter.

## 8. Heatmap — deploy per subdir

```
runtime      1928  | ##################################################
scripts       956  | #########################
playbooks     369  | ##########
certs         166  | ####
group_vars    108  | ##
templates      72  | #
inventory     66   | #
```

| Subdir | LOC |
| --- | ---: |
| runtime | 1 928 |
| scripts | 956 |
| playbooks | 369 |
| certs | 166 |
| group_vars | 108 |
| templates | 72 |
| inventory | 66 |

> `deploy/runtime/checklist-verify.sh` (1 067 LOC) and `deploy/templates/velox-server.env.j2` are also long single files worth tracking.

## 9. Heatmap — shared per subdir

```
controltransport  5356  | ##################################################
contract          2112  | #######################
payload            532  | #####
obs                284  | ##
identity           184  | #
paths              163  | #
taskcontract        88  | #
validation          83  | #
placement           62  | #
media               55  | #
```

| Subdir | LOC |
| --- | ---: |
| controltransport | 5 356 |
| contract | 2 112 |
| payload | 532 |
| obs | 284 |
| identity | 184 |
| paths | 163 |
| taskcontract | 88 |
| validation | 83 |
| placement | 62 |
| media | 55 |

> `shared/controltransport/pb/worker_control.pb.go` (4 460 LOC) is **all generated code**, kept for reference only.

---

## 10c. Known LOC carry-over

The following files currently exceed the per-category LOC threshold defined
in §11. They are tracked as `KNOWN_VIOLATIONS` in
`scripts/ci/check-loc-thresholds.sh` so the gate passes on `main`. Each
entry MUST be removed when the corresponding refactor lands, per §13.

### Round-0 baseline (pre-existing)
- `tests/operational/artlist_live_e2e_verify.sh` (757 LOC, shell) —
  pre-existing operational E2E verifier, tracked for Round-4 dedup with
  `tests/e2e/grpc-control-plane/run.sh` into `tests/_lib/sh/` helpers.

### Round-5 carry-over (snapshot 2026-08-08)

| File | LOC | Category | Refactor target | Status |
| --- | ---: | --- | --- | --- |
| `tests/e2e/workload/run.sh` | 705 | shell | AGENTS plan step — split workload E2E orchestrator into `tests/_lib/sh/` phase helpers | real refactor target, tracked |

`tests/e2e/workload/run.sh` exceeded the §11 shell threshold (700) when
the workload E2E orchestration grew (video-counter label cardinality,
semaphore fast-fail, artifact gate promotion). It is a single-purpose
operator E2E orchestrator; the refactor split is tracked above and in
`KNOWN_VIOLATIONS_ROUND3` of `scripts/ci/check-loc-thresholds.sh`.

### Round-4 carry-over (snapshot 2026-07-28)

The following three files currently exceed the per-category LOC
threshold defined in §11. They are tracked as `KNOWN_VIOLATIONS` in
`scripts/ci/check-loc-thresholds.sh` so the gate passes on `main`.

| File | LOC | Category | Refactor target | Status |
| --- | ---: | --- | --- | --- |
| `tests/e2e/grpc-control-plane/run.sh` | 737 | shell | AGENTS plan step 7 — dedup into `tests/_lib/sh/` helpers | real refactor target, tracked |
| `CHANGELOG.md` | 1 691 | docs | **structural** (see justification below) | exempt, WARNING-only |
| `DataServer/api/openapi.yaml` | 1 235 | yaml | **structural** (see justification below) | exempt, WARNING-only |

**Structural exemption justification (CHANGELOG.md, openapi.yaml):**

Two of the three Round-4 carry-overs are **structural** — their size
grows monotonically by design, and a "refactor" would be fictitious:

- **`CHANGELOG.md`** is a *cumulative* release-notes file. Its entire
  purpose is the single-source chronological history of the project
  (one canonical timeline, one `git blame` view, one place to read
  release-to-release). Splitting it per release would destroy this
  contract. The growth is the deliverable, not a smell. Mitigation:
  track size in CI (`STRUCTURAL_LONG_FILES` WARNING-only, no gate),
  not refactor it.

- **`DataServer/api/openapi.yaml`** is the OpenAPI *single-source-of-truth*
  spec for the entire HTTP/gRPC surface. Every endpoint / schema /
  parameter is a real declaration, not boilerplate. A "refactor" would
  mean either (a) splitting per tag — which loses the single-source
  `$ref` graph and forces consumers to walk multiple files — or (b)
  compressing descriptions (cosmetic, not refactor). Real reduction
  requires a spec redesign (e.g. component-ize per resource), tracked
  under AGENTS plan Round-5+. Until then, the file stays as-is and
  is monitored via `STRUCTURAL_LONG_FILES` WARNING-only.

Both files are also listed in `scripts/ci/check-loc-thresholds.sh`
under `STRUCTURAL_LONG_FILES` (with the same per-file justification)
and re-emitted as `::warning` annotations on every CI run so the
trend stays visible in PRs. See `docs/metrics-catalog.md §R.1` for
the formal monitoring entry.

The third carry-over (`tests/e2e/grpc-control-plane/run.sh`) is a
*real* refactor target per AGENTS plan step 7 and is not in the
structural set; remove its `KNOWN_VIOLATIONS_ROUND2` entry when the
dedup lands.

### CI wiring

The gate is wired to a **dedicated status check**
(`.github/workflows/loc-thresholds.yml`, runs on push to `main` +
every pull_request + a daily 06:00 UTC drift detector + manual
`workflow_dispatch`). The check appears in the PR UI as
`loc-thresholds / LOC threshold gate` and is registered in
`scripts/ci/inspect-branch-protection.sh` `CANONICAL_REQUIRED`. The
same script also runs as an inline sub-step of the `verify` job in
`.github/workflows/ci.yml` (with `if: always()`) so the inline log
shows the per-file annotation stack in the same job as the rest of
the verify output. Both invocations call the same script, so the two
views cannot disagree.

**Operator action**: after this workflow file lands on `main`, an
operator must re-run `./scripts/ci/enable-branch-protection.sh` to
register `loc-thresholds / LOC threshold gate` as a required status
check in the GitHub branch-protection payload. Until that runs, the
dedicated workflow is defined but not enforced — CI will pass locally
+ on PRs but the branch protection won't actually require the gate on
merge. Verify with `./scripts/ci/inspect-branch-protection.sh` (its
audit pass now requires the 8th entry `loc-thresholds / LOC
threshold gate`).

### Round-4 gate change
- `scripts/ci/check-loc-thresholds.sh` gained a `BUILD_NOISE_EXCLUDES`
  array (nested `*/.git`, `*/node_modules`, `*/build`, `*/.pb-cache`)
  that is now prepended to every per-category scan. This silences
  false positives in third-party and CMake build trees that were
  wrongly tripping the gate. Per-category excludes (e.g.
  `./docs/archive`, `./shared/controltransport/pb/*.pb.go`) are kept
  on a per-scan basis.
