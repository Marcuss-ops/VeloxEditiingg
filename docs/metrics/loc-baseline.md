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
| Total measured LOC *(top-level areas)* | **198 439** |
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

> **Note on totals.** "Total measured LOC (top-level areas) = 198 439" is the sum of the per-area rows in §2 (each row measured by file extensions within its top-level directory). The per-language rows above are computed by walking the whole repository root for each extension (`.go`, `.sh`, `.md`, `.yml`, …). They cover a wider universe (including files outside the seven top-level areas) so they sum to a larger number (~211 661). Both views are correct; the per-area figure is the canonical measure used by §3–§9.

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

Per top-level sub-package inside `DataServer/internal`. Bar scaled to max (`store` = 27 259). The ASCII block above shows the 15 largest sub-packages for visual density; the table below is exhaustive (every direct-child sub-package found by `DataServer/internal/*/`).

```
store                27259  | ##################################################
handlers             21235  | ##########################################
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

Every Go file path encountered by `os.walk` under `DataServer/internal/handlers` is bucketed into its deepest file-bearing directory. Rows are listed top-down (parents first, then leaves). Bar scaled to max (`server/youtube` = 2 811). Sum of all rows = **21 235 LOC**, exactly matching the §3 `handlers` package total.

```
server/youtube                  2811  | ##################################################
remote/ansible                  2049  | ####################################
server/darkeditor               1658  | ###############################
server/darkeditor/processors    1185  | ######################
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
| server/darkeditor | 1 658 |
| server/darkeditor/processors | 1 185 |
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
| **Total** | **21 235** |

> Notes:
> * The `<root>` row is `.go` files that live directly in `DataServer/internal/handlers/` (no subpackage).
> * Sub-directories that contain their own `.go` files are listed as independent rows; they are **not** already summed into the parent row above them. To compute the cost of e.g. the whole `server/darkeditor/` subtree, add `server/darkeditor` and `server/darkeditor/processors`.
> * Earlier draft of this section used `find -maxdepth` and family and under-counted several intermediate leaves (showing e.g. `darkeditor = 1 658` only). This version uses `os.walk` (any-depth) and is authoritative.

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
