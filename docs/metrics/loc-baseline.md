# LOC Baseline — VeloxEditingg

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

Top-15 packages. Bar scaled to max (`store` = 27 259).

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

```
server/youtube         2811  | ##################################################
remote/ansible         2049  | ####################################
server/darkeditor      1658  | ###############################
server/api             1466  | ##########################
server/drive           1485  | ##########################
server/calendar        1399  | ########################
server/pipeline         994  | ##################
server/script           922  | ################
remote/workers         1413  | ##########################
remote/livestream       352  | ######
server/audit            305  | #####
server/smoke           255   | ####
remote/install         240   | ####
server/jobs            137   | ##
server/groups          174   | ###
remote (top-level)       0   |
server (top-level)       0   |
web (top-level)          0   |
web/proxy              233   | ####
web/explorer           138   | ##
web/spa                 68   | ##
server/health           11   | ##
```

| Sub-package | LOC |
| --- | ---: |
| server/youtube | 2 811 |
| remote/ansible | 2 049 |
| darkeditor | 1 658 |
| api | 1 466 |
| drive | 1 485 |
| remote/workers | 1 413 |
| calendar | 1 399 |
| pipeline | 994 |
| script | 922 |
| livestream | 352 |
| audit | 305 |
| smoke | 255 |
| install | 240 |
| groups | 174 |
| jobs | 137 |
| explorer | 138 |
| web/spa | 68 |
| health | 11 |

> Note: `server`, `remote`, `web` root packages register 0 LOC at this depth because they only delegate to sub-packages (no top-level `.go` file).

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

## 10. Hotspot files (LOC > policy)

Threshold policy used in this baseline:
- **Production Go:** warn > 600 LOC, refactor-required > 900 LOC
- **Test Go:** warn > 900 LOC, refactor-required > 1 200 LOC
- **Generated** (`.pb.go`, gRPC schemas, mocks): exempt
- **Docs:** warn > 800 LOC, refactor-required > 1 200 LOC
- **Scripts:** warn > 400 LOC, refactor-required > 700 LOC

### 10a. Longest production Go files

| LOC | Path | Package |
| ---: | --- | --- |
| 2 045 | `DataServer/internal/store/sqlite_task_repository.go` | store |
| 1 188 | `DataServer/internal/metrics/collector.go` | metrics |
| 1 099 | `DataServer/internal/metrics/supervisor.go` | metrics |
| 1 082 | `DataServer/internal/grpcserver/handler.go` | grpcserver |
| 982 | `RemoteCodex/native/worker-agent-go/internal/worker/worker.go` | worker |
| 880 | `RemoteCodex/native/worker-agent-go/internal/telemetry/resource_sampler.go` | telemetry |
| 865 | `DataServer/internal/completion/coordinator.go` | completion |
| 856 | `DataServer/internal/store/sqlite_task_attempt_repository.go` | store |
| 828 | `DataServer/internal/jobs/enqueue/enqueue.go` | jobs/enqueue |
| 811 | `DataServer/internal/forwarding/runner.go` | forwarding |
| 788 | `RemoteCodex/native/worker-agent-go/internal/publisher/transport_registry.go` | publisher |
| 758 | `DataServer/internal/store/store_creator_forwardings_lease.go` | store |
| 703 | `DataServer/internal/metrics/catalog.go` | metrics |
| 696 | `RemoteCodex/native/worker-agent-go/internal/taskrunner/runner.go` | taskrunner |
| 693 | `DataServer/internal/observability/service.go` | observability |
| 654 | `DataServer/cmd/dev-hello-client/main.go` | cmd |
| 640 | `RemoteCodex/native/worker-agent-go/internal/spool/store.go` | spool |
| 638 | `DataServer/internal/ingest/service.go` | ingest |
| 628 | `DataServer/internal/artifacts/reconciler.go` | artifacts |
| 626 | `RemoteCodex/native/worker-agent-go/cmd/velox-worker-agent/main.go` | cmd |
| 614 | `DataServer/internal/services/drive/service.go` | services/drive |
| 584 | `RemoteCodex/native/worker-agent-go/pkg/config/config.go` | pkg/config |

### 10b. Longest test files

| LOC | Path | Package |
| ---: | --- | --- |
| 1 521 | `DataServer/internal/store/sqlite_task_atomic_test.go` | store |
| 1 331 | `DataServer/internal/jobs/enqueue/enqueue_test.go` | jobs/enqueue |
| 1 283 | `DataServer/internal/store/sqlite_youtube_entities_test.go` | store |
| 1 201 | `RemoteCodex/native/worker-agent-go/pkg/config/config_test.go` | pkg/config |
| 1 129 | `DataServer/internal/grpcserver/handler_jobs_test.go` | grpcserver |
| 1 096 | `DataServer/internal/completion/coordinator_test.go` | completion |
| 964 | `DataServer/internal/store/migrations/migrations_test.go` | store/migrations |
| 956 | `RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream_test.go` | transport |
| 931 | `DataServer/internal/store/e2e_metrics_flow_test.go` | store |
| 859 | `DataServer/internal/artifacts/retry_budget_propagation_test.go` | artifacts |
| 832 | `DataServer/internal/ingest/service_test.go` | ingest |
| 742 | `DataServer/internal/artifacts/service_test.go` | artifacts |
| 735 | `DataServer/internal/store/store_creator_forwardings_test.go` | store |
| 718 | `DataServer/internal/outbox/outbox_test.go` | outbox |
| 671 | `DataServer/internal/workers/registry_test.go` | workers |
| 661 | `RemoteCodex/native/worker-agent-go/internal/publisher/transport_registry_test.go` | publisher |
| 660 | `DataServer/internal/completion/reconcile_test.go` | completion |
| 639 | `DataServer/internal/store/atomic_job_task_test.go` | store |
| 610 | `DataServer/internal/artifacts/service_finalize_ffprobe_test.go` | artifacts |
| 595 | `DataServer/internal/handlers/server/script/handler_test.go` | handlers/server/script |
| 327 | `DataServer/cmd/dev-hello-client/shutdown_test.go` | cmd |
| 324 | `DataServer/cmd/server/bootstrap_hardening_test.go` | cmd/server |

### 10c. Longest non-Go files

| LOC | Path | Category |
| ---: | --- | --- |
| 1 492 | `docs/architecture/CURRENT-TO-TARGET-ARCHITECTURE.md` | docs |
| 1 067 | `deploy/runtime/checklist-verify.sh` | deploy script |
| 794 | `scripts/cert/certify-worker-2c-2d.sh` | cert script |
| 765 | `docs/archive/architecture-pre-grpc.md` | archive docs |
| 696 | `docs/completion-protocol.md` | docs |
| 694 | `docs/operations/03-build-deploy-and-ci-hardening.md` | docs |
| 632 | `tests/e2e/grpc-control-plane/run.sh` | test infra |
| 488 | `DataServer/cmd/server/bootstrap_composition.go` | prod Go (cmd) |
| 413 | `DataServer/cmd/server/bootstrap_hardening_test.go` | test Go (cmd) |

---

## 11. Threshold policy (proposed)

| File type | Warn at | Refactor required at |
| --- | ---: | ---: |
| Production Go (`.go` excluding `_test.go`) | 600 | 900 |
| Test Go (`*_test.go`) | 900 | 1 200 |
| Generated (`*.pb.go`, mocks) | n/a (exempt) | n/a |
| Shell scripts (`.sh`, `.bash`) | 400 | 700 |
| Documentation (`.md`) | 800 | 1 200 |
| CI / Ansible (`.yml`, `.yaml`) | 400 | 800 |

These thresholds will be promoted into a `golangci-lint` `funlen` rule (Go) plus a CI guard for scripts/docs, see follow-up plan.

---

## 12. Methodology — how to reproduce

```bash
# Top-level area totals
for area in DataServer RemoteCodex/native/worker-agent-go scripts docs shared deploy Pipeline; do
  total=$(find "$area" -type f \
    \( -name '*.go' -o -name '*.py' -o -name '*.ts' -o -name '*.tsx' \
       -o -name '*.sh' -o -name '*.yaml' -o -name '*.yml' \
       -o -name '*.md' -o -name '*.json' -o -name '*.proto' \) \
    -not -path '*/.git/*' -not -path '*/node_modules/*' \
    -not -path '*/venv/*' -not -path '*/__pycache__/*' \
    -not -path '*/target/*' -not -path '*/dist/*' -not -path '*/build/*' \
    -exec wc -l {} + | tail -1 | awk '{print $1}')
  echo "$area: ${total:-0}"
done

# Per-package breakdown
for pkg in $(ls -d DataServer/internal/*/); do
  total=$(find "$pkg" -type f -name '*.go' -exec wc -l {} + | tail -1 | awk '{print $1}')
  echo "$(basename $pkg)|${total:-0}"
done | sort -t '|' -k2 -rn
```

> Repeat the run after each meaningful refactor round and append a section "Round N" with delta vs. baseline.

---

## 13. Next steps (roadmap)

1. **Wire CI LOC gate** — add `.golangci.yml` `funlen` rule (Go) and a per-file-length check for `.sh`/`.md`/`.yml` in CI.
2. **Refactor `DataServer/internal/store/sqlite_task_repository.go`** (2 045 LOC) — split per domain, keep public API identical.
3. **Refactor `DataServer/internal/metrics/{collector.go,supervisor.go}`** — extract sub-aggregators under `metrics/<sub>/`.
4. **Refactor `DataServer/internal/grpcserver/handler.go`** — one file per RPC service family.
5. **Refactor `worker-agent-go/internal/worker/worker.go`** — separate lifecycle / claim / registration.
6. **Refactor `DataServer/internal/completion/coordinator.go`** — split phases of completion protocol.
7. **Refactor `DataServer/internal/jobs/enqueue/enqueue.go`** + its 1 331-LOC test.
8. **Refactor `DataServer/internal/store/sqlite_task_attempt_repository.go`** + atomic test.
9. **Refactor worker-agent subsystems** (`resource_sampler.go`, `transport_registry.go`, `taskrunner/runner.go`, `spool/store.go`).
10. **Document file-size policy** in `CONTRIBUTING.md`, link from CI gate error messages.
11. **Re-measure LOC after each refactor** and append a Round N section here.
12. **Track cumulative delta** in this file over time to measure progress.

---

## 14. Tags / glosary

| Tag | Meaning |
| --- | --- |
| `[prod]` | Code that ships in production binaries |
| `[test]` | Go test file; excluded from binary build |
| `[generated]` | Produced by `protoc` / mockgen / similar; do not edit by hand |
| `[script]` | `.sh` / `.bash` automation |
| `[doc]` | Markdown documentation |
| `[infra]` | Ansible / k8s / docker / CI |

> Re-run the measurement, classify each long file using these tags, and link each hotspot to a follow-up refactor.
