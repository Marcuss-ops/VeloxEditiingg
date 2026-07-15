# LOC Baseline — Hotspots, policy and methodology

> Document set: [Part 1 — Baseline maps](loc-baseline.md) · **Part 2 — Hotspots, policy and methodology** · [Part 3 — Refactor history](loc-refactor-history.md)

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

The baseline was produced by the following reproducible commands. Re-run them after each refactor round and append the delta to `loc-refactor-history.md`.

```bash
# Top-level area totals (canonical scope behind §1 / §2)
for area in DataServer RemoteCodex/native/worker-agent-go scripts docs shared deploy Pipeline; do
  total=$(find "$area" -type f \
    \( -name '*.go' -o -name '*.py' -o -name '*.ts' -o -name '*.tsx' \
       -o -name '*.sh' -o -name '*.yaml' -o -name '*.yml' \
       -o -name '*.md' -o -name '*.json' -o -name '*.proto' \) \
    -not -path '*/.git/*' -not -path '*/node_modules/*' \
    -not -path '*/venv/*' -not -path '*/__pycache__/*' \
    -not -path '*/target/*' -not -path '*/dist/*' -not -path '*/build/*' \
    -exec wc -l {} + 2>/dev/null | tail -1 | awk '{print $1}')
  echo "$area: ${total:-0}"
done

# Per-extension global totals (drives §1 language columns)
for ext in go sh md yml yaml json proto py ts tsx js jsx; do
  total=$(find . -type f -name "*.$ext" \
    -not -path '*/.git/*' -not -path '*/node_modules/*' \
    -not -path '*/venv/*' -not -path '*/target/*' \
    -exec wc -l {} + 2>/dev/null | tail -1 | awk '{print $1}')
  echo ".$ext: ${total:-0}"
done

# DataServer/internal per-package (drives §3)
for pkg in $(ls -d DataServer/internal/*/); do
  total=$(find "$pkg" -type f -name '*.go' -exec wc -l {} + 2>/dev/null | tail -1 | awk '{print $1}')
  echo "$(basename $pkg)|${total:-0}"
done | sort -t '|' -k2 -rn

# DataServer/internal/handlers per-directory, ANY-depth
python3 - <<'PY'
import os, subprocess
root = 'DataServer/internal/handlers'
totals = {}
for dp, _, fn in os.walk(root):
    gos = [f for f in fn if f.endswith('.go')]
    if not gos:
        continue
    rows = []
    for f in gos:
        out = subprocess.check_output(['wc', '-l', os.path.join(dp, f)]).decode().split()[0]
        rows.append(int(out))
    rel = os.path.relpath(dp, root)
    totals['<root>' if rel == '.' else rel] = totals.get(rel if rel != '.' else '<root>', 0) + sum(rows)
for k in sorted(totals, key=lambda x: (-totals[x], x)):
    print(f'{k}|{totals[k]}')
print(f'TOTAL|{sum(totals.values())}')
PY

# RemoteCodex worker-agent-go internal + pkg per package
for mod in RemoteCodex/native/worker-agent-go/internal RemoteCodex/native/worker-agent-go/pkg; do
  for pkg in $(ls -d "$mod"/*/); do
    total=$(find "$pkg" -type f -name '*.go' -exec wc -l {} + 2>/dev/null | tail -1 | awk '{print $1}')
    echo "$pkg|${total:-0}"
  done | sort -t '|' -k2 -rn
done

# scripts / docs / deploy / shared per-subdir
for area in scripts docs deploy shared; do
  echo "## $area"
  case "$area" in
    scripts|deploy) pattern='*.sh'; pattern2='*.py' ;;
    docs)           pattern='*.md' ;;
    shared)         pattern='*.go'; pattern2='*.proto' ;;
  esac
  for d in $(ls -d $area/*/ 2>/dev/null); do
    total=$(find "$d" -type f \( -name "$pattern" -o -name "$pattern2" \) \
      -exec wc -l {} + 2>/dev/null | tail -1 | awk '{print $1}')
    echo "  $(basename $d)|${total:-0}"
  done | sort -t '|' -k2 -rn
done
```

> Repeat the run after each meaningful refactor round and append a section "Round N" with delta vs. baseline.

---

## 13. Next steps (roadmap)

1. **Wire CI LOC gate** — add `.golangci.yml` `funlen` rule (Go) and a per-file-length check for `.sh`/`.md`/`.yml` in CI; tracked under `docs/metrics/loc-todo.md` in the next step.
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

---
