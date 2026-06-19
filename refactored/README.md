# Velox — refactored/ subtree (deprecated post-promotion)

> **STATUS: this subtree is being collapsed into the repo root.**
> The Agent B PR (`codex/promote-ops-root`) has promoted `deploy/`
> content to the repo root and moved the worker/master image release
> workflows to `.github/workflows/`. DataServer/, RemoteCodex/, and
> shared/ remain under `refactored/` because the source code is
> out-of-scope for the Agent B pass; they will be promoted in the
> follow-on promotion PRs after Operator runs the post-pr
> two-worker-hardening runbook and merges Agent A.
>
> This README is preserved temporarily so a post-merge clone still
> locator-able until the `git rm -r refactored/` step in the
> Agent B followup completes. Once that lands, this file goes too.

## What this subtree WAS

```
refactored/
├── DataServer/                # Master server (Go/Gin + gRPC)        → promoted root in followon PR
├── RemoteCodex/               # Worker agent (Go) + video engine     → promoted root in followon PR
├── shared/                    # Shared Go lib                        → promoted root in followon PR
├── deploy/                    # PROMOTED in `codex/promote-ops-root`
│   ├── ansible.cfg            →  deploy/ansible.cfg
│   ├── requirements.yml       →  deploy/requirements.yml
│   ├── group_vars/            →  deploy/group_vars/
│   ├── inventory/             →  deploy/inventory/
│   ├── playbooks/             →  deploy/playbooks/
│   ├── runtime/               →  deploy/runtime/
│   ├── scripts/               →  deploy/scripts/
│   ├── templates/             →  deploy/templates/
│   └── velox-server.env.example →  deploy/velox-server.env.example
├── .github/                   # workflows PROMOTED in `codex/promote-ops-root`
│   └── workflows/             →  .github/workflows/
│       ├── master-image.yml   →  .github/workflows/master-image.yml
│       └── worker-image.yml   →  .github/workflows/worker-image.yml
├── docs/                      # STILL under refactored/ until subtree collapse
├── frontend_standalone/       # STILL under refactored/ until subtree collapse
└── ops/                       # ad-hoc operational helpers (not promoted)
```

## Source-of-truth after the promotion

The canonical locations are:

* `deploy/` for every Ansible + systemd + env-template concern
* `.github/workflows/` for GH Actions, including the master/worker
  image release pipelines (moved from `refactored/.github/workflows/`)
* `DataServer/`, `RemoteCodex/`, `shared/`, `frontend_standalone/`
  remain under `refactored/` until the eventual subtree collapse PR
* `docs/` is shared (top-level + `refactored/docs/`)

## Historical references

* `refactored/DataServer/internal/config/workers_validator.go`
  holds the canonical two-worker rule (`ValidateProductionWorkers`).
* `refactored/CHANGELOG.md` has the Agent A lockdown entry; the
  unredacted historical IP bloc was sanitized to
  `[REDACTED — historical public master endpoint]` per the post-PR
  hardening runbook.
* `docs/post-pr-two-worker-hardening.md` has the operator-side
  scrub + force-push + credential rotation acceptance criteria.

## Next steps for an operator landing on this branch

1. Read `docs/post-pr-two-worker-hardening.md` end-to-end. The
   four destructive actions are explicitly operator-delegated; the
   PR does not perform them.
2. Once Agent A is merged to main, rebase Agent B's
   `codex/promote-ops-root` onto updated `main`. After rebase,
   follow the spec's "eliminare ciò che resta di refactored" step
   (operator action; not executed in this branch).
3. Test gate: `python3 deploy/scripts/validate-jinja-render.py`
   must continue to return PASS on the post-merge `main`.
