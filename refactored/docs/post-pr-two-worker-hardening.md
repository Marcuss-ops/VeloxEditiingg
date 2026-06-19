# Post-PR Two-Worker Production Hardening — Operator Runbook

> **Audience**: operators who have MERGED PRs 1, 2, and 3 of the
> two-worker production lockdown onto `main`. The code changes in
> `codex/two-worker-production` (this branch) are necessary but NOT
> sufficient. The remaining actions are **operator-delegated and
> destructive** — this document lists them in execution order and
> identifies what to communicate to the wider team.

## Why this is a separate runbook (and not in the PR)

The PR shipped on this branch touches 18 files but does NOT perform
the four operator actions below. They were intentionally excluded from
the PR diff because they are irreversible, environment-specific, and
require coordination outside the repository:

1. Rewrite git history to remove previously published IPs/IDs.
2. Coordinated force-push to the canonical remote.
3. Rotate admin tokens, TLS certs, ansible-vault passwords, and any
   credential that may have been exposed in the rewritten history.
4. Recreate the two worker nodes with clean configuration.

Each item is non-trivial and makes prior in-tree evidence obsolete.
Doing any of them in a partial way leaks more than it scrubs.

---

## Pre-flight checklist (today, before any destructive action)

- [ ] All three PRs are merged onto `main` (PR 1, PR 2, PR 3 =
      `codex/two-worker-production`).
- [ ] There are no open feature branches that depend on the old
      `scripts/local-workers.sh.deprecated` stub, the old
      `VELOX_DB_DSN` alias, or the historical IPs/IDs in the changelog.
- [ ] All operators have read this runbook.
- [ ] A maintenance window is communicated and the master is being
      rolled to a known-good state via the new inventory layout
      (`[velox_workers]` not `[workers]`).

---

## Step 1 — History rewrite

Goal: remove previously published identifiers (`velox-worker-523925eb`,
`velox-worker-13197`, the historical master public IP referenced in
the 2026-06-13 changelog block, any Tailscale peer IP that landed in
committed files) from the git history before the rewritten repo is
pushed back to the canonical remote.

Tooling: `git-filter-repo` is the recommended tool (fast + safe
defaults). `bfg-repo-cleaner` is the alternative. Choose one and
stick with it for the whole scrub.

```bash
# Mirror first — never do anything irreversible on the live clone.
git clone --mirror git@github.com:marcuss-ops/velox-main.git \
    /tmp/velox-history-scrub.git

cd /tmp/velox-history-scrub.git

# Build a replacements file marking the historical public IPs/IDs to
# remove. NEVER replace with the literal new value — use a clear
# placeholder so a future grep can prove they are gone.
cat > /tmp/scrub-replacements.txt <<'REPL'
velox-worker-523925eb==>REDACTED_PROD_WORKER_1_ID
velox-worker-13197==>REDACTED_PROD_WORKER_2_ID
51.91.11.36==>REDACTED_PUBLIC_MASTER_IP
REPL

git filter-repo --replace-text /tmp/scrub-replacements.txt \
    --force
```

Verify the scrub worked:

```bash
# Every grep must return ZERO matches across ALL commits.
git log --all -p | grep -E 'velox-worker-523925eb|velox-worker-13197|<historical master IP>' \
    || echo "OK: identifiers fully scrubbed."
```

`git log --remotes --branches --tags` should still show every
commit, with diffs rerolled to contain only the redacted text.

---

## Step 2 — Coordinated force-push

Goal: rewrite the canonical `origin/main` so the cleaned history is
what `git clone` produces for every consumer.

```bash
# Dry-run check (mandatory) — show what would change.
git fetch origin
git rev-list --left-right --count origin/main...HEAD
# Confirm the left-right graph shows the scrubbed repo as the
# superset of origin and all contributors' pushes are in the
# rewrite.

# Real push (coordinated with all contributors).
git remote add scrub-target git@github.com:marcuss-ops/velox-main.git
git push scrub-target --force --all
git push scrub-target --force --tags
```

Coordinate with all active contributors BEFORE this push:

- Pause everyone from pushing to `main`.
- Share the rewritten history hash.
- Re-clone OR `git fetch origin && git reset --hard origin/main` for
  every developer worktree.

The first push wins. Subsequent pushes from pre-scrub clones will
re-introduce the identifiers.

---

## Step 3 — Credential rotation

Goal: invalidate every secret whose plaintext could have been
captured by the rewritten history (commit metadata, PR comments,
CI logs, branches that survived because contributors did not
fast-forward).

Rotate in this order:

1. **`VELOX_ADMIN_TOKEN`** on the master.
   - Generate: `openssl rand -hex 32`.
   - Distribute via the ansible vault (never paste in chat).
   - Restart the master service; old token fails closed at first REST
     call.
2. **Ansible vault password**.
   - Generate a new vault key, re-encrypt `deploy/group_vars/vault.yml`,
     commit the rotated vault, do NOT commit the new password.
3. **Worker credentials** (`ssh_private_key_file` + per-worker
   `velox-deploy` `sudo` password).
   - Re-issue via `bootstrap-ssh.yml` against the new inventory.
4. **TLS material** (master and gRPC):
   - Re-issue the server cert chain via the operator's preferred CA
     (Let's Encrypt / internal CA).
   - Re-issue the per-host worker client certs.
   - Re-issue the CA bundle.
   - Replace the relevant files in `/etc/velox/secrets/` and
     `/etc/velox-worker/certs/` on each host.
5. **Operator credential store** (`.velox-passwords.txt`).
   - Re-export from the vault to a fresh file on each operator's
     workstation.

If anything in this list was ever pasted in a public PR comment,
issue tracker, or chat log, treat it as already public and rotate
the upstream identity too (e.g. the OAuth client secret backing
the YouTube integration).

---

## Step 4 — Worker recreation with clean configuration

Goal: bring the two workers back online WITHOUT carrying forward
any of the credentials or filesystem state we just rotated.

Per-worker checklist:

- [ ] Destroy the existing instance (or stop the systemd unit) so
      nothing re-encrypts against the rotated keys.
- [ ] Re-image the host from the operator's canonical AMI/snapshot.
- [ ] Update DNS to the new host IP (this also becomes the new
      `ansible_host` in `production.ini`).
- [ ] Run `ansible-playbook -i deploy/inventory/production.ini \
      bootstrap-ssh.yml normalize_worker_systemd.yml`.
- [ ] Confirm systemd unit is active and the worker authenticated
      against the new `VELOX_ALLOWED_WORKERS` allowlist.
- [ ] Confirm the gRPC handshake completed (`grpc_cli ls
      <master>:9000` shows the new worker_id).

Repeat for worker_2.

After both workers are green:

- Pull the master DNS record from the old master to the new one
  (the master itself is NOT recreated by this runbook — the master
  state is in SQLite + blob store, and a different rotation process
  applies to it).
- File an internal incident report that lists every identifier/IP
  scrubbed and the rotations performed, with the timestamps and the
  operator who signed each step.

---

## What this PR does NOT cover

- Rotation of the master's TLS cert (handed off to the operator's
  PKI process; this PR only retired the `VELOX_DB_DSN` alias and
  pinned `VELOX_DB_PATH`).
- Migration of any in-flight jobs (caller is responsible for draining
  the queue before the master restart).
- Removal of historical Tailscale IPs that may still live in
  operator-only runbooks (`.md` files not committed) — those are
  out of repo scope.

---

## Acceptance criteria

The two-worker production lockdown is complete when, on the
**post-scrub clone** of `main`:

```bash
# No historical worker IDs anywhere.
git grep -nE 'velox-worker-523925eb|velox-worker-13197' || echo OK

# No historical public IP anywhere.
git grep -nE '<historical master public IP>' || echo OK

# No DSN alias anywhere.
git grep -nE 'VELOX_DB_DSN=' || echo OK

# No legacy stub referenced.
git grep -nE 'local-workers\.sh\.deprecated' || echo OK

# Canonical allowlist rule confirmed.
grep -n 'ValidateProductionWorkers' refactored/DataServer/internal/config/workers_validator.go
grep -n 'CHANGE_ME_WORKER_1,CHANGE_ME_WORKER_2' refactored/deploy/velox-server.env.example
grep -n '\[velox_workers\]' refactored/deploy/inventory/production.ini.example
```

All five checks return `OK` (and the two lines printed by the
last grep confirm the canonical rule is wired end-to-end).

---

## Related documents

- `refactored/CHANGELOG.md` — entries for PR 1, PR 2, and PR 3.
- `refactored/DataServer/internal/config/workers_validator.go` —
  the canonical two-worker rule.
- `refactored/DataServer/data/ansible/playbooks/tasks/prechecks.yml`
  — the Ansible pre-flight that mirrors the canonical rule.
- `refactored/deploy/inventory/production.ini.example` — the
  operator-facing inventory template (NEVER committed in full).
