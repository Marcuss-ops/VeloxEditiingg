# Phase 0 — CI required checks, branch protection, and SHA pinning

> **Cap. 1 of `docs/100-percent-plan/`**: the canonical release gate.
> "100%" means: every merge is gated by four required CI checks; the
> deterministic rendered artifact is byte-pinned via `E2E_EXPECTED_SHA256`;
> no merge bypass exists, not even for admins.
>
> This document is the **operator runbook** for setting up Phase 0 on a
> fresh repository. After setup, the four checks self-enforce — branches
> cannot be merged without all of them green.

---

## 1. Phase 0 in one page

| Building block                 | What it does                                                                                                        | Where it lives                                                       |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Four canonical required checks | `make verify` + 3 E2E targets; no PR merges unless all four are green on `main`                                     | `.github/workflows/{ci,e2e-grpc,e2e-workload,e2e-workload-mtls}.yml` |
| Branch protection              | `strict=true, enforce_admins=true, linear_history=true, force_pushes=false`                                         | `scripts/ci/enable-branch-protection.sh`                             |
| `E2E_EXPECTED_SHA256` secret   | Pinned `sha256` of the deterministic render output; fail-closed when absent                                         | Runner env (auto-bound via `secrets.E2E_EXPECTED_SHA256`)            |
| Local CI mirror                | `git pull && make verify && E2E_EXPECTED_SHA256=$PIN make e2e-{grpc,workload,workload-mtls}`                        | `scripts/ci/local-verify-mirror.sh`                                  |
| First-run SHA capture          | `E2E_CAPTURE_SHA256=1 make local-verify-mirror` derives the pin from the rendered artifact → `.e2e-expected-sha256.local` | `scripts/ci/local-verify-mirror.sh`                              |

The four required-check identifiers that GitHub matches against `branch_protection.contexts[]` are:

```text
1. CI / make verify
2. E2E gRPC control plane / make e2e-grpc (6-case matrix)
3. E2E workload (real) / make e2e-workload (Hello→Artifact→SUCCEEDED)
4. E2E workload-mTLS (PR 7) / make e2e-workload-mtls (mTLS, channel=staging)
```

These strings are derived from `<workflow-name> / <job-name>`. The workflow
files are the **single source of truth** — if you rename a workflow or job,
update the `contexts[]` array in `scripts/ci/enable-branch-protection.sh`.

---

## 2. Operator prerequisites (one time only)

```bash
# 1. Install the GitHub CLI (https://cli.github.com)
brew install gh           # macOS
sudo apt install gh       # Debian/Ubuntu
# OR download from https://github.com/cli/cli/releases

# 2. Authenticate
gh auth login
# → follow the browser prompt; pick HTTPS, enable SSO for your org

# 3. Verify
gh auth status
gh repo view --json url -q .url
# → should print e.g. https://github.com/Marcuss-ops/velox-server
```

That's the operator side. The repository side requires **one GitHub
secret** (see Step 3.

---

## 3. Wire `E2E_EXPECTED_SHA256` as a repo secret

The two workload workflows (`e2e-workload.yml`, `e2e-workload-mtls.yml`)
auto-inject the secret at job-env level (added in Phase 0):

```yaml
env:
  E2E_EXPECTED_SHA256: ${{ secrets.E2E_EXPECTED_SHA256 }}
```

Both Phase-0 workflows resolve the secret via this binding. There is no
additional configuration in the runner.

### 3a. Capture the canonical SHA on the platform that CI runs on

The pin is **platform-specific**: same FFmpeg version, same libx264 build,
same architecture produce the same bytes. CI uses `ubuntu-latest` so the
pin MUST be captured there.

```bash
# On an ubuntu-latest-equivalent host (e.g. Ubuntu 24.04):
sudo apt-get install -y ffmpeg sqlite3 python3 jq
E2E_EXPECTED_SHA256=dummy \
  ./scripts/ci/enable-branch-protection.sh --dry-run   # sanity, optional

E2E_CAPTURE_SHA256=1 \
  ./scripts/ci/local-verify-mirror.sh
# `local-verify-mirror` runs the workload tests ONCE with a placeholder
# SHA, harvests the real SHA from /tmp/velox-e2e-workload/storage/artifact.sha256,
# and writes it to .e2e-expected-sha256.local in the repo root.

# Read the captured pin
cat .e2e-expected-sha256.local
# → 64-char sha256. This is the canonical pin for ubuntu-latest.
```

Or — if you trust your CI runner's image as the source of truth — run
`make e2e-workload` on the runner, harvest the SHA from the artifact
log, and pin it as the repo secret. The CI-side provision is:

```bash
# After a passing CI run, harvest from the runner log:
make e2e-workload
ls -la /tmp/velox-e2e-workload/storage/artifact.sha256
cat /tmp/velox-e2e-workload/storage/artifact.sha256
# → "<real-sha>  <artifact-name>.mp4"
```

### 3b. Add the secret in GitHub repo settings

```text
GitHub Browser → repo → Settings → Secrets and variables → Actions
  → New repository secret
    Name:   E2E_EXPECTED_SHA256
    Value:  (the 64-char sha256 from §3a)
```

Or programmatically (admin scope required):

```bash
echo -n "$PIN" | gh secret set E2E_EXPECTED_SHA256 --repo "$OWNER/$REPO"

# Verify
gh secret list --repo "$OWNER/$REPO" | grep E2E_EXPECTED_SHA256
```

Both workload workflows will now auto-inject the secret on every PR.

---

## 4. Apply branch protection

```bash
make enable-branch-protection
# (or ./scripts/ci/enable-branch-protection.sh for the dry-run path)

# Verify
make inspect-branch-protection
# (or ./scripts/ci/inspect-branch-protection.sh)
```

Expected output of `inspect-branch-protection`:

```text
→ required_status_checks audit:
   ✓ CI / make verify
   ✓ E2E gRPC control plane / make e2e-grpc (6-case matrix)
   ✓ E2E workload (real) / make e2e-workload (Hello→Artifact→SUCCEEDED)
   ✓ E2E workload-mTLS (PR 7) / make e2e-workload-mtls (mTLS, channel=staging)
✓ all four Phase-0 required checks present.
```

If any check is `MISSING`, GitHub renamed the workflow or job. Re-run
`enable-branch-protection` after fixing the workflow YAML **and** updating
the canonical contexts array in `scripts/ci/enable-branch-protection.sh`.

---

## 5. Pre-PR local mirror

Once branch protection is live, every PR has a 4-check round-trip cost.
Developers can pre-flight locally with:

```bash
# Default: fail-closed unless E2E_EXPECTED_SHA256 is set or
# .e2e-expected-sha256.local exists.
make local-verify-mirror
# (or ./scripts/ci/local-verify-mirror.sh)

# First-run capture path (writes .e2e-expected-sha256.local)
E2E_CAPTURE_SHA256=1 make local-verify-mirror

# Fast feedback loop (skip cmake + docker)
SKIP_HEAVY=1 make local-verify-mirror

# make verify only (no e2e)
SKIP_E2E=1 make local-verify-mirror
```

Resolution order for `E2E_EXPECTED_SHA256`:

1. Operator export (`E2E_EXPECTED_SHA256=<sha> make local-verify-mirror`)
2. `.e2e-expected-sha256.local` repo-root file (gitignored, captured)
3. Repo secret via CI (only on GitHub Actions runner)
4. `E2E_CAPTURE_SHA256=1` capture mode (writes the file)

---

## 6. END-TO-END verification

Once §3 (secret) and §4 (protection) and §5 (mirror) all pass:

1. **Open a throwaway PR** (e.g. typo fix) on `main`.
2. GitHub should show all four required checks running:
   ```
   ✓ CI / make verify                                — required
   ✓ E2E gRPC control plane / make e2e-grpc (...)    — required
   ✓ E2E workload (real) / make e2e-workload (...)   — required
   ✓ E2E workload-mTLS (PR 7) / make e2e-workload-mtls (...) — required
   ```
3. The merge button is **blocked** if any of them is red.
4. The merge button is **enabled** only when all four are green AND the
   branch has CODEOWNER review approval (`.github/CODEOWNERS`).
5. Force-push is denied (`allow_force_pushes=false`).
6. Merge commits are forbidden (`required_linear_history=true`).

---

## 7. Rotating the SHA pin

The pin MUST be regenerated whenever any of these change:

| Change                                              | Effect on SHA |
| --------------------------------------------------- | ------------- |
| FFmpeg version (`ffmpeg -version`)                  | bytes differ  |
| libx264 build (Ubuntu apt update)                   | bytes differ  |
| Runner image (`ubuntu-22.04` → `ubuntu-24.04`)      | bytes differ  |
| Render fixture (the ffmpeg lavfi invocation in `tests/e2e/workload/run.sh`) | bytes differ  |
| Velox server binary encoding parameters             | bytes differ  |

Regeneration procedure:

```bash
# 1. On the canonical CI platform (ubuntu-latest equivalent):
E2E_CAPTURE_SHA256=1 make local-verify-mirror
NEW_SHA=$(cat .e2e-expected-sha256.local)

# 2. Update the GitHub repo secret
echo -n "$NEW_SHA" | gh secret set E2E_EXPECTED_SHA256 --repo "$OWNER/$REPO"

# 3. Open a PR to confirm the new pin matches the runner's render.
gh pr create --title "chore(ci): rotate E2E_EXPECTED_SHA256 pin" \
             --body "Pin updated to $NEW_SHA — verifier gate re-test."
```

The rotation PR must pass all four required checks on its first attempt;
otherwise the pin does not actually match the CI render and the rotation
is incomplete.

---

## 8. Incident response: removing branch protection

**This is a Phase 0 violation.** Only use it when:

- a CI breaking change is so widespread that all four checks red on every PR;
- a critical security CVE forces a hotfix with no time to wait for the matrix;
- an operator action has set the protection into a contradictory state.

```bash
make disable-branch-protection
# → Prints a loud warning box + 10s TTY confirm prompt.
# → In non-tty / CI:  VELOX_FORCE_REMOVE_PROTECTION=1 make disable-branch-protection
```

Restoration is immediate and idempotent:

```bash
make enable-branch-protection
make inspect-branch-protection
# → verify the four checks are listed again.
```

---

## 9. Troubleshooting

### 9a. The "make e2e-workload" check is red: SHA mismatch

Means the rendered bytes differ from the secret. Either:

- the platform drift is reported in §7 (regenerate);
- the secret was typed incorrectly (verify via `gh secret list`);
- the local render passes but CI render fails (means a runner image drift):
  `[ACTION] regenerate pin on the same runner image`;

### 9b. "Branch protection already exists" — idempotent?

`enable-branch-protection.sh` is idempotent: the JSON payload is the same
on every run. Re-running with no edits is a 200 OK no-op from GitHub. To
verify, `make inspect-branch-protection`.

### 9c. "gh: command not found" or "gh not authenticated"

`brew install gh && gh auth login`. On minimal containers, `apt install
gh` or use the static binary from GitHub releases.

### 9d. The local-mirror output says "make verify FAILED: working tree dirty"

Commit or stash your changes before running `make local-verify-mirror`.
The script refuses to pull/rebase onto a dirty tree to avoid silent
state corruption.

### 9e. The local-mirror output says "E2E_EXPECTED_SHA256 not set"

Resolution paths in priority order:

```bash
# 1. Capture (first time only):
E2E_CAPTURE_SHA256=1 make local-verify-mirror

# 2. Hand-set:
E2E_EXPECTED_SHA256=$(cat .e2e-expected-sha256.local) make local-verify-mirror

# 3. Override (e.g. CI's pin):
E2E_EXPECTED_SHA256=abc123... make local-verify-mirror
```

---

## 10. Summary

| Step | Action                                                    | Tool              | Exit |
| ---- | --------------------------------------------------------- | ----------------- | ---- |
| 1    | Install + auth `gh`                                       | `gh auth login`   | —    |
| 2    | Capture the canonical SHA on ubuntu-latest equivalent    | `E2E_CAPTURE=1`   | 0    |
| 3    | Set GitHub secret `E2E_EXPECTED_SHA256`                   | `gh secret set`   | —    |
| 4    | Apply branch protection                                   | `make enable-…`   | 0    |
| 5    | Verify protection state                                   | `make inspect-…`  | 0    |
| 6    | Open a throwaway PR, confirm merge button gated          | browser           | ✓    |
| 7    | Pre-flight every PR with `make local-verify-mirror`       | local             | 0    |
| 8    | Rotate the pin when FFmpeg/runner/fixture changes        | §7 above          | 0    |

After step 6, Phase 0 is **DONE** and Phase 1 (image certification) can begin.
