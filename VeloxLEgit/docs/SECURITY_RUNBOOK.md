# Velox Security Runbook (PR-5 / P0 Closure)

> **Audience:** SRE + on-call. **Scope:** All credentials, infrastructure
> identifiers, and gRPC / OAuth wiring that gate a production populate.
> **Owner:** Velox security WG. **Review cadence:** every PR-touching-deploy
> change.

This runbook is the canonical remediation map for the exposure surfaced
during the PR-5 audit (2026-06). It is a living document; every secret
rotation, hostname change, or release-channel flip must update §3.

---

## 1. Triage & Containment

When a leak is suspected (PR scan failing, anomalous token usage, scanner
report), the first 15 minutes are containment, not investigation:

| Time | Action |
|---|---|
| **T+0** | Set the affected secret's status to `revoked` in the upstream issuer (Google Cloud Console / Tailscale admin / Vault) |
| **T+1** | Block the affected credential at the network layer (Tailscale ACL, OAuth app restriction) |
| **T+5** | Snapshot the repo to a private branch for forensics; do NOT delete yet |
| **T+10** | Page the on-call security lead with §2 inventory filled in |
| **T+15** | Open an incident channel; freeze any planned deploys until §4 complete |

**Never** delay rotation because "we don't know the full impact." Rotating
a credential invalidates the prior secret, which is the entire point of
§1.

---

## 2. Impact / Exposure Inventory

Snapshot taken 2026-06 (PR-5 audit). Treat every entry below as live
until §3 confirms a fresh rotation date.

### 2.1 OAuth / GCP

| Entry | Status | Exposed in |
|---|---|---|
| `youtube-uploader-safe` project — OAuth Web client (id `712843578597-…`) | **Active** — live on disk in `DataServer/data/youtube/credentials/credentials.json` (gitignored). Was present in **historical git commits** under `VeloxEditing/refactored/DataServer/` paths (see §4). |
| `drive-uploading-…` project — OAuth Desktop client (id `964460747662-…`) | **Active** — same. |

### 2.2 Tailscale / Infrastructure

| Entry | Status | Exposed in |
|---|---|---|
| Tailnet hostname `vps-…tail…ts.net` (the audit found one such name in `drive/credentials/credentials.json`) | **Active** — drive OAuth callback URL. |

### 2.3 DuckDNS / GitHub Pages

| Entry | Status | Exposed in |
|---|---|---|
| `veloxmanager.duckdns.org` | **Active** — YouTube OAuth callback URL. |
| `marcus-ops.github.io/My-Video-Tool-Youtube-` | **Active** — YouTube OAuth JS origin. |

### 2.4 Operator-managed env

These are operator-populated, never committed in plaintext form (the
`deploy/group_vars/vault.yml.example` template is committed; the real
`vault.yml` is `ansible-vault`-encrypted locally and `gitignored`):

- `VELOX_ADMIN_TOKEN` (min 32 chars)
- `VELOX_REMOTE_ENGINE_TOKEN`
- `VELOX_YT_OAUTH_TOKEN_KEY` (base64 32-byte symmetric key for at-rest
  token encryption)
- `WORKER_TOKEN` / `VELOX_WORKER_TOKEN` (per-host worker credential)
- `VELOX_NVIDIA_API_KEY` (optional, only when GPU inference used)

### 2.5 GitHub / Container Registry

| Entry | Status |
|---|---|
| `secrets.GITHUB_TOKEN` (per workflow run, auto-issued) | Auto-managed; no rotation action needed. |
| Container-registry pull credential (Ansible Vault) | Operator-managed; rotated on personnel change. |

---

## 3. Secret Rotation Procedures

### 3.1 Google OAuth client_secret (P0 — DO THIS FIRST)

The two `GOCSPX-…` secrets are independently rotated under each project's
own Google Cloud Console. Steps:

1. **Owner only**: open https://console.cloud.google.com → select the
   project (`youtube-uploader-safe` OR `drive-uploading-…`) → **APIs &
   Services → Credentials → OAuth 2.0 Client IDs**.
2. Click the affected Web / Desktop client → **Reset secret**. The
   console generates a new `GOCSPX-…` string; the client_id stays the
   same (rotating client_id would require updating the redirect URI list).
3. **Replace** the new secret into the local `credentials.json` for the
   matching project. Verify the JSON parses and the `redirect_uris`
   array is intact.
4. **Smoke-test**: run `bash deploy/scripts/lib-validations.sh` is_https_url
   on every redirect URI; run a fresh OAuth dance via the locally-issued
   callback URL `http://localhost:8000/api/.../oauth/callback`.
5. **Sanity-check** that `bash scripts/ci/check-secrets.sh --include-untracked`
   still fires on the old `GOCSPX-…` in the file you just rotated (it
   SHOULD — that confirms the regex covers the format. Only after Step 6
   does the scan pass).

### 3.2 Tailscale tailnet hostname

Tailnet hostnames are auto-generated; rotating means **re-authenticating
the device**. Standard Tailscale admin flow. The hostname `vps-…ts.net`
is not a secret per se — it's a reachable address — but it leaks fleet
size to attackers, hence §4.

### 3.3 DuckDNS

DuckDNS hostname `veloxmanager.duckdns.org` is a third-party dynamic
DNS under duckdns.org account control. Rotating means **changing the
DuckDNS subdomain** (e.g. to `veloxmanager-2.duckdns.org`); update every
OAuth redirect URI + the worker `control_grpc_url` env accordingly. This
is an AddressSanitizer-class fix — the previous hostname should not be
re-used for a different project.

### 3.4 operator-managed env

`VELOX_ADMIN_TOKEN`, `WORKER_TOKEN`, `VELOX_YT_OAUTH_TOKEN_KEY`,
`VELOX_REMOTE_ENGINE_TOKEN`, `VELOX_NVIDIA_API_KEY`: operator-managed
under `deploy/group_vars/vault.yml` (ansible-vault-encrypted). Use
`ansible-vault rekey` to rotate. No external issuer involved.

---

## 4. Git-History Scrubbing

The PR-5 audit confirmed that credential artefacts were present in the
historical tree under `VeloxEditing/refactored/DataServer/{client_secret*.json, data/drive/tokens/*.json, data/secrets/youtube/tokens/*}`.
**Even if rotation completes**, the historical blob is still part of the
git graph on every clone until rewritten. Operators MUST run scrub
before any production population completes:

```bash
# SAFE-BY-DEFAULT. The script in scripts/ci/operator-history-scrub.sh
# refuses to do anything destructive without this env var:
export YES_I_REALLY_WANT_TO_SCRUB=1
bash scripts/ci/operator-history-scrub.sh --dry-run   # ALWAYS first
bash scripts/ci/operator-history-scrub.sh            # actual scrub + force-push
```

**Important**:
- History rewriting is a force-push. Every existing clone becomes stale.
- Coordinate via the Velox deployment channel BEFORE running.
- After force-push, validate the `git-filter-repo` post-condition:
  `git log --all --diff-filter=A --name-only | grep -E 'client_secret|tokens'`,
  which MUST return empty.

---

## 5. Deploy / Infrastructure Remediation

### 5.1 Release-channel guard (PR-5 5d)

`VELOX_RELEASE_CHANNEL` is now honored. The master refuses to start if
`VELOX_GRPC_ALLOW_INSECURE_DEV=true` AND `VELOX_RELEASE_CHANNEL != dev`.
Production deploys MUST set `VELOX_RELEASE_CHANNEL=production` (or
`staging`) and supply the TLS triple
`VELOX_GRPC_TLS_{CERT,KEY,CA}_FILE`.

The validator at `deploy/validate-master-env.sh` warns when
`VELOX_RELEASE_CHANNEL=dev` is found, even if the env file passes every
hard-fail rule.

### 5.2 Local-disk credential hygiene

`.gitignore` already covers:
- `DataServer/data/secrets/`
- `DataServer/data/drive/tokens/`
- `DataServer/data/youtube/` (entire directory)
- `DataServer/data/drive/credentials/` (added in PR-5)

Operators copy `*.example` fixtures to `credentials.json` locally; the
real file is gitignored; only the `.example` placeholder ships in the
repo.

### 5.3 CI gate

`make verify-fast` runs `scripts/ci/check-secrets.sh` (CI-mode: tracked
files only). Any future commit with `GOCSPX-*` / `*.ts.net` /
`*.duckdns.org` / `youtube-uploader-safe` etc. fails the build.

Operators running locally can opt into the untracked scan:
`bash scripts/ci/check-secrets.sh --include-untracked`. This is a NO-OP
on a clean tree but flags drift if you accidentally DROP a real secret
file into `DataServer/data/` without renaming.

---

## 6. Post-Mortem Template

After every rotation / scrub incident, file a post-mortem at
`docs/post-mortems/<YYYY-MM-DD>-<slug>.md` with:

1. **What was exposed** (file path, commit SHA, secret type, project).
2. **Why it got there** (operator error? missing exclusion? upstream
   vector?).
3. **How it was contained** (rotation date, scrub command run, deploy
   snapshot).
4. **Detection latency** (time between exposure and detection, broken
   down by signal source: CI scan / manual audit / external report).
5. **Remediation** (which check landed in §5 to prevent recurrence).
6. **Open follow-ups** (with commit refs).

A periodic dry-run of `bash scripts/ci/check-secrets.sh --include-untracked`
across the operator's local checkout is recommended (weekly).

---

## Appendix — Quick Reference

| Need | Action |
|---|---|
| Re-rotate a Google OAuth secret | §3.1 |
| Re-authenticate a Tailscale device (hostname rotation) | §3.2 |
| Change the DuckDNS subdomain | §3.3 |
| Re-key the ansible-vault | §3.4 (`ansible-vault rekey deploy/group_vars/vault.yml`) |
| Audit the repo for new leaks | §5.3 + `bash scripts/ci/check-secrets.sh --include-untracked` |
| Scrub historical git blobs | §4 + `bash scripts/ci/operator-history-scrub.sh` |
| File a post-mortem | §6 |
