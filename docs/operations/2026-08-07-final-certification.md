# Final certification — 2026-08-07

## Decision

**NOT CERTIFIED for SSH legacy removal.** OpenBao is operational and its
least-privilege checks pass. The CA and hardening policy now pass on the three
reachable workers, but the Master and one worker remain unreachable and two
worker runtimes are not converged. `authorized_keys` and password-based SSH
therefore remain transition controls and must not be removed yet.

This report contains metadata and outcomes only. It contains no token,
password, private key, certificate body, or secret value.

## Repository baseline

| Check | Result |
| --- | --- |
| Branch | `main` |
| HEAD | `c7d6f7be` |
| Expected OpenBao commit | `df589232` is an ancestor |
| Expected worker-runtime commit | `fd7e7558` is an ancestor |
| `origin/main` | aligned with HEAD at certification time |
| Local worktree | pre-existing uncommitted changes preserved; no reset/pull-overwrite performed |

## OpenBao

Passed:

- Bash syntax and ShellCheck for the SSH CA scripts.
- `scripts/ci/test-openbao-ssh-ca.sh` structural gate, idempotence check,
  certificate signing, TTL check, and negative `root` principal check.
- Live `verify-ssh-ca.sh` against the healthy local OpenBao container.
- Live AppRole login for `ssh-operator` and `admin`.
- `ssh-operator` capability check: `update` on
  `ssh/sign/velox-operator`, `read` on CA/role metadata, no role mutation,
  and `deny` on the Velox KV path.

The role currently permits both `velox-admin` and `velox-deploy` as certificate
principals. Only `velox-deploy` has been treated as the canonical Unix login;
there is no verified `velox-admin` → Unix-account mapping in `sshd`.

## VPS evidence — initial and remediation pass

The following evidence was collected using read-only SSH probes where the host
was reachable:

| Target | SSH reachability | CA file | Effective SSH policy | Worker |
| --- | --- | --- | --- | --- |
| `57.129.132.133` | TCP/22 open, SSH closes/rejects all available key/account combinations | not reached | not reached | not certified |
| `57.131.20.173` | reachable | CA installed; fingerprint matches | `TrustedUserCAKeys` set, password/root disabled | service inactive; container TLS/config failure |
| `149.56.131.97` | reachable | CA installed; fingerprint matches | `TrustedUserCAKeys` set, password/root disabled | service active; state dir canonical |
| `51.222.204.158` | reachable | CA installed; fingerprint matches | `TrustedUserCAKeys` set, password/root disabled | canonical unit absent; legacy units remain |
| Master `51.91.11.36` | REST health reachable (`200`), operator token returns `401`; SSH rejects all available key/account combinations | not reached | not reached | not certified |

No legacy per-worker systemd drop-ins were found on the three reachable
workers. `authorized_keys` was still present on at least two reachable
workers. The canonical CA deployment and SSH hardening were applied to all
three reachable workers without removing the fallback key path.

## Certificate-only E2E result

OpenBao issued a valid short-lived `velox-deploy` certificate and the
certificate fingerprint matched the local OpenBao CA fingerprint. A first SSH
probe appeared successful, but that result was rejected as insufficient
evidence because the same private key could still authenticate through
`authorized_keys`.

The corrected probe forced:

```text
PubkeyAcceptedAlgorithms=ssh-ed25519-cert-v01@openssh.com
```

Certificate-only login initially failed because the CA was absent. After the
controlled CA/policy rollout, certificate-only login as `velox-deploy` passed
on all three reachable workers. The test used the certificate algorithm only;
it could not fall back to the legacy raw public key.

## Legacy disposition

Do **not** remove yet:

- `vault_velox_operator_pubkey` and the `authorized_key` bootstrap task;
- `/home/velox-deploy/.ssh/authorized_keys`;
- SSH password/ref fallback (`ssh_host_*` / `ansible_ssh_pass` compatibility).

Keep enforced or target these controls:

- `PermitRootLogin no`;
- `PasswordAuthentication no`;
- `KbdInteractiveAuthentication no`;
- `TrustedUserCAKeys /etc/ssh/trusted-user-ca-keys.pem`;
- canonical `VELOX_STATE_DIR=/var/lib/velox-worker` on every worker;
- zero legacy per-worker systemd drop-ins.

## Required remediation before re-certification

1. Restore SSH reachability to `57.129.132.133` and the Master.
2. Materialize the OpenBao CA public key on Master and every remaining node.
3. Apply and verify the effective `sshd -T` policy everywhere.
4. Restore inactive workers and converge every host on the canonical worker
   unit/state directory; do not start legacy units as a substitute.
5. Issue a `velox-deploy` certificate through the `ssh-operator` AppRole.
6. Prove certificate-only login on Master and every worker.
7. Only then remove `authorized_keys` and password/ref fallbacks in one
   reviewed change window.

## Live remediation progress

The following safe partial convergence has since been applied to the three
reachable workers:

- CA public key installed at `/etc/ssh/trusted-user-ca-keys.pem`, mode `0600`,
  with the expected fingerprint.
- SSH hardening loaded from the precedence-first drop-in and validated with
  `sshd -t`; the daemon was reloaded.
- `velox-deploy` account created and locked on the worker where it was absent.
- Certificate-only login as `velox-deploy` passed on all three workers.
- Legacy `authorized_keys` was deliberately retained as a fallback.
- `worker_57_131` canonical service was restarted successfully, but readiness
  remains `not_registered` because its current credential does not match a
  credential available in OpenBao.
- `worker_523925` still has a legacy container/unit and lacks the canonical
  service/environment; it was not switched blindly because the pinned image
  and canonical credential material are not available for a safe convergence.

The Master identity and `57.129.132.133` remain unresolved. No fleet-wide
legacy removal is authorized until these conditions are cleared.

The legacy `51.222.204.158` material is not a safe conversion source: its
legacy environment advertises a different worker identity, lacks the
canonical service environment, and uses a local mutable image reference.
Replacing it requires an authoritative worker credential and a pinned
canonical release, not an in-place unit rename.

## 2026-08-07 — 51.222.204.158 final end-to-end certification attempt

**Decision: NOT CERTIFIED.** This is a read-only certification attempt; no
worker, container, state, backup, or Chronon3d mutation was performed during
the probe. The requested PASS verdict is rejected because the canonical worker
is not active/healthy and the Master still reports it disconnected.

### Evidence snapshot

| Criterion | Result | Evidence |
| --- | --- | --- |
| Legacy Master unit absent | **PASS** | `/etc/systemd/system/velox-master.service` absent; `LoadState=not-found`, `ActiveState=inactive` |
| Legacy Master `:8080` free | **PASS** | no listener on `:8080` |
| Legacy worker container absent | **PASS** | `velox-worker-worker_523925` absent |
| Legacy worker unit absent | **FAIL** | `/etc/systemd/system/velox-worker-worker_523925.service` remains present, inactive/dead |
| Canonical worker unit active | **FAIL** | `velox-worker.service` is `activating/auto-restart`, `MainPID=0` |
| Canonical container healthy | **FAIL** | canonical container is not running/healthy |
| Canonical image digest | **PASS** | `ghcr.io/marcuss-ops/velox-worker@sha256:3631d2716f20e2cd72b3b612ff4ef8d26346fbb7a6c45f4d8dbc19512e5bc1bd` |
| Canonical env/state paths | **PASS** | canonical `worker.env`, state root and work directory are present with configured values |
| Migration manifest/marker | **FAIL — ABSENT** | `/var/lib/velox-worker/migration/{manifest.json,completed}` absent in the probe |
| Master canonical worker | **FAIL** | `velox-worker-523925eb`: `DISCONNECTED`, `session_active=false`, heartbeat age `12992s`, `0` executors |
| Master fleet gate | **FAIL** | `2/4` canonical workers are `CONNECTED` with `session_active=true` |
| Legacy Master backup | **PASS** | `/root/velox-legacy-backup/20260807T121843Z` retained |
| Master state archive | **PASS** | `velox-master-state-20260807T122825Z.tar.gz`, gzip-valid, SHA-256 `8aa99db65257e50e12c469ec23031b29da8a9d58badf72bd6b4c5d3ceb9cb414` |
| Chronon3d `:8000` | **PASS** | HTTP `200`, `text/html; charset=utf-8`, `458` bytes |

Master API probe returned HTTP `200`. The target legacy identity
`velox_worker_51_222_204_158` was not accepted as a substitute for the
canonical identity; the canonical `velox-worker-523925eb` remains
`DISCONNECTED` with no active session.

### Required blockers before certification

1. Converge `velox-worker-523925eb` onto an active, healthy canonical runtime.
2. Confirm the canonical worker reconnects with `CONNECTED` and
   `session_active=true` and a fresh heartbeat.
3. Reach the fleet gate of `4/4` canonical workers connected with active
   sessions; the current result is `2/4`.
4. Remove the remaining legacy worker unit only after the canonical cutover
   is proven and its unit is backed up.
5. Re-run this end-to-end certification and update the evidence snapshot.

No cleanup or deletion is authorized by this report. The legacy worker unit
must not be treated as removed merely because its container is absent.

## Repository evidence policy

This report contains metadata and outcomes only. It contains no token,
password, private key, certificate body, credential, session ID, or secret
value. Pre-existing worktree changes were not staged or included in the
certification commit.
