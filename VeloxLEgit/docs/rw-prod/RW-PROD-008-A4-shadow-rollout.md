# RW-PROD-008 A4 — Shadow-mode rollout runbook (ffprobe invariant)

**Priority:** P0
**Scope:** Velox master host (worker port deferred — see Stage 3)
**Owner:** ops / on-call
**Tripwire code commit:** `6759969` (deploy surface) + the upcoming
shadow-mode commit (parseFFProbeMode, structured logs, install-server.sh
tri-state preflight)

Catches the canonical C++-engine `amix-collapse` regression: a
6-voice payload collapsing to a 1-track READY row because the
master-mix step counted only the surviving track. The pre-commit
ffprobe gate fires BEFORE the CAS RECEIVED → FINALIZING transition,
so a trip leaves the artifact in STAGING with no orphan tx state.

This runbook walks the **two-stage rollout** (master goes from
shadow → enforce) the operator is performing tonight.

---

## 0. Tri-state semantics (one screen, before you start)

The master reads `VELOX_FFPROBE_VERIFY_ON_FINALIZE` and parses it
into a closed enum via `parseFFProbeMode()` in
`DataServer/internal/artifacts/service_finalize_ffprobe.go`:

| Env literal      | Mode       | Action on trip                                             |
| :--------------- | :--------- | :--------------------------------------------------------- |
| `shadow`         | **Shadow** | Logs structured `[TRIPWIRE] event=…` event. **Returns nil** to the orchestrator. Production-safe. Master keeps producing `SUCCEEDED`. |
| `enforce`        | **Enforce**| Logs + **returns** `ErrFFProbeAudioCountMismatch` (count off) or `ErrFFProbeInvariantMissingBinary` (PATH miss). Orchestrator aborts the finalize tx cleanly. |
| `true`           | **Enforce**| Legacy alias for `enforce`. Env files shipped before the tri-state keep their hard-trip behavior. |
| unset / `off` / `1` / `TRUE` / `TRUE ` (trailing space) / typos | **Off** | No-op. Strict case-sensitive match — `True` / ` Enable` / `shadow ` do NOT enable. The preflight and the Go gate share this set verbatim so an operator typo disables visibly rather than silently. |

`systemd EnvironmentFile=` canonical forms are accepted by both
the Go gate and the `install-server.sh` preflight: `=VAL`,
`="VAL"`, `='VAL'`, with optional inline-`#`-comment after a
whitespace separator.

---

## 1. Pre-flight (do this BEFORE editing /etc/velox-server.env)

1. **ffprobe on master** — verify it's installed and on PATH:
   ```bash
   command -v ffprobe || { echo 'install ffmpeg first'; \
     sudo apt-get install -y ffmpeg; }   # or distro equivalent (dnf, apk, etc.)
   ```
   The pre-flight `install-server.sh` step (already wired as Step 4a) hard-fails the install if the env file enables any active trip mode and `ffprobe` is missing — but verify by hand first.

2. **No master unit unattended** — confirm the master is at the
   shadow-rollout commit (`6759969` + the upcoming shadow-mode commit).
   Both are needed: `6759969` ships the deploy surface; the shadow-mode
   commit ships the tri-state parseFFProbeMode and the structured log
   events the runbook greps for.

3. **Audit verdict** — `bash deploy/validate-master-env.sh /etc/velox-server.env`
   must pass (rc=0). The installer runs this anyway; rerunning by hand
   surfaces hard-fail rules without restarting systemd.

---

## 2. Stage 1 — enable shadow mode (now)

Goal: capture a 24h clean signal showing the tripwire fires on the
right things (count mismatches) and is silent on healthy traffic
(1:1 plan:audio ratio).

### 2.0 Risk surface — read this first (operator's responsibility)

Shadow mode is **observational, not interventional**. During the
24h window:

- A real amix-collapse regression (e.g. 6-voice payload collapsing
  to 1-track) WILL still be processed by the master. The artifact
  row WILL land as `artifacts.status='READY'` and the per-job
  delivery rows WILL be stamped — with the wrong audio stream
  count. The trip is **loud in journalctl**, **silent on the wire**.
- The expected number of defective `READY` rows in the 24h window
  is bounded only by the prevalence of the underlying regression.
  In a healthy fleet, that number is zero; in a fleet with the C++
  amix-collapse bug reintroduced, it can grow rapidly.
- The tripwire is a **detection** tool here, not a **mitigation**.
  Mitigation downstream is the Reconciler (24h sweep orphans any
  artifact whose `verified_at > 24h ago` but whose job is not
  SUCCEEDED) + the per-job resubmit policy. Neither of those moves
  during Stage 1.
- Operators MUST understand this trade-off: if the C++ engine is
  actively regressing, you WILL produce defective READYs for the
  duration of the shadow window. The 24h window's calibration
  signal is the count of `event=ffprobe_invariant_mismatch` log
  lines — not the absence of defective artifacts.

Implication: if the master is currently suspected of having an
amix-collapse regression, do NOT enable Stage 1 shadow first —
fix the engine, or skip straight to Stage 2 enforce (a count
mismatch in Stage 2 trips and the orphan-blob pattern keeps the
artifact out of `READY` until the C++ engine is fixed).

1. **Edit** `/etc/velox-server.env` (only place the gate reads):
   ```diff
   - # VELOX_FFPROBE_VERIFY_ON_FINALIZE=shadow
   + VELOX_FFPROBE_VERIFY_ON_FINALIZE=shadow
   ```
   Note: `shadow` (lowercase, no quotes, no trailing whitespace).

2. **Re-run the installer** to clear the preflight:
   ```bash
   sudo ./deploy/install-server.sh
   ```
   Step 4a logs `[INSTALL] VELOX_FFPROBE_VERIFY_ON_FINALIZE=shadow detected. Verifying ffprobe on PATH...` then `[  OK] ffprobe binary found at /usr/bin/ffprobe` (or wherever).

3. **Restart** so systemd reseats the env:
   ```bash
   sudo systemctl restart velox-server
   ```

4. **Confirm the gate is wired** — after restart, mode shadow is
   loaded and the gate logs every Finalize (including matches).
   First verification:
   ```bash
   sudo journalctl -u velox-server -n 200 -f | grep '\[TRIPWIRE\]'
   ```
   You should see the stream of `[TRIPWIRE] event=ffprobe_invariant_match mode=shadow job_id=…`
   lines on healthy traffic.

---

## 3. Stage 1 — 24h monitoring

Run these queries against `journalctl` (or whichever log aggregator
is in front of the master — the field names are stable):

### 3.1 Tally + filter by event class

```bash
# Match events on healthy traffic (the gate ran + saw count match):
sudo journalctl -u velox-server --since "24 hours ago" \
  | grep 'event=ffprobe_invariant_match' | wc -l

# Real data regressions (catches amix-collapse):
sudo journalctl -u velox-server --since "24 hours ago" \
  | grep 'event=ffprobe_invariant_mismatch' | wc -l

# Infra miss on master (the gate's PATH lookup failed):
sudo journalctl -u velox-server --since "24 hours ago" \
  | grep 'event=ffprobe_invariant_missing_binary' | wc -l

# Orchestrator-bug class (empty jobID / absBlob / stat err / counter err):
sudo journalctl -u velox-server --since "24 hours ago" \
  | grep -E 'event=ffprobe_invariant_(orchestrator_bug|counter_error)' | wc -l
```

### 3.2 Inspect a specific trip

```bash
# Last 10 mismatch events with structured fields:
sudo journalctl -u velox-server --since "24 hours ago" \
  | grep 'event=ffprobe_invariant_mismatch' | tail -10
```

Sample line shape:
```
[TRIPWIRE] event=ffprobe_invariant_mismatch mode=shadow job_id=J-AMIX-1 \
  expected_streams=6 actual_streams=1 override_dest_id="" \
  blob=/var/lib/velox/data/final/artifacts/sha256/ab/abcd…ef.mp4
```

### 3.3 Exit criteria for promoting to Stage 2

| Event class                            | 24h threshold | Action                                                                                |
| :------------------------------------- | :------------ | :------------------------------------------------------------------------------------ |
| `event=ffprobe_invariant_match`        | > 0           | Gate ran on real traffic. Healthy signal.                                             |
| `event=ffprobe_invariant_mismatch`     | 0             | No regressions detected → safe to flip to **Stage 2 enforce**.                        |
| `event=ffprobe_invariant_mismatch`     | > 0           | **DO NOT** promote. Open a regression ticket with the `expected_streams` / `actual_streams` pairs; investigate the C++ engine's master-mix step before flipping to abort.        |
| `event=ffprobe_invariant_missing_binary` | any         | ffprobe disappeared mid-window — check `apt` / container image, audit why, fix root cause, then decide Stage 2. |
| `event=ffprobe_invariant_orchestrator_bug` | any       | Bugs in Finalize callers (empty fields) — fix in `artifacts.Service` callers, NOT a Stage 2 promotion blocker. |
| `event=ffprobe_invariant_counter_error` | any        | The gate's per-job plan query failed. Investigate master DB health before promoting. |

---

## 4. Stage 2 — promote to enforce (later)

Once the 24h shadow window is **clean** (the table in §3.3 above),
flip the SAME master to enforce so upcoming regressions trip
loudly instead of going silent in logs:

1. **Edit** `/etc/velox-server.env`:
   ```diff
   - VELOX_FFPROBE_VERIFY_ON_FINALIZE=shadow
   + VELOX_FFPROBE_VERIFY_ON_FINALIZE=enforce
   ```

2. **Re-run the installer** for the preflight:
   ```bash
   sudo ./deploy/install-server.sh
   ```

3. **Restart**:
   ```bash
   sudo systemctl restart velox-server
   ```

4. **Confirm the trip behaviour** — re-run §3 queries; from Stage 2
   onward `event=ffprobe_invariant_match` is silent (the gate is
   no-op on healthy traffic so as not to spam logs with redundant
   "match" lines that operators no longer need to confirm). Count
   mismatches become **production aborts** — every regression
   produces an `artifact_status=STAGING` orphan blob (the 24h
   Reconciler sweep reclaims).

Stage 2 is a one-line edit; the operator can flip back to `shadow`
at any time without losing jobs.

---

## 5. Kill-switch / rollback

Anywhere on the master, immediate stop:

```bash
# Comment out the env file line:
sudo sed -i 's/^VELOX_FFPROBE_VERIFY_ON_FINALIZE=/# VELOX_FFPROBE_VERIFY_ON_FINALIZE=/' /etc/velox-server.env
sudo systemctl restart velox-server
```

The gate returns to `parseFFProbeMode() == Off` on the very next
Finalize. No data loss, no schema migration — the env variable is
purely operational.

Alternative rollback paths:
- Set the env value to `off` (explicit no-op, equivalent to commenting out).
- Replace the value with a typo (defeats the strict-literal parser — useful for staged rollouts where you want to prove the parseFFProbeMode fence).
- Revert the systemd unit file: the env file is the source of truth, no unit-level config involved.

---

## 6. Stage 3 — worker port (DEFERRED, not tonight)

A future incarnation of the tripwire may move the gate to the
worker side so the worker invokes ffprobe on its rendered artifact
BEFORE uploading to the master (i.e. a worker-local pre-upload
check). This deferral is explicit in the design memo / scope; the
master-side gate is sufficient for the canonical amix-collapse
class until that future work lands.

Implications of a worker-side port when it lands:
- `t.Setenv` and `predflight_ffprobe_invariant` need a parallel
  worker-side equivalent (`runtime/worker.env.example`,
  `prepare-host.sh`).
- The structured log events use `worker_id=` instead of `job_id=`
  (or alongside) so log aggregation queries can route per-host.
- Workers need `ffmpeg` installed (currently they do not).

Until Stage 3 lands, **do not install ffmpeg on worker hosts** —
no tripwire on the worker side means the binary would be unused
weight.

---

## 7. Reference — log event shape

The table below is the canonical grep contract. New event classes
MUST be added per the column order (mode first, then context keys)
to keep parsers stable.

| Event class                              | Mode field | Schema (canonical field order)                                                                                |
| :--------------------------------------- | :--------- | :------------------------------------------------------------------------------------------------------------ |
| `event=ffprobe_invariant_match`          | `shadow`   | `mode=shadow job_id=<JID> expected_streams=<N> actual_streams=<M> blob=<abs-path>`                              |
| `event=ffprobe_invariant_mismatch`       | both       | `mode=<shadow\|enforce> job_id=<JID> expected_streams=<N> actual_streams=<M> override_dest_id="<DID-or-empty>" blob=<abs-path>` |
| `event=ffprobe_invariant_mismatch`       | both       | (alt: ffprobe exec failure) `mode=<shadow\|enforce> job_id=<JID> reason=ffprobe_exec err="<exec stderr>"`        |
| `event=ffprobe_invariant_missing_binary` | both       | `mode=<shadow\|enforce> job_id=<JID> err="<exec.LookPath stderr>"`                                              |
| `event=ffprobe_invariant_orchestrator_bug` | both     | `mode=<shadow\|enforce> reason=<empty_job_id\|empty_abs_blob\|stat_blob_failed> [job_id=<JID>] [blob=<path>] err="<stat err>"` |
| `event=ffprobe_invariant_counter_error`  | both       | `mode=<shadow\|enforce> job_id=<JID> err="<CountExpectedDeliveries err>"`                                       |

All events are prefixed with `[TRIPWIRE]` so dashboard / alert
plumbing can filter on the tag without parsing structured fields.
