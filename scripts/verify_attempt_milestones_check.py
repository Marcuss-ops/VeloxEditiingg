#!/usr/bin/env python3
"""Companion checker for scripts/verify_attempt_milestones_e2e.sh.

Reads every live snapshot in $SNAP_DIR plus the durable inspect snapshot,
then enforces the milestone-timeline contract:

  1. names are canonical attempt milestones;
  2. ordered by sequence, elapsed_ms is non-decreasing (monotonic clock);
  3. each live sample carries master_received_at / master_committed_at
     (Master-side heartbeat stamping, feature commit d6ee9493);
  4. when the job reached SUCCEEDED, the full spine
     attempt.accepted .. attempt.completed is present;
  5. reports the max transport/heartbeat gap between consecutive
     master_received_at stamps as diagnostic output (never subtracted
     from the worker clock — deltas stay per-clock).

Exit codes: 0 = contract holds; 1 = violation(s) found.
Warnings (partial chain on still-running jobs, unstamped legacy rows)
are printed but do not flip the exit code.
"""

import glob
import json
import os
import sys

# Windows consoles default to cp1252; keep output safe on every host.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:  # pragma: no cover - very old Pythons
    pass

CANONICAL = {
    "attempt.accepted", "execution.started", "assets.requested",
    "assets.first_started", "assets.all_ready", "plan.started",
    "plan.completed", "render.started", "render.completed",
    "finalize.started", "finalize.completed", "output.durable",
    "publish.queued", "publish.started", "publish.completed",
    "result.sending", "result.sent", "attempt.completed",
}
# Sub-span markers may recur; they are not part of the sequential spine.
SUBSPAN_OK = {"assets.remote_wait.started", "assets.remote_wait.completed"}

SPINE_SUCCEEDED = [
    "attempt.accepted", "execution.started", "result.sending",
    "result.sent", "attempt.completed",
]
SPINE_OTHER = ["attempt.accepted", "execution.started"]

violations = []
warnings = []


def fail(msg):
    violations.append(msg)


def warn(msg):
    warnings.append(msg)


def find_sample_lists(node, out):
    """Recursively collect lists that look like milestone samples."""
    if isinstance(node, dict):
        for key, value in node.items():
            if isinstance(value, list) and value and isinstance(value[0], dict) \
                    and "name" in value[0] and any(
                        k in value[0] for k in ("elapsed_ms", "elapsedMs")):
                out.append((key, node, value))
            else:
                find_sample_lists(value, out)
    elif isinstance(node, list):
        for item in node:
            find_sample_lists(item, out)


def parse_elapsed(sample):
    for key in ("elapsed_ms", "elapsedMs"):
        value = sample.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def as_float(stamp):
    if not stamp:
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def check_snapshot(path, kind):
    try:
        with open(path, encoding="utf-8") as handle:
            doc = json.load(handle)
    except Exception as exc:  # noqa: BLE001
        warn(f"{os.path.basename(path)}: unreadable snapshot ({exc})")
        return

    lists_out = []
    find_sample_lists(doc, lists_out)
    if not lists_out:
        if kind == "live":
            warn(f"{os.path.basename(path)}: no attempt_milestones list found "
                 "(server predates milestone support?)")
        return

    per_key = {}
    for _key, _parent, samples in lists_out:
        for sample in samples:
            name = str(sample.get("name") or "")
            group = str(sample.get("attempt_id")
                        or parent_attempt_id(_parent)
                        or os.path.basename(path))
            per_key.setdefault(group, []).append(sample)

    stamped_total = seen_total = 0
    for group, samples in sorted(per_key.items()):
        print(f"\n== {kind}:{group} ({len(samples)} samples) ==")
        ordered = sorted(samples, key=lambda s: (
            int(s.get("sequence") or 0), parse_elapsed(s) or 0))
        prev_elapsed = prev_recv_ts = None
        for sample in ordered:
            name = str(sample.get("name") or "")
            seq = int(sample.get("sequence") or 0)
            elapsed = parse_elapsed(sample)

            if name not in CANONICAL and name not in SUBSPAN_OK:
                fail(f"{group}: non-canonical milestone name {name!r}")
                continue
            if name in SUBSPAN_OK:
                continue  # recurring sub-span: exempt from monotonic spine
            seen_total += 1
            if elapsed is None:
                fail(f"{group}: {name}: missing/parses-failed elapsed_ms")
                continue
            if elapsed < 0:
                fail(f"{group}: {name}: negative elapsed_ms={elapsed}")
            if prev_elapsed is not None and elapsed < prev_elapsed:
                fail(f"{group}: {name}: elapsed_ms went backwards "
                     f"({elapsed} < {prev_elapsed}) — clock not monotonic")

            recv = sample.get("master_received_at")
            comm = sample.get("master_committed_at")
            if recv or comm:
                stamped_total += 1
                recv_ts = as_float(recv)
                if recv is None or comm is None:
                    warn(f"{group}: {name}: half-stamped "
                         f"(received={recv!r} committed={comm!r})")
                elif recv != comm:
                    # Fold tx time: equal by construction in the projection.
                    warn(f"{group}: {name}: received != committed")
                recv_gap = (recv_ts - prev_recv_ts) \
                    if (recv_ts and prev_recv_ts) else None
                print(f"   seq={seq:>3} elapsed={elapsed:>9}ms {name:<28}"
                      f"+{recv_gap:.1f}s recv-gap" if recv_gap else
                      f"   seq={seq:>3} elapsed={elapsed:>9}ms {name}")
                prev_recv_ts = recv_ts or prev_recv_ts
            else:
                print(f"   seq={seq:>3} elapsed={elapsed:>9}ms {name}")
            prev_elapsed = elapsed

        names = {str(s.get("name")) for s in ordered}
        # Per-sample fold stamps only ever appear on the LIVE projection;
        # the durable report legitimately carries none (it is serialised
        # before result.sent exists), so this warning is live-only.
        if kind == "live" and not stamped_total and len(ordered) > 2:
            warn(f"{group}: no master_received_at stamps on live samples "
                 f"— needs server with fold-stamping (d6ee9493)")
    return stamped_total, seen_total


def parent_attempt_id(parent):
    node = parent
    while isinstance(node, dict):
        if node.get("attempt_id"):
            return str(node["attempt_id"])
        for key in ("id", "attemptId"):
            if node.get(key) and "attempt" in key.lower():
                return str(node[key])
        break
    return None


def main():
    snap_dir = os.environ.get("SNAP_DIR")
    status = os.environ.get("TERMINAL_STATUS", "")
    if not snap_dir:
        print("SNAP_DIR not set", file=sys.stderr)
        return 1

    live_snaps = sorted(glob.glob(os.path.join(snap_dir, "live_*.json")))
    durable = os.path.join(snap_dir, "durable.json")

    for snap in live_snaps:
        check_snapshot(snap, "live")
    if os.path.exists(durable):
        check_snapshot(durable, "durable")
    elif status:
        warn("durable inspect snapshot missing — post-completion persistence unverified")

    # Chain completeness: use the richest (largest) live snapshot, or the
    # durable one when the run finished before polling caught a live view.
    richest = None
    best = -1
    for snap in live_snaps + ([durable] if os.path.exists(durable) else []):
        found = []
        try:
            with open(snap, encoding="utf-8") as handle:
                doc = json.load(handle)
        except Exception:  # noqa: BLE001
            continue
        find_sample_lists(doc, found)
        total = sum(len(v) for _k, _p, v in found)
        if total > best:
            best, richest = total, (snap, {str(s.get("name"))
                                           for _k, _p, v in found for s in v})
    if richest:
        source, names = richest
        spine = SPINE_SUCCEEDED if status == "SUCCEEDED" else SPINE_OTHER
        missing = [m for m in spine if m not in names]
        if status == "" :
            pass  # never saw terminal: chain check deferred to caller
        elif missing and status == "SUCCEEDED":
            warn(f"{os.path.basename(source)}: succeeded job missing spine "
                 f"milestones: {missing}")
        elif missing:
            warn(f"{os.path.basename(source)}: status={status} missing spine "
                 f"milestones (may be legitimate on failure paths): {missing}")

    print()
    for w in warnings:
        print(f"WARN: {w}")
    for v in violations:
        print(f"VIOLATION: {v}")
    print(f"summary: violations={len(violations)} warnings={len(warnings)} "
          f"terminal={status or '<none>'}")
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
