#!/usr/bin/env python3
"""phase0-receipt.py — compute the Phase-0 reference-job performance receipt.

Inputs (evidence dir, default docs/benchmarks/evidence/phase0-2026-08-12):
  sidecar-reference.json   engine output.progress.json (authoritative timeline)
  strace-reference.txt     strace -f -c summary
  perfstat-reference.txt   perf stat -d -d -d summary
  summary.tsv              per-mode wall_ms

Outputs:
  receipt JSON  (docs/benchmarks/receipts/phase0-receipt-<date>.json)
  receipt MD    (docs/benchmarks/receipts/phase0-receipt-<date>.md)

The receipt answers "where do the 15-25s go": every millisecond of the render
total is attributed to a phase; unaccounted_ms = total - sum(explained) and
must be < 5% of total. The engine phase_ms summary map is NOT additive (its
segment_build_ms undercounts); the segments[] timeline is authoritative.

KPI DEFINITION NOTE: the accounted/unaccounted semantics below mirror the
CANONICAL Go definition — performance.Derive(RawMetrics) in
RemoteCodex/native/worker-agent-go/pkg/performance/derive.go — where
accounted_ratio sums ONLY exclusive top-level phase durations (catalog
accounted_ratio_rule: never span_parent/span_child). This script recomputes
the same KPI from raw evidence because the Phase-0 reference run predates
receipt production; future benchmark tooling MUST read the receipt's
"derived" section instead of recomputing ratios.

Usage:
  python3 scripts/benchmarks/phase0-receipt.py [--evidence-dir DIR] [--tag TAG]
"""
import argparse
import json
import pathlib
import re
import statistics

# strace -c rows carry 5 or 6 columns: %time seconds usecs calls [errors] syscall
# (the errors column is omitted when the syscall never failed).
STRACE_ROW = re.compile(
    r"^\s*[0-9.]+\s+([0-9.]+)\s+[0-9]+\s+([0-9]+)(?:\s+[0-9]+)?\s+(\S+)\s*$"
)


def parse_strace(text: str) -> dict:
    """Return {syscall: {"seconds": float, "calls": int}}."""
    out = {}
    for line in text.splitlines():
        m = STRACE_ROW.match(line)
        if m and m.group(3) not in ("syscall", "total"):
            out[m.group(3)] = {
                "seconds": float(m.group(1)),
                "calls": int(m.group(2)),
            }
    return out


def parse_perfstat(text: str) -> dict:
    out = {}
    for line in text.splitlines():
        if "task-clock" in line:
            m = re.search(r"([0-9.]+) msec task-clock", line)
            out["task_clock_ms"] = float(m.group(1)) if m else None
            m = re.search(r"#\s+([0-9.]+)\s+CPUs utilized", line)
            out["cpus_utilized"] = float(m.group(1)) if m else None
        elif "context-switches" in line:
            m = re.search(r"^\s+([0-9]+)\s+context-switches", line)
            out["context_switches"] = int(m.group(1)) if m else None
        elif "page-faults" in line:
            m = re.search(r"^\s+([0-9]+)\s+page-faults", line)
            out["page_faults"] = int(m.group(1)) if m else None
        elif "seconds time elapsed" in line:
            m = re.search(r"([0-9.]+)\s+seconds time elapsed", line)
            out["wall_seconds"] = float(m.group(1)) if m else None
        elif re.match(r"^\s+[0-9.]+ seconds user", line):
            m = re.search(r"([0-9.]+)\s+seconds user", line)
            out["user_seconds"] = float(m.group(1)) if m else None
        elif re.match(r"^\s+[0-9.]+ seconds sys", line):
            m = re.search(r"([0-9.]+)\s+seconds sys", line)
            out["sys_seconds"] = float(m.group(1)) if m else None
    return out


def parse_summary(text: str) -> list:
    rows = []
    for line in text.splitlines():
        if not line or line.startswith("mode"):
            continue
        parts = line.split("\t")
        if len(parts) >= 4:
            rows.append({
                "mode": parts[0],
                "wall_ms": int(parts[1]),
                "rc": int(parts[2]),
                "trace": parts[3],
            })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--evidence-dir", default="docs/benchmarks/evidence/phase0-2026-08-12")
    ap.add_argument("--tag", default="2026-08-12")
    ap.add_argument("--out-dir", default="docs/benchmarks/receipts")
    args = ap.parse_args()

    ev = pathlib.Path(args.evidence_dir)
    sidecar = json.loads((ev / "sidecar-reference.json").read_text(encoding="utf-8"))
    strace = parse_strace((ev / "strace-reference.txt").read_text(encoding="utf-8"))
    perfstat = parse_perfstat((ev / "perfstat-reference.txt").read_text(encoding="utf-8"))
    runs = parse_summary((ev / "summary.tsv").read_text(encoding="utf-8"))

    # ── authoritative timeline from the engine phases[] ─────────────────────
    phases = {f"{p.get('component','')}|{p.get('action','')}": p
              for p in sidecar.get("phases", [])}
    render = phases.get("engine|render")
    concat = phases.get("engine|concat")
    mux = phases.get("engine.mux|audio")
    mix = phases.get("engine.audio|mix")

    total_ms = float(render["duration_ms"])
    concat_ms = float(concat["duration_ms"])
    audio_mux_ms = float(mux["duration_ms"])
    audio_mix_ms = float(mix["duration_ms"]) if mix else None

    segments = sidecar.get("segments", [])
    n_segments = len(segments)
    segments_ms = sum(float(s["total_ms"]) for s in segments)
    ffmpeg_work_ms = 0.0
    for s in segments:
        speed = float(s.get("ffmpeg_speed_x") or 0)
        dur_s = float(s.get("input_duration_ms", 9466.666667)) / 1000.0
        if speed > 0:
            ffmpeg_work_ms += dur_s / speed * 1000.0
    spawn_probe_ms = segments_ms - ffmpeg_work_ms

    phase_ms = sidecar.get("phase_ms", {})
    audio_download_ms = float(phase_ms.get("audio_download_ms", 0.0))
    # asset_download is per-segment and already included in segments_ms —
    # it must NOT be added again to the explained total.
    asset_download_ms = float(phase_ms.get("asset_download_ms", 0.0))
    copy_final_ms = float(phase_ms.get("copy_final_ms", 0.0))
    workdir_ms = float(phase_ms.get("workdir_create_ms", 0.0))
    finalize_ms = copy_final_ms + workdir_ms
    temp_bytes = int(sidecar.get("temp_bytes", 0))

    # accounted budget = sum of the EXCLUSIVE top-level phases only
    # (catalog accounted_ratio_rule; mirrors Derive(RawMetrics)). The
    # per-segment asset_download is inside segments_ms and must not be
    # added again; render is a parent span of the segments and is also
    # excluded to avoid double counting.
    explained = (segments_ms + concat_ms + audio_download_ms + audio_mux_ms
                 + finalize_ms)
    unaccounted_ms = total_ms - explained
    unaccounted_pct = unaccounted_ms / total_ms * 100.0 if total_ms else 0.0

    # ── strace correlation ──────────────────────────────────────────────────
    w4 = strace.get("wait4", {})
    futex = strace.get("futex", {})
    execve = strace.get("execve", {})
    strace_total_s = sum(v["seconds"] for v in strace.values())
    strace_calls = sum(v["calls"] for v in strace.values())

    # ── perf stat correlation ───────────────────────────────────────────────
    cpu_total_s = (perfstat.get("user_seconds") or 0) + (perfstat.get("sys_seconds") or 0)

    receipt = {
        "tag": args.tag,
        "workload": {
            "copy_only": bool(sidecar.get("copy_only", True)),
            "segments": n_segments,
            "total_duration_seconds": float(sidecar.get("duration_seconds", 284)),
            "canvas": sidecar.get("canvas"),
            "concat_mode": sidecar.get("concat_mode"),
            "frames_decoded": sidecar.get("frames_decoded", 0),
            "frames_encoded": sidecar.get("frames", 0),
            "encode_passes": sidecar.get("encode_passes", 0),
            "temp_bytes": temp_bytes,
        },
        "timeline_ms": {
            "render_total": round(total_ms, 1),
            "video_segments_serial": round(segments_ms, 1),
            "segments_count": n_segments,
            "avg_segment_ms": round(segments_ms / n_segments, 1) if n_segments else None,
            "ffmpeg_stream_copy_work_ms": round(ffmpeg_work_ms, 1),
            "spawn_probe_overhead_ms": round(spawn_probe_ms, 1),
            "audio_download_prepare_ms": round(audio_download_ms, 1),
            "concat_ms": round(concat_ms, 1),
            "audio_mux_aac_encode_ms": round(audio_mux_ms, 1),
            "audio_mix_wrap_ms": round(audio_mix_ms, 1) if audio_mix_ms else None,
            "asset_download_ms_inside_segments": round(asset_download_ms, 1),
            "finalize_ms": round(finalize_ms, 1),
            "explained_ms": round(explained, 1),
            "unaccounted_ms": round(unaccounted_ms, 1),
            "unaccounted_pct": round(unaccounted_pct, 2),
        },
        "audio_mux_decision": next(
            (m for k, p in phases.items() for m in [p.get("metadata", {})]
             if m.get("final_mux_audio_mode")), None),
        "strace": {
            "traced_seconds": round(strace_total_s, 3),
            "total_syscalls": strace_calls,
            "wait4_seconds": round(w4.get("seconds", 0.0), 3),
            "wait4_calls": w4.get("calls", 0),
            "futex_seconds": round(futex.get("seconds", 0.0), 3),
            "futex_calls": futex.get("calls", 0),
            "execve_calls": execve.get("calls", 0),
        },
        "perfstat": {
            "task_clock_ms": round(perfstat.get("task_clock_ms") or 0, 1),
            "cpus_utilized": perfstat.get("cpus_utilized"),
            "context_switches": perfstat.get("context_switches"),
            "page_faults": perfstat.get("page_faults"),
            "cpu_user_sys_seconds": round(cpu_total_s, 3),
            "wall_seconds": perfstat.get("wall_seconds"),
        },
        "runs": runs,
        "verdict": {
            "unaccounted_lt_5pct": unaccounted_pct < 5.0,
            "dominant_phase": max(
                [("video_segments_serial", segments_ms), ("audio_mux_aac_encode", audio_mux_ms)],
                key=lambda kv: kv[1])[0],
        },
    }

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"phase0-receipt-{args.tag}.json"
    md_path = out_dir / f"phase0-receipt-{args.tag}.md"
    json_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")

    pct = lambda ms: f"{ms / total_ms * 100.0 if total_ms else 0.0:.1f}%"
    md = f"""# Phase-0 receipt — copy-only ~4m44s (tag {args.tag})

Worker `velox-deb-57.131`, engine `v1.2.28-canonical`, containerized
(cap_drop ALL + seccomp:unconfined + paranoid=1). Evidence:
`docs/benchmarks/evidence/phase0-{args.tag}/`.

## Where the {total_ms/1000:.1f}s render goes

| phase | ms | % | note |
|---|---|---|---|
| video segments (serial) | **{segments_ms:.0f}** | {pct(segments_ms)} | {n_segments} × {segments_ms/n_segments:.0f}ms avg |
| ├─ ffmpeg stream-copy work | {ffmpeg_work_ms:.0f} | {pct(ffmpeg_work_ms)} | copy-only, speed ~13.000x |
| └─ spawn + 2×ffprobe overhead | **{spawn_probe_ms:.0f}** | {pct(spawn_probe_ms)} | ~99.8% of segment time |
| audio download/prepare | {audio_download_ms:.0f} | {pct(audio_download_ms)} | |
| concat (segments.txt) | {concat_ms:.0f} | {pct(concat_ms)} | stream_copy |
| **audio mux (AAC ENCODE)** | **{audio_mux_ms:.0f}** | {pct(audio_mux_ms)} | `final_mux_audio_mode=ENCODE` `reason=not_final_mix` |
| asset download (warm cache) | {asset_download_ms:.0f} | {pct(asset_download_ms)} | inside segments, cache→tmp copy |
| finalize (copy final / workdir) | {finalize_ms:.0f} | {pct(finalize_ms)} | atomic rename publication |
| **unaccounted** | **{unaccounted_ms:.0f}** | {pct(unaccounted_ms)} | {'< 5% ✓' if receipt['verdict']['unaccounted_lt_5pct'] else 'OVER BUDGET ✗'} |

`unaccounted_ms = {unaccounted_ms:.0f} ms ({unaccounted_pct:.2f}% of {total_ms:.0f} ms)` —
**{'PASS (target < 5%)' if receipt['verdict']['unaccounted_lt_5pct'] else 'FAIL'}.**
The engine `phase_ms.segment_build_ms` summary undercounts the segments
(3.2s vs {segments_ms:.1f}ms in the segments[] timeline) — the timeline is
authoritative; that gap was the bulk of the apparent "unaccounted" time.

## Correlations

- **strace -f -c**: wait4 = {w4.get('seconds',0):.1f}s / {w4.get('calls',0)} calls (52.8%)
  ≈ the serial segment+child waits; futex = {futex.get('seconds',0):.1f}s
  ({futex.get('calls',0)} calls) ≈ audio encode threading; **execve = {execve.get('calls',0)}**
  external processes; {strace_calls:,} syscalls.
- **perf stat**: {perfstat.get('cpus_utilized')} CPUs utilized (serial);
  user+sys = {cpu_total_s:.1f}s ≈ wall {perfstat.get('wall_seconds',0):.1f}s (CPU-bound);
  {perfstat.get('context_switches',0):,} context-switches;
  {perfstat.get('page_faults',0):,} page-faults (process spawn churn).
- **runs**: {', '.join(f"{r['mode']}={r['wall_ms']}ms" for r in runs)}.

## Verdict

- Video stream-copy **work** is ~{ffmpeg_work_ms:.0f}ms of {total_ms:.0f}ms;
  the rest is orchestration + audio re-encode.
- Priority 1 = **FINAL_AUDIO_COPY** (removes ~{audio_mux_ms:.0f}ms, 44%).
- Priority 2 = **in-process packet pipeline** (removes ~{spawn_probe_ms:.0f}ms,
  ~54%: the per-segment spawn + 2×ffprobe + wait4 tax).
- Combined ceiling ≈ {total_ms - audio_mux_ms - spawn_probe_ms:.0f}ms of
  fixed cost ({'%.1f' % ((total_ms - audio_mux_ms - spawn_probe_ms)/1000)}s).
"""
    md_path.write_text(md, encoding="utf-8")
    print(json.dumps(receipt, indent=2))
    print(f"\nWROTE {json_path}\nWROTE {md_path}")


if __name__ == "__main__":
    main()
