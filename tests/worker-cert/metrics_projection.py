#!/usr/bin/env python3
"""Worker-scoped Prometheus projection for the parallelism certification.

parallel_bench.py consumes a single metrics endpoint whose series are
expressed as UNLABELLED base names for the resource gauges and the
cache counters (see test_parallel_bench.py), while:

  * the worker telemetry emits every series with a static label
    ({label="asset"}, {label="total"}, ...);
  * the master projects per-worker resource gauges labelled with
    worker_id=... (cpu/iowait in micro/milli units).

This adapter is a presentation-layer projection only — it never changes
a worker, the master, or any telemetry counter. On each /metrics scrape
it:

  1. scrapes the worker /metrics (cache counters: requests/downloads/
     duplicates/errors);
  2. scrapes the master /metrics and selects ONLY the certification
     worker's series (worker_id=...);
  3. rewrites the selected series into the harness-expected shape,
     normalizing the master's micro-unit (cpu x1e-6) and milli-unit
     (iowait x1e-3) scales back to ratios in [0,1];
  4. serves the merged exposition text on a local port.

Usage:

  TOKEN_FILE=/path/velox-token.env python3 tests/worker-cert/metrics_projection.py \
    --worker-id velox-worker-local \
    --worker-metrics http://127.0.0.1:9090/metrics \
    --master-metrics http://127.0.0.1:8000/metrics \
    --port 9101

  # one-shot output (no server):
  ... --print
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional

# Families scraped from the WORKER whose static {label=...} is stripped
# to the unlabelled base name the harness looks up.
WORKER_UNLABEL_FAMILIES = {
    "velox_cache_downloads_total",
    "velox_cache_download_bytes_total",
    "velox_cache_duplicate_downloads_total",
    "velox_cache_duplicate_download_bytes_total",
    "velox_worker_errors_total",
}

# Families scraped from the MASTER, selected by worker_id=..., with the
# label stripped and (for ratio gauges) unit normalization applied.
MASTER_SELECT_FAMILIES = {
    "velox_worker_cpu_utilization_ratio": "1e-6",  # micro-units -> ratio
    "velox_worker_cpu_iowait_ratio": "1e-3",       # milli-units -> ratio
    "velox_worker_process_rss_bytes": None,        # raw bytes
}


def read_token(token_file: Optional[str]) -> str:
    value = os.getenv("VELOX_ADMIN_TOKEN", "").strip()
    if not value and token_file:
        path = Path(token_file)
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("VELOX_ADMIN_TOKEN="):
                    value = line.split("=", 1)[1].strip().strip("'\"")
                    break
    return value


def scrape(url: str, token: str) -> str:
    headers = {"Accept": "text/plain"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.read().decode("utf-8", errors="replace")


def _fmt(value: float) -> str:
    """Render a projected value without scientific notation."""
    if value.is_integer():
        return str(int(value))
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return text


def _normalize(value: float, unit: Optional[str]) -> float:
    if unit == "1e-6":
        # Master emits micro-units (e.g. 965952 = 0.965952). Defensive:
        # already-ratio values are passed through.
        return value / 1_000_000.0 if value > 100 else value
    if unit == "1e-3":
        # Master emits milli-units (e.g. 252 = 0.252).
        return value / 1000.0 if value > 1 else value
    return value


def project(worker_text: str, master_text: str, worker_id: str) -> str:
    """Rewrite the two scrapes into the harness-expected exposition."""
    lines: list[str] = []

    for raw in worker_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            lines.append(line)
            continue
        if " " not in line:
            continue
        name, _rest = line.split(None, 1)
        base = name.split("{", 1)[0]
        if base in WORKER_UNLABEL_FAMILIES:
            # velox_cache_downloads_total{label="asset"} -> unlabelled.
            # velox_cache_requests_total{result="hit"} is kept verbatim
            # (the harness addresses cache hit/miss by result label).
            if "result=" in name:
                lines.append(line)
            else:
                lines.append(f"{base} {line.split(None, 1)[1]}")
        else:
            lines.append(line)

    for raw in master_text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or " " not in line:
            continue
        name, rest = line.split(None, 1)
        base = name.split("{", 1)[0]
        if base not in MASTER_SELECT_FAMILIES:
            continue
        # Only the certification worker's series.
        if f'worker_id="{worker_id}"' not in name:
            continue
        try:
            value = float(rest.split()[0])
        except ValueError:
            continue
        value = _normalize(value, MASTER_SELECT_FAMILIES[base])
        lines.append(f"{base} {_fmt(value)}")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--worker-metrics", default="http://127.0.0.1:9090/metrics")
    parser.add_argument("--master-metrics", default="http://127.0.0.1:8000/metrics")
    parser.add_argument("--port", type=int, default=9101)
    parser.add_argument("--token-file", default=os.getenv("TOKEN_FILE", ""))
    parser.add_argument("--print", action="store_true", help="one-shot output, no server")
    args = parser.parse_args()

    token = read_token(args.token_file or None)

    def current() -> str:
        worker_text = scrape(args.worker_metrics, token)
        try:
            master_text = scrape(args.master_metrics, token)
        except (urllib.error.URLError, urllib.error.HTTPError) as exc:
            print(f"[projection] master scrape unavailable ({exc}); serving worker-derived series only",
                  file=sys.stderr)
            master_text = ""
        return project(worker_text, master_text, args.worker_id)

    if args.print:
        sys.stdout.write(current())
        return 0

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (stdlib handler)
            if self.path.rstrip("/") != "/metrics":
                self.send_response(404)
                self.end_headers()
                return
            body = current().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _fmt: str, *args) -> None:  # silence request logs
            pass

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"[projection] serving worker-scoped projection for {args.worker_id} on :{args.port}/metrics",
          file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
