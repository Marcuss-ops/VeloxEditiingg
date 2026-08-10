#!/usr/bin/env python3
"""Render the runtime worker configuration from the operator template."""

from __future__ import annotations

import json
import sys


def render(
    src: str,
    dst: str,
    worker_id: str,
    worker_name: str,
    control_grpc_url: str,
    master_url: str,
    work_dir: str,
    health_port: int,
    protocol_version: str,
    bundle_version: str,
    bundle_hash: str,
    image_digest: str,
    allow_insecure: bool,
) -> None:
    with open(src) as source_file:
        cfg = json.load(source_file)

    # Strip operator-side documentation keys (prefix _) so runtime JSON is clean.
    cfg = {key: value for key, value in cfg.items() if not key.startswith("_")}

    # Operator-supplied fields — ALWAYS overwrite from flags.
    cfg["worker_id"] = worker_id
    cfg["worker_name"] = worker_name
    cfg["control_grpc_url"] = control_grpc_url
    cfg["master_url"] = master_url
    cfg["work_dir"] = work_dir
    cfg["health_port"] = health_port
    cfg["protocol_version"] = protocol_version
    cfg.setdefault("log_level", "info")

    # Optional overrides — only write when explicitly non-empty / non-false.
    if bundle_version:
        cfg["bundle_version"] = bundle_version
    elif "bundle_version" in cfg and cfg["bundle_version"] == "":
        pass  # keep empty; runtime fills from env/ldflags/VERSION.txt
    if bundle_hash:
        cfg["bundle_hash"] = bundle_hash
    elif "bundle_hash" in cfg and cfg["bundle_hash"] == "":
        pass  # keep empty; runtime fills from VELOX_BUNDLE_HASH / BUNDLE_HASH.txt

    if image_digest:
        cfg["image_digest"] = image_digest

    if allow_insecure:
        cfg["allow_insecure_grpc_dev"] = True
    else:
        cfg["allow_insecure_grpc_dev"] = False

    # Schema sanity defaults. HTTP polling-era keys were dropped in PR3 final;
    # the worker is gRPC-push only.
    cfg.setdefault("max_active_jobs", 1)
    # Prometheus is enabled by default so worker cache metrics are scrapeable.
    cfg.setdefault("prometheus_port", 9090)

    with open(dst, "w") as destination_file:
        json.dump(cfg, destination_file, indent=2, sort_keys=False)


def main() -> None:
    (
        _program,
        src,
        dst,
        worker_id,
        worker_name,
        control_grpc_url,
        master_url,
        work_dir,
        health_port,
        protocol_version,
        bundle_version,
        bundle_hash,
        image_digest,
        allow_insecure,
    ) = sys.argv
    render(
        src,
        dst,
        worker_id,
        worker_name,
        control_grpc_url,
        master_url,
        work_dir,
        int(health_port),
        protocol_version,
        bundle_version,
        bundle_hash,
        image_digest,
        allow_insecure.lower() == "true",
    )


if __name__ == "__main__":
    main()
