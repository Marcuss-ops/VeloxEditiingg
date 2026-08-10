#!/usr/bin/env python3
"""Compute the deployment fingerprint for rendered worker configuration."""

from __future__ import annotations

import hashlib
import sys


def fingerprint(config_path: str, compose_path: str = "", image_digest: str = "") -> str:
    with open(config_path, "rb") as config_file:
        data = config_file.read()

    # compose.yml is optional; a missing host copy is intentionally ignored.
    if compose_path:
        try:
            with open(compose_path, "rb") as compose_file:
                data += compose_file.read()
        except FileNotFoundError:
            pass

    # image_digest is optional and may be empty when Docker/image inspection fails.
    if image_digest:
        data += image_digest.encode()

    return hashlib.sha256(data).hexdigest()


def main() -> None:
    _program, config_path, *optional = sys.argv
    compose_path = optional[0] if len(optional) > 0 else ""
    image_digest = optional[1] if len(optional) > 1 else ""
    print(fingerprint(config_path, compose_path, image_digest))


if __name__ == "__main__":
    main()
