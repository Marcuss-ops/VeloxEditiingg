"""HTTP client, token reading, and M2M key provisioning for capacity certification."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def read_token() -> str:
    """Read VELOX_ADMIN_TOKEN from env or TOKEN_FILE."""
    value = os.getenv("VELOX_ADMIN_TOKEN", "").strip()
    if not value and os.getenv("TOKEN_FILE"):
        path = Path(os.environ["TOKEN_FILE"])
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.startswith("VELOX_ADMIN_TOKEN="):
                    value = line.split("=", 1)[1].strip().strip("'\"")
                    break
    if not value or any(ch in value for ch in "\r\n"):
        raise RuntimeError("VELOX_ADMIN_TOKEN or TOKEN_FILE is required")
    return value


def http_json(
    method: str,
    url: str,
    token: str,
    body: Any = None,
    timeout: float = 30,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    """Execute an HTTP request and return (status, parsed_body, headers)."""
    data = None
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw) if raw else {}, dict(response.headers)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw[:500]}
        return exc.code, payload, dict(exc.headers)


def scrape_text(url: str, token: str) -> str:
    """Fetch raw text from a URL (e.g. Prometheus /metrics endpoint)."""
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        return ""


def provision_m2m(master_url: str, admin_token: str) -> tuple[str, str]:
    """Create an ephemeral M2M key and return (client_id, plaintext_secret)."""
    client_id = f"parallel-bench-{int(time.time())}-{os.getpid()}"
    status, body, _ = http_json(
        "POST",
        f"{master_url}/api/v1/admin/m2m/keys",
        admin_token,
        {
            "client_id": client_id,
            "description": "parallelism certification ephemeral client",
            "scopes": ["jobs.submit"],
            "rate_limit_rps": 20,
            "rate_limit_burst": 40,
            "quota_max_scenes": 100,
            "quota_max_total_secs": 3600,
        },
    )
    if status != 201 or not body.get("plaintext_secret"):
        raise RuntimeError(f"M2M provisioning failed: HTTP {status}: {body}")
    return client_id, str(body["plaintext_secret"])


def delete_m2m(master_url: str, admin_token: str, client_id: str) -> None:
    """Delete an ephemeral M2M key."""
    http_json(
        "DELETE",
        f"{master_url}/api/v1/admin/m2m/keys/{urllib.parse.quote(client_id)}",
        admin_token,
        timeout=10,
    )
