from __future__ import annotations
import shutil
from pathlib import Path
from typing import Any

LOCK_FILES = {
    "SingletonCookie",
    "SingletonLock",
    "SingletonSocket",
    "CrashpadMetricsActive.pma",
}

def _copy_profile_tree(source: Path, target: Path) -> None:
    if not source.exists():
        raise FileNotFoundError(f"profile source dir not found: {source}")

    target.parent.mkdir(parents=True, exist_ok=True)

    def _ignore(_: str, names: list[str]) -> set[str]:
        ignored = set()
        for name in names:
            if name in LOCK_FILES or name.endswith(".tmp"):
                ignored.add(name)
        return ignored

    shutil.copytree(source, target, dirs_exist_ok=True, ignore=_ignore)

def _load_netscape_cookie_jar(path: Path) -> list[dict[str, Any]]:
    cookies: list[dict[str, Any]] = []
    if not path.exists():
        return cookies

    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") and not line.startswith("#HttpOnly_"):
            continue

        http_only = False
        if line.startswith("#HttpOnly_"):
            http_only = True
            line = line[len("#HttpOnly_") :]

        parts = line.split("\t")
        if len(parts) != 7:
            continue

        domain, _flag, cookie_path, secure, expires, name, value = parts
        cookies.append(
            {
                "name": name,
                "value": value,
                "domain": domain,
                "path": cookie_path,
                "expires": int(expires),
                "httpOnly": http_only,
                "secure": secure.upper() == "TRUE",
            }
        )

    return cookies

def _unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
