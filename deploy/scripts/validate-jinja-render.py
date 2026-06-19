#!/usr/bin/env python3
"""
Smoke-test for the Velox master env Jinja2 template.

Renders deploy/templates/velox-server.env.j2 against deploy/group_vars/all.yml
plus a synthetic vault token, writes the rendered env to a temporary file,
and asserts that every documented field resolves correctly.

This is a pure structural sanity check: it confirms there are no unrresolved
Jinja markers, no real worker IDs leaked into the versioned render, and that
each known VELOX_* field is populated from the expected source.

Usage:
  python3 deploy/scripts/validate-jinja-render.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import jinja2
import yaml

# Python on Windows defaults its stdout/file encoding to cp1252, which
# cannot encode the unicode box-drawing characters used in the env
# template (U+2500 'BOX DRAWINGS LIGHT HORIZONTAL'). Force UTF-8 so the
# smoke-test runs identically on Linux CI and Windows developer hosts.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except (AttributeError, ValueError):
    pass

REPO_ROOT = Path(__file__).resolve().parents[2]


def fail(msg: str) -> None:
    print(f"  FAIL: {msg}")


def ok(msg: str) -> None:
    print(f"  OK:   {msg}")


def main() -> int:
    errors = 0

    all_yml_path = REPO_ROOT / "deploy" / "group_vars" / "all.yml"
    template_path = REPO_ROOT / "deploy" / "templates" / "velox-server.env.j2"
    out_path = Path("/tmp/rendered_velox_master_env")

    with all_yml_path.open() as fh:
        data = yaml.safe_load(fh.read())

    # Inject a synthetic vault token so we can assert secret substitution.
    data["vault_velox_admin_token"] = "TEST_TOKEN_64_chars_" + ("x" * 56)

    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(str(template_path.parent)),
        keep_trailing_newline=True,
        trim_blocks=False,
        lstrip_blocks=False,
    )
    template = env.get_template(template_path.name)
    rendered = template.render(**data)
    # Force UTF-8 so the box-drawing chars in the template survive on
    # Windows (default cp1252 would raise UnicodeEncodeError).
    out_path.write_text(rendered, encoding="utf-8")

    print(f"=== Jinja render: {template_path} -> {out_path} ({len(rendered)} B) ===")

    checks = [
        ("no unresolved {{ … }} markers",
         "{{" not in rendered and "}}" not in rendered),
        ("no unrresolved Jinja comments { # …# }",
         "{#" not in rendered and "#}" not in rendered),
        # Note: CHANGE_ME placeholders are INTENTIONALLY present in the
        # committed render; real worker IDs are injected via the
        # ansible vault. We do NOT block on their presence anymore.
        ("VELOX_MASTER_PORT=8000 resolved",
         "VELOX_MASTER_PORT=8000" in rendered),
        ("VELOX_GRPC_PORT=9000 resolved",
         "VELOX_GRPC_PORT=9000" in rendered),
        ("VELOX_ADMIN_TOKEN substituted with secret",
         rendered.startswith("") and "VELOX_ADMIN_TOKEN=TEST_TOKEN_64_chars_" in rendered),
        # Two-worker canonical topology: render MUST carry the
        # placeholder pair, NOT any real worker IDs.
        ("VELOX_ALLOWED_WORKERS resolves from group_vars (canonical placeholder)",
         "VELOX_ALLOWED_WORKERS=CHANGE_ME_ALLOWED_WORKERS" in rendered),
        ("VELOX_ALLOWED_WORKERS contains no real worker IDs in committed render",
         "velox-worker-" not in rendered),
        ("VELOX_ALLOWED_WORKERS does not contain '*' wildcard",
         "*" not in rendered.split("VELOX_ALLOWED_WORKERS=", 1)[1].split("\n", 1)[0]),
        ("VELOX_DB_PATH populated",
         "VELOX_DB_PATH=/var/lib/velox/data/velox.db" in rendered),
        # Legacy alias retired — any presence in the rendered env is a
        # regression the smoke-test catches BEFORE the next deploy.
        ("VELOX_DB_DSN alias retired (must NOT appear in render)",
         "VELOX_DB_DSN=" not in rendered),
        ("GIN_MODE=release literal",
         "GIN_MODE=release" in rendered),
        ("VELOX_CODE_VERSION=1.1.1",
         "VELOX_CODE_VERSION=1.1.1" in rendered),
        ("no /{[/] left from non-stringified dict keys",
         "<class" not in rendered),
    ]
    for name, passed in checks:
        if passed:
            ok(name)
        else:
            fail(name)
            errors += 1

    print()
    if errors == 0:
        print("Smoke test: PASS")
        return 0

    print(f"Smoke test: FAIL ({errors} check(s) failed)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
