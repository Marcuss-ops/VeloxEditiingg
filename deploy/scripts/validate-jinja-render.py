#!/usr/bin/env python3
"""
Smoke-test for the Velox master env Jinja2 template.

Renders deploy/templates/velox-server.env.j2 against deploy/group_vars/all.yml
plus a synthetic vault token, writes the rendered env to a temporary file,
and asserts that every documented field resolves correctly.

This is a pure structural sanity check: it confirms there are no unrresolved
Jinja markers, no CHANGE_ME placeholders left in the rendered output, and that
each known VELOX_* field is populated from the expected source.

Usage:
  python3 deploy/scripts/validate-jinja-render.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import jinja2
import yaml

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
    out_path.write_text(rendered)

    print(f"=== Jinja render: {template_path} -> {out_path} ({len(rendered)} B) ===")

    checks = [
        ("no unresolved {{ … }} markers",
         "{{" not in rendered and "}}" not in rendered),
        ("no unrresolved Jinja comments { # …# }",
         "{#" not in rendered and "#}" not in rendered),
        ("CHANGE_ME absent in populated render",
         "CHANGE_ME" not in rendered),
        ("VELOX_MASTER_PORT=8000 resolved",
         "VELOX_MASTER_PORT=8000" in rendered),
        ("VELOX_GRPC_PORT=9000 resolved",
         "VELOX_GRPC_PORT=9000" in rendered),
        ("VELOX_ADMIN_TOKEN substituted with secret",
         rendered.startswith("") and "VELOX_ADMIN_TOKEN=TEST_TOKEN_64_chars_" in rendered),
        ("VELOX_ALLOWED_WORKERS joined with commas (current two-worker topology)",
         "VELOX_ALLOWED_WORKERS=velox-worker-523925eb,velox-worker-13197" in rendered),
        ("VELOX_DB_PATH populated",
         "VELOX_DB_PATH=/var/lib/velox/data/velox.db" in rendered),
        ("VELOX_DB_DSN tracks VELOX_DB_PATH (legacy alias)",
         "VELOX_DB_DSN=/var/lib/velox/data/velox.db" in rendered),
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
