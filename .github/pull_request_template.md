# Pull Request

## Canonical path

- [ ] This change uses the canonical owner of the capability. (See
      `docs/architecture/OWNERSHIP.md`.)
- [ ] It does NOT introduce a second writer or a second entrypoint for
      job state / assets / delivery / config.
- [ ] It does NOT introduce silent fallbacks.
- [ ] It does NOT introduce dual-write.
- [ ] It does NOT add a permanent legacy alias. (Compatibility shims
      MUST carry the COMPATIBILITY block with `Owner:` issue number and
      `Remove after:` date \u2014 see `check-single-writer.sh`.)

## Cleanup

- [ ] All callers of the touched capability have been migrated.
- [ ] The old path has been deleted (no `_legacy`, `_old`, `.deprecated`
      suffix survives).
- [ ] `git grep` returns no matches for the old symbol.
- [ ] No stub, wrapper, or compatibility shim without an owner + removal
      deadline remains.

## Verification

- [ ] `make verify` passes locally.
- [ ] New invariant tests have been added (single-writer, registry
      completeness, etc. \u2014 not duplicate implementation tests).
- [ ] Migrations are forward-only (no `DROP TABLE`, no SQLite-unsafe
      `ALTER \u2026 RENAME COLUMN`).
- [ ] Branch is rebased on `origin/main`.
- [ ] This PR contains exactly ONE coherent change (one refactor, one
      feature, or one fix).
