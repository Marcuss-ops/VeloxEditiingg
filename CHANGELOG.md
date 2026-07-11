## v1.2.21 (2026-07-11)

### Behavior changes

- DataServer fallback SPA: long-dead default "frontend_standalone/web/dist" path replaced by "VeloxFrontend/web/dist" (submodule). Falls back to live handler when VELOX_SPA_DIR is unset AND submodule dist/ exists. Operators using VELOX_SPA_DIR are unaffected.

## [Unreleased] - 2026-07-11

### Submodule relationship
- `VeloxEditiingg/.gitmodules` pins `VeloxFrontend` to commit `a2113ae` (intentional, by user request).
- Standalone `VeloxFrontend` HEAD is at `2369671` (newer than the submodule pin).
- The pin in the parent is preserved as-is: anyone who clones `VeloxEditiingg` gets `VeloxFrontend` at `a2113ae`, NOT at its latest standalone HEAD.
- This is by design for the migration backup: the parent project snapshot reflects the state at the backup time, not a rolling HEAD.

