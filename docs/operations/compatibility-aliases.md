# Compatibility alias registry

Velox keeps one registry in `shared/compatibility/registry.go`. Producers emit
canonical fields; readers use the registry during the temporary migration
window. New package-local alias lists are not allowed.

Each registry entry records:

- canonical key and legacy aliases;
- owner and known consumers;
- per-alias read and rejection counters;
- removal date;
- minimum compatible contract version.

The master exports:

```text
velox_compatibility_alias_reads_total{alias,canonical}
velox_compatibility_rejections_total{alias,canonical}
```

Labels are bounded by the registry and never include job, task, artifact, or
client identity.

## Modes

`VELOX_COMPATIBILITY_MODE=compat` is the default. Legacy aliases remain
accepted and every actual alias read increments the registry counter and the
Prometheus metric.

`VELOX_COMPATIBILITY_MODE=strict` rejects registered aliases at the canonical
HTTP submission boundary and increments the rejection counter. The response is
an `HTTP 422` `legacy_alias_rejected` validation error. Existing internal
readers also fail closed for a registered alias when strict mode is active.

Invalid mode values fail configuration validation. An empty mode in manually
constructed test configurations is treated as the compatibility default.

## Removal workflow

1. Inspect `Registry()` and the per-alias counters.
2. Confirm all listed consumers have migrated to the canonical key.
3. Run `compat` until reads remain zero for the agreed migration window.
4. Enable `strict` and verify rejection metrics remain zero.
5. Remove the alias from the single registry, update the CI inventory, and
   retain a negative test proving the old shape is rejected.
