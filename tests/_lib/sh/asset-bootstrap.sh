# =============================================================================
# tests/_lib/sh/asset-bootstrap.sh — generic workdir + stub binary writers.
# =============================================================================
# Extracted from run.sh's setup_shared() (which writes BUNDLE_HASH.txt +
# engine_selftest_baseline.sha256 + the velox_video_engine stub into the
# matrix workdir). Generalized so any consumer can seed the standard layout
# without re-implementing.
# =============================================================================

# bootstrap_workdir <workdir> <bundle_hash> — create the standard e2e layout
# under <workdir>:
#   <workdir>/state/                       (Velox runtime state)
#   <workdir>/temp/                        (Velox scratch)
#   <workdir>/tests/fixtures/              (engine self-test fixtures)
#   <workdir>/BUNDLE_HASH.txt              (== <bundle_hash>)
#   <workdir>/tests/fixtures/engine_selftest_baseline.sha256
bootstrap_workdir() {
  local workdir="$1" bundle_hash="$2"
  ensure_dir "$workdir/state" "$workdir/temp" "$workdir/tests/fixtures"
  printf '%s' "$bundle_hash" > "$workdir/BUNDLE_HASH.txt"
  printf 'velox-e2e-stub-output' | sha256sum | awk '{print $1}' \
    > "$workdir/tests/fixtures/engine_selftest_baseline.sha256"
}

# write_stub_binary <out_path> <body-content> — write a +x stub bash script.
# Caller is responsible for escaping $ correctly in body (multiline content
# is supported; the caller writes a literal string).
write_stub_binary() {
  local out="$1" body="$2"
  printf '%s\n' "$body" > "$out"
  chmod +x "$out"
}
