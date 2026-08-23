# shellcheck shell=bash

assert_artifact_exists() {
  info "Verification 1: artifact exists on disk"
  artifact="$(find "$STORAGE_DIR" -type f \( -name '*.mp4' -o -name '*.f4v' \) 2>/dev/null | head -1 || true)"
  if [[ -z "$artifact" ]]; then
    fail "no .mp4 or .f4v artifact found in $STORAGE_DIR"
    ls -laR "$STORAGE_DIR" 2>/dev/null || true
    exit 1
  fi
  art_size="$(stat -c%s "$artifact" 2>/dev/null || stat -f%z "$artifact" 2>/dev/null || echo 0)"
  if (( art_size < 1000 )); then
    fail "artifact too small: ${art_size} bytes (expected ≥1 KB)"
    exit 1
  fi
  pass "artifact: $(basename "$artifact") (${art_size} bytes)"
}

assert_artifact_sha256() {
  if [[ -z "${E2E_EXPECTED_SHA256:-}" ]]; then
    fail "E2E_EXPECTED_SHA256 must be set for deterministic SHA-256 enforcement (was unset)"
    exit 1
  fi
  info "Verification 3: SHA-256 checksum (expected=${E2E_EXPECTED_SHA256:0:16}...)"
  sha="$(sha256sum "$artifact" 2>/dev/null | awk '{print $1}' || shasum -a 256 "$artifact" 2>/dev/null | awk '{print $1}' || true)"
  if [[ -z "$sha" ]]; then
    fail "SHA-256 could not be computed"
    exit 1
  fi
  echo "$sha  $(basename "$artifact")" > "$STORAGE_DIR/artifact.sha256"
  if [[ "$sha" != "$E2E_EXPECTED_SHA256" ]]; then
    fail "SHA-256 mismatch: got ${sha:0:16}... want ${E2E_EXPECTED_SHA256:0:16}..."
    exit 1
  fi
  pass "SHA-256 matches: ${sha:0:16}..."
}
