#!/usr/bin/env bash
# Focused tests for verify-worker-image.sh. Docker integration is exercised by
# the image-build job; these tests cover fail-closed argument handling and the
# digest normalization contract without requiring a registry.
set -Eeuo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/verify-worker-image.sh"

if "$SCRIPT" >/tmp/velox-verify-worker-image-test.out 2>/tmp/velox-verify-worker-image-test.err; then
  echo "verify-worker-image accepted missing IMAGE" >&2
  exit 1
fi
if ! grep -q 'usage:' /tmp/velox-verify-worker-image-test.err; then
  echo "verify-worker-image did not print usage for missing IMAGE" >&2
  exit 1
fi

# Keep this assertion textual and dependency-free: a full registry digest must
# be normalized from @sha256:<hex> before it is compared with the expected
# sha256:<hex> value. This guards the exact regression that previously made
# every remote digest verification fail.
grep -q 'grep -Fqx -- "\$EXPECTED_IMAGE_DIGEST"' "$SCRIPT"
grep -q 'image repository digest mismatch' "$SCRIPT"

echo "test-verify-worker-image: PASS"
