#!/usr/bin/env bash
# CI guard: ensure no legacy UpdateJobFields references remain in production code.
# Excludes test files, compatibility adapters, and migration files.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

forbidden_patterns=(
  'UpdateJobFields('
  'UpdateJobFieldsStrictWhitelistKey'
  'UpdateJobFieldsLegacyKeys'
  'ErrJobFieldNotWhitelisted'
  'legacyKeyWarnLog'
  'logLegacyKeyOnce'
  '"master_video_path"'
  '"drive_url"'
  '"video_uploaded"'
  '"youtube_upload_status"'
  '"drive_upload_status"'
)

exclude_globs=(
  ':!**/compat/**'
  ':!**/*_test.go'
  ':!**/migrations/**'
  ':!scripts/check_no_legacy_job_mutations.sh'
)

exit_code=0

for pattern in "${forbidden_patterns[@]}"; do
  matches=$(git grep -n "$pattern" -- '*.go' "${exclude_globs[@]}" 2>/dev/null || true)
  if [ -n "$matches" ]; then
    echo "❌ Forbidden pattern found: $pattern"
    echo "$matches"
    echo ""
    exit_code=1
  fi
done

if [ $exit_code -eq 0 ]; then
  echo "✅ No legacy job mutation patterns found."
else
  echo ""
  echo "One or more legacy patterns were found. These must be removed."
  echo "See the PR3 spec for the canonical replacements."
fi

exit $exit_code
