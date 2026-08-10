# New production-file LOC guardrail

`scripts/ci/check-new-production-loc.py` is a complementary, non-invasive LOC
check. Unlike `check-loc-thresholds.sh`, it does **not** scan the whole tree:
it compares Git-added paths (`--diff-filter=A`) between `BASE_REF...HEAD` and
reports only newly added production files.

## Configuration

- `NEW_PRODUCTION_FILE_LOC_THRESHOLD` — positive integer LOC limit.
- Default: **600**.
- `BASE_REF` — Git base revision; CI supplies the pull-request base SHA or the
  previous push SHA. For a branch-creation event with an all-zero base, the
  checker compares against Git's empty tree. Scheduled/manual runs should
  provide an explicit base.
- `HEAD_REF` — Git head revision; defaults to `HEAD`.

The count matches `wc -l`: only newline-terminated lines are counted.

## Exclusions

The guardrail excludes:

- generated files (`Code generated`, `DO NOT EDIT`, `@generated` markers, and
  conventional `_gen`/`_generated` filenames);
- protobuf output (`.pb.*`, `_grpc.pb.go`);
- fixture, testdata, snapshot, golden, generated, archive and test paths;
- `.github/workflows/`;
- documentation (`.md`, `.rst`, `.adoc`, `.txt`, README, LICENSE, CHANGELOG);
- binary files detected by NUL bytes;
- unsupported file extensions.

Production source/config extensions are limited to the repository's common
source formats (`.go`, `.sh`, `.py`, `.cpp`, `.json`, `.yaml`, and related
extensions). Existing long files are intentionally ignored by this guardrail;
they remain covered by the full-tree category gate.

## Exit codes and output

- `0`: no new production file exceeds the threshold;
- `1`: at least one new production file exceeds the threshold;
- `2`: invalid threshold, missing/unresolvable Git base, or unreadable added
  blob.

Violations emit GitHub Actions annotations such as:

```text
::error file=path/to/new_file.go::new production file has 742 LOC (threshold 600)
```

Run locally with:

```bash
BASE_REF=origin/main HEAD_REF=HEAD \
  python3 scripts/ci/check-new-production-loc.py
bash scripts/ci/test-new-production-loc.sh
```
