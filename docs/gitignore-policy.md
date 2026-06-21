# .gitignore policy

This document is the canonical rule book for anyone adding, modifying, or
reviewing an entry in `.gitignore` in this repository. It exists because a
single one-line `.gitignore` rule took three commits to converge on a
correct form (`65bc3f3d → ca6908a1 → 050d39a3` — first attempted form,
broken-by-polish form, final surgical form), all due to ambiguous-scope
patterns. The lesson learned is encoded below so future contributors do
not repeat the regression.

## Repository location

This file lives at `docs/gitignore-policy.md` as a standalone document
rather than appended to [`docs/architecture/OWNERSHIP.md`](docs/architecture/OWNERSHIP.md).
The reasoning:

- `OWNERSHIP.md` is reserved for code/codename ownership tables —
  canonical writers and forbidden side paths. Gitignore hygiene is a
  different concern surface: it constrains review process, not code
  generation or runtime semantics.
- The two files have different audiences: `OWNERSHIP.md` is read by
  anyone changing a module. This policy is read by anyone adding a
  transient artifact, reproduction scratch directory, or noisy
  build output to a contributor's working tree.
- Liveness differs: `OWNERSHIP.md` rows are append-only and change on
  architectural milestones. Gitignore policy evolves in lock-step with
  the CI lint that enforces it (planned; see
  [CI integration](#ci-integration-planned-not-in-this-commit)).

If you are tempted to consolidate, don't — the two docs serve distinct
purposes and merging would dilute both.

## Canonical rule forms

The four **anchor-pattern shapes** below cover path-rooted rules; they
are the patterns with the highest collision risk (and therefore the
highest review rigour). The `.gitignore` also commonly uses patterns
outside this table — bare-name suffix globs (`*.bak`, `*.log`, `*.tmp`,
`*.swp`), recursion globs (`<subdir>/**`), and negation rules
(`!pattern`). Each of these must be verified case-by-case using
[Pre-commit verification](#pre-commit-verification), but they do not
require the "both anchors" rule below.

| Form | Meaning | Example | When to use |
| --- | --- | --- | --- |
| `/<name>/` | Root-level directory only | `/dist/`, `/controltransport/` | Canonical form for top-level orphan build artifacts (e.g. transient `protoc` output dropped at repo root). |
| `/<name>` | Root-level file or directory (file-or-dir, NOT dir-only) | `/VERSION.txt.bak` | Root-level stray files. Rare — usually you want `/<name>/` instead. |
| `<subdir>/<name>` | Anchored to `.gitignore` location (= repo root, in this repo) | `DataServer/bin`, `frontend_standalone/web/dark_editor/dist` | Scoped ignores that must NOT match at any other depth. Does **not** recurse. |
| `<subdir>/**` | Everything under a subdir | `RemoteCodex/native/video-engine-cpp/build/**` | Recursive ignore under one specific subdir. |

Anything not in this table — bare names, trailing-but-not-leading slashes,
leading-but-not-trailing slashes, glob patterns without an explicit anchor
— must be reviewed with extra scrutiny. The next section explains why.

## Banned patterns

### Bare names: `controltransport`

```gitignore
controltransport
```

A bare name matches **any path segment** equal to that name, at **any
depth**, for **files or directories**. The rule above silently matches
both:

- the orphan `./controltransport/` at the repo root (intended)
- every legitimate file/dir under `shared/controltransport/...` (NOT
  intended — silently hidden from `git status` / `git add .`)

This is how a single typo produced the bug fixed in commit `050d39a3`.
Bare names are **banned** for any rule that risks colliding with a tracked
parent.

### Trailing-slash only: `controltransport/`

```gitignore
controltransport/
```

A trailing `/` says "dir-only" but without a leading `/` the rule still
matches at any depth. The example above matches both:

- `./controltransport/` at the repo root (intended)
- `shared/controltransport/` (NOT intended, but dir-only so less harmful
  than the bare-name case — still wrong)

Always combine trailing-slash **with** leading-slash when you mean a
root-level dir-only ignore.

### Leading-slash only: `/controltransport`

```gitignore
/controltransport
```

A leading `/` anchors to repo root but no trailing `/` matches a file or
directory indifferently. Wastes precision: if your intent is "the root
build artifact IS a directory, not a stray file", state that explicitly
with the trailing slash.

### Why three `/`-flavours?

Git's pattern syntax overloads `/` for two orthogonal concerns: **anchor
to root** and **constrain to directory**. The four legal forms above are
the cartesian product of those two boolean axes. The easy mistake is to
forget one axis and pay for it with a silent collision. The fix is always
to **make both axes explicit** even when you think one is obvious.

### Bare-name suffixes: `*.bak` (allowed, NOT banned)

```gitignore
*.bak
*.tmp
*.log
*.swp
```

A bare-name pattern that matches only a SUFFIX (or any name containing
a glob meta-character) cannot collide with a tracked directory, because
no tracked directory is literally named `*.bak` or `*.log`. These
patterns are SAFE and should NOT be confused with the banned bare-name
PATH-SEGMENT patterns in the previous subsections.

The key contrast:

- `controltransport` — bare PATH SEGMENT, collides with
  `shared/controltransport/...` (banned).
- `*.bak` — bare SUFFIX GLOB, cannot collide with any tracked path
  segment (allowed).

If you are tempted to add `*.controltransport` or any pattern whose
name segment is shared with a tracked subdir, **don't** — that crosses
back from suffix-glob into banned territory.

## Pre-commit verification

> **Manual recipe only — do NOT copy the probe-dir block below into a
> CI script.** The probe block creates `./orphan_dir/` in the working
> tree and removes it inline; a CI job that mimics this has a race
> window where `./orphan_dir/` is visible to `git status` and could
> pollute diff reports. For automated verification, see the bash
> skeleton under [CI integration](#ci-integration-planned-not-in-this-commit).

Before pushing any change to `.gitignore`, every new or modified rule
MUST be smoke-tested with `git check-ignore -nv`. The `-n` flag includes
non-matching paths in the output (so you see status — matched AND
not-matched — for every probed path), and `-v` prints the rule text plus
line number that fired for any matched path. This is the canonical
recipe:

```bash
# 1. For each rule added/changed, check it does NOT ignore any TRACKED file
#    whose path might collide with the rule's pattern.
git check-ignore -nv shared/controltransport/errors.go \
                    shared/controltransport/transport.go \
                    shared/controltransport/pb/worker_control.pb.go
# Expected: empty output. If ANY of these prints a match, the rule is wrong.

# 2. Confirm the rule DOES ignore the intended orphan path. Create a probe:
mkdir -p /tmp/probe
touch /tmp/probe/orphan_dir/file
cp -r /tmp/probe/orphan_dir ./orphan_dir  # create at root
git check-ignore -nv ./orphan_dir ./orphan_dir/file
# Expected: stdout line(s) — at minimum "./orphan_dir/" matched by rule.
rm -rf ./orphan_dir

# 3. Double-check sibling-but-canonical files (untracked) are still visible:
git status
# Expected: any untracked file under shared/controltransport/ still shows.
```

A reviewer touching `.gitignore` is entitled to ask for the output of the
above commands. Reviewers are encouraged to run the same checks
themselves as a second pair of eyes.

The `git check-ignore` command is documented at
<https://git-scm.com/docs/git-check-ignore>. The flags we depend on:

- `-n` / `--non-matching` — show paths that would NOT be ignored as well
  as paths that would. Helps catch over-eager rules.
- `-v` / `--verbose` — include the matched rule text and line number in
  the output. Makes it trivial to find the offending rule.

## Worked example: the `controltransport/` incident

The orphan `./controltransport/` directory at the repo root is created
when a `protoc` regeneration attempt with a misconfigured
`--go_opt=paths=source_relative` variant writes its output at the wrong
relative path before being moved into the canonical location
(`shared/controltransport/`). Three iterations were required to converge
on a correct rule:

| Commit | Rule | Status | Why |
| --- | --- | --- | --- |
| `65bc3f3d` | `controltransport/` | unsafe | trailing-only-slash — matched `shared/controltransport/` too |
| `ca6908a1` | `controltransport` | unsafe | bare name — collided with every `shared/controltransport/...` file |
| `050d39a3` | `/controltransport/` | ✅ accepted | root-only AND dir-only — minimal-correct, surgical |

The correct form anchors to root (`/`) **and** restricts to dir-only
(`/`) **simultaneously**. No future contributor should be tempted to
"polish" this back to fewer slashes — fewer slashes is strictly less
precise.

## CI integration (planned, NOT in this commit)

The existence of this policy implies a CI smoke-test that fails fast on
ambiguous-scope patterns. The minimum acceptable lint would:

1. Parse `.gitignore` and reject any rule that lacks both anchors unless
   it is a glob or explicitly scoped (per the table above).
2. Probe a small set of known-canonical tracked paths (e.g.
   `shared/controltransport/pb/*.pb.go`) and assert `git check-ignore -nv`
   on each emits no output.
3. Probe the documented orphan paths and assert `git check-ignore -nv`
   emits the expected rule match.


A minimal seed that future contributors can extend (the same pattern
`OWNERSHIP.md` recommends for new architectural invariants) is captured
in a script scaffold:

​```bash
#!/usr/bin/env bash
# scripts/ci/check-gitignore.sh
# Fail-fast on ambiguous-scope patterns. See docs/gitignore-policy.md.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
fail=0
# Rule 1: reject bare-name rules that are NOT suffix-glob safe.
# Strip inline comments (anything from '#' onwards, AFTER the
# lineno:rule split) so 'rule  # comment' is treated as 'rule'.
while IFS=: read -r lineno rule; do
  rule="${rule%%#*}"             # strip inline comment
  rule="${rule%"${rule##*[![:space:]]}"}"  # trim trailing whitespace
  case "$rule" in
    '')        continue ;;        # blank after trim
    */*)       continue ;;        # contains slash — path-anchored
    /*)        continue ;;        # leading slash — root-anchored
    \*\.\*)    continue ;;        # bare-name suffix glob (e.g. *.bak)
    \*\?*\*)   continue ;;        # bare-name glob with multi-char (e.g. *foo*)
  esac
  echo "FAIL line $lineno: bare-name rule '$rule' (ambiguous scope)"
  fail=1
done < <(grep -nE '^[[:space:]]*[^[:space:]#]' "$ROOT/.gitignore")
# Rule 2: canonical tracked paths MUST NOT be ignored.
for path in shared/controltransport/errors.go \
            shared/controltransport/transport.go \
            shared/controltransport/pb/worker_control.pb.go; do
  if [[ -e "$ROOT/$path" ]] && git -C "$ROOT" check-ignore -q "$path"; then
    echo "FAIL: tracked path '$path' is ignored — gitignore rule collision!"
    fail=1
  fi
done
exit "$fail"
​```

This is filed as a follow-up; the present commit establishes the policy
text, not the enforcement. The CI lint will land in a separate PR so this
document is the single source of truth against which the lint is later
calibrated.

## Quick reference

- ✅ `/<name>/` for top-level orphan dirs (most common)
- ✅ `/<name>` for top-level orphan files (rare)
- ✅ `<subdir>/<name>` for path-scoped ignores (no recursion by default)
- ✅ `<subdir>/**` for recursive ignores under one subdir (when needed)
- ✅ bare-name suffix globs (`*.bak`, `*.log`, `*.tmp`, `*.swp`) — cannot collide with a tracked path segment
- ❌ bare-name **path-segment** patterns (`controltransport`) — silent collision with any tracked subdir of the same name
- ❌ trailing-only-slash (`name/`) — silent collision with subdirs
- ❌ leading-only-slash (`/name`) — wasted precision, matches file or dir
- Always: `git check-ignore -nv <tracked-path>` before commit
- Always: `git check-ignore -nv <orphan-path>` after commit to verify

In case of doubt: **add both anchors** and verify with `git check-ignore`.
