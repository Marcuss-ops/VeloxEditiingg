# Milestone PR — YouTube → Social API separation (closed)

This is a **single-file docs marker** for the milestone Pull Request
[YouTube → Social API separation (closed)](#) so the PR has a
diff to point at (GitHub rejects self-PRs where head == base). The
conclusive record for the separation lives in two other places that
this PR also references:

- **Conclusive changelog record:** `CHANGELOG.md` PR-15.9 +
  `docs/CHANGELOG.md` PR-15.9 (Removed / Added / Changed / Commit
  chain / Verification / Refs)
- **Release tag:** `v1.2.21-yt-removed` (annotated, points at
  `b3cd004 chore(changelog): finalize YouTube→Social migration record`)
- **Milestone issue:** #34 (closed by this PR)
- **End-to-end integration coverage:** commit `5b0759c test(integration):
  end-to-end social_repo boundary coverage` — 13 sub-tests covering the
  6 documented scenarios on both the enqueue pre-flight AND the
  `DeliveryRunner.DeliverArtifact` paths.

## 12-commit closure chain (chronological)

| # | Hash | Subject |
| --- | --- | --- |
| 1  | `777a7f8` | `chore(store): drop residual YouTube tables and types` |
| 2  | `ef579fb` | `test(deliveries): confine HTTP Social only, drop YouTube tests` |
| 3  | `98220a4` | `chore(deploy): drop YouTube env and secrets, keep Social only` |
| 4  | `53eb01b` | `chore(deps): tidy, drop YouTube google deps` |
| 5  | `ffc5157` | `docs: remove YouTube references, document Social API boundary` |
| 6  | `aa16b6e` | `chore(model): rename YouTube→Delivery intent (no-op, verified)` |
| 7  | `06ded17` | `refactor(validator): delegate destination validation to Social API` |
| 8  | `cae8f21` | `chore: verify Velox is YouTube-free` |
| 9  | `62526a9` | `chore(audit): Velox is YouTube-free verification` |
| 10 | `59ba4eb` | `chore(worker-agent): drop YouTube default in OutputFormat, fix Dockerfile comment` |
| 11 | `b3cd004` | `chore(changelog): finalize YouTube→Social migration record` |
| 12 | `5b0759c` | `test(integration): end-to-end social_repo boundary coverage` |

## Verification checklist (results)

- `git grep -ni "youtube" -- ':!docs/' ':!CHANGELOG.md'` → 0 active-code matches
- `git grep -ni "youtube/v3|youtubeanalytics|VELOX_YOUTUBE|YOUTUBE_|oauth.*youtube"` → 0 matches
- `find DataServer Pipeline RemoteCodex -iname '*youtube*'` → only migration testdata fixtures
- `cd DataServer && go build ./... && go vet ./... && go test ./...` → PASS
- `cd RemoteCodex/native/worker-agent-go && go build ./...` → PASS
- `cd DataServer && go test -count=1 -v ./internal/integration_test/...` → 13/13 PASS

## Future-work followups (post-closure)

- ✅ **DONE** — httptest integration tests for social_repo boundary (`5b0759c`, 13 sub-tests)
- ⬜ open — pin `OutputFormat` empty-default semantic in `RemoteCodex/native/worker-agent-go/pkg/video/pipelines/entities/compiler_test.go`
- ⬜ open — mirror Social API pre-flight at `FinalizeVerified` (one-shot `socialclient.Client.ValidateDestination` right before `DeliverArtifact`)
- ⬜ open — audit zero-dependency Drive integration for parity against the removed `google.golang.org/api/drive`-backed version

## Replay guide (for future contributors auditing this)

```bash
git log --oneline 777a7f8..5b0759c
git grep -ni "youtube" -- ":!docs/" ":!CHANGELOG.md"
( cd DataServer && go build ./... && go vet ./... && go test ./... )
( cd RemoteCodex/native/worker-agent-go && go build ./... )
( cd DataServer && go test -count=1 -v ./internal/integration_test/... )
```
