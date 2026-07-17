# handler-test-preTriage archive (2026-07-17)

## What this is

This archive preserves the working-tree state of the pre-existing test-failure investigation
in `DataServer/internal/handlers/server/script/handler_test.go` from the 2026-07-17 triage
session. Files here exist ONLY to keep audit-trail back-linkable after the original /tmp
working-trail would have been wiped by tmpfs cleanup.

The triage went through two iterations before being RESTORED (reverted) in favour of a
discrete follow-up issue / fix (see forward-state § §19.8 of
`docs/metrics/loc-refactor-history.md`):

| Iter | What it added / changed | Outcome |
| ---- | ----------------------- | ------- |
| **iter-1** | Added a local `mockVoiceover := httptest.NewServer(...)` block; redirected request `voiceover_path` URL to point at the mock. | Test STILL FAILED with `VOICEOVER_ASSET_UNAVAILABLE` 404 (because `image_link` was a real URL too). |
| **iter-2** | In addition to iter-1, redirected `image_link` URL to the same mock. | Test STILL FAILED — now with a DIFFERENT error: `delivery_plan canonical-purity` enqueue rejection. Cascade went past the user's "small and well-bounded" criterion, so the iter-1+2 changes were REVERTED in the post-triage clean-up commit (see § 19.7 audit, "8-file surface-area audit"). |

## Files

| File | Bytes | SHA-256 (BEFORE copy) |
| ---- | ----: | --------------------- |
| `diff-8074890-overflow.patch` | 8076 | `f94843724a145f1ba2b46569472ffe60d59cb9331eeab8ad18a6d669e021aa9d` |
| `handler_test.go.iter12` | 21133 | `418f0abcb3515dc8be5fbdaf93fa39e6f4cb90dad8757952d00cf6cd6c734a55` |

## Provenance

* **Session**: 2026-07-17 (audit + §19.7 round on `main`)
* **Trigger**: pre-existing test failure `TestGenerateWithImages_UsesCreatorStageWhenConfigured` returning `want 200, got 422` body `VOICEOVER_ASSET_UNAVAILABLE` (`download failed with status 404`)
* **Reference SHA anchored at**: `original 8074890` (size-band-policy original commit; the iter-1+2 diff captures the working-tree overflow relative to this baseline)
* **Forward state**: see `docs/metrics/loc-refactor-history.md` § §19.8 — the test fix is deferred to a discrete follow-up issue; the iter-1+2 attempt was REVERTED.

## Why kept (not deleted)

The user (`--NO BRANCHES ONLY MAIN--` policy) cares about audit-trail integrity. Even when a
triage fix is reverted, preserving the diff + the iter-state file lets a future reader
reproduce the investigation without re-deriving the cascade. /tmp is ephemeral; `docs/trials/`
is git-tracked.

## Forward follow-up

If/when the test failure is fixed (separate issue), this archive can be deleted via:

```bash
git rm -r docs/trials/handler-test-preTriage.2026-07-17
git commit -m 'archive(tests): retire handler-test-preTriage archive post-fix'
```
