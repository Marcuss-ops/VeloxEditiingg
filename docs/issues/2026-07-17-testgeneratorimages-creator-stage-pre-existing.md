# Issue filed 2026-07-17 — pre-existing test failure (audit-trail pointer)

## Headline

`TestGenerateWithImages_UsesCreatorStageWhenConfigured` (in
`DataServer/internal/handlers/server/script/handler_test.go`) has been
failing at HEAD on `main` since before the size-band-policy slice
(PR-15.7) landed. The 2026-07-17 audit confirmed HEAD AND HEAD~3 both
fail; this is **pre-existing**, not introduced by the policy work.

## Owner

`enqueue` (delivery_plan canonical-purity contract) +
`creatorflow` (creator-stage resolver / DTO / orchestration path).

Cross-references:
- `DataServer/internal/handlers/server/script/handler_test.go`
  (test under repair — line 213+)
- `DataServer/internal/handlers/server/script/handler.go`
  (production handler)
- `DataServer/internal/creatorflow/*` (resolver)
- `DataServer/internal/jobs/enqueue/*` (canonical-purity validation)

## Audit-trail anchors

- **§ 19.7** of `docs/metrics/loc-refactor-history.md` — says the
  iter-1+2 mockVoiceover attempt was REVERTED per the user's
  "small and well-bounded" criterion.
- **§ 19.8** of the same document — the forward-pointer that this
  issue FILLS.
- **Triage archive** at `docs/trials/handler-test-preTriage.2026-07-17/`
  (carry-over of the iter-1+2 cascade snapshot, with `//go:build ignore`
  to prevent compile-time confusion).

## Two-stage cascade (the actual failure mode)

### Stage 1 — Real-world 404 on example.com URLs

The test sends an HTTP request whose `voiceover_path` is
`https://example.com/voice.mp3` and whose `scenes[].image_link` is
`https://example.com/scene1.png`. The asset service tries to download
both URLs and gets 404 from `example.com`. The handler reflects
`download failed with status 404` and returns HTTP 422.

```
handler_test.go:275 assertion: want 200, got 422
body: {"code":"VOICEOVER_ASSET_UNAVAILABLE","field":"reference",
       "message":"download failed with status 404",
       "ok":false,"source_type":"http"}
```

### Stage 2 — Iter-1+2 mockVoiceover attempt cascades

Adding `mockVoiceover := httptest.NewServer(...)` to the test and
redirecting `voiceover_path` to the mock didn't pass either: the test
hit a DIFFERENT failure mode. After the asset-rewrite step, the
enqueue path rejected with `delivery_plan is required for
canonical-purity enqueue`. So Stage 1's 404 problem masked Stage 2's
canonical-purity gap — fixing one surfaces the other.

The user's `"small and well-bounded"` policy (`commit a fix on main
if/only if small and well-bounded`) precludes shipping either partial
fix. The iter-1+2 attempt was REVERTED (in commit chains preceding
this issue filing). This issue tracks the properly-bounded fix.

## Suggested fix scope

Three reasonable paths (any ONE of these):

1. **Hermetic inline mocks** (per the eval-turn verdict in this
   session — C "staying inline"). Add a `mockAssetSrv` per-test (or
   per-package) that returns canned bytes for the request-side URL;
   keep the existing `mockCreator` mock unchanged. Estimated +20-30
   lines.

2. **Bypass-creator refactor.** Skip the failing test variant —
   consolidate into `TestGenerateWithImages_BypassesCreatorForRenderReadyPayload`
   (which already passes per § 19.4 verification). Estimated -50
   lines, +0 lines (net removal).

3. **Canonical-purity fixture overhaul.** Build a complete
   `delivery_plan` for the scene_video test payload that matches the
   enqueue side's contract. Estimated +40-60 lines.

The user's brief fixes the OWNER: enqueue + creatorflow jointly.
Code change stays on `main` per project rules (NO BRANCHES,
fast-forward friendly).

## Reproduction

```bash
cd DataServer && go test -count=1 -v \
  ./internal/handlers/server/script/... 2>&1 | tail -30
# Expected: failure at handler_test.go:275 with HTTP 422 + VOICEOVER_ASSET_UNAVAILABLE
```

## Brief anchor

PR-15.7 (size-band policy slice) — this issue was surfaced during the
post-PR-15.7 audit, NOT introduced by it.
