# Consumer Audit — Analytics HTTP Endpoints

**Date**: 19 June 2026
**Repo state**: local HEAD = `98b3606` (commit "fix: gRPC connection reset after restart —
server readiness signal + worker fast retry"), 17 commits behind
upstream `d0e8a2ff`.
**Scope**: enumerate every consumer of `/api/v1/analytics*` and `/api/youtube/manager/analytics*`
endpoints so the upstream removal (Step 0 sync) is safe.

## Methodology

```
rg -F "analytics"
  over {*.sh, *.py, *.ts, *.tsx, *.js, *.jsx, *.json, *.yml, *.yaml}
  under {scripts/, refactored/frontend_standalone/, refactored/DataServer/, docs/}
```

Excludes data-layer audit references (path strings pointing at JSON files,
not HTTP calls — those are not live consumers).

## Findings

### 1. `refactored/scripts/dod_check.sh` — two matches (data layer)

```
line 366:   "analytics/analytics_cache.json"
line 367:   "analytics/youtube_api_cache.json"
```

These are referenced inside `dod_check.sh`'s `checkLegacyJSONSources` array —
they list local file paths the data-layer audit expects to be removed (or
migrated to SQLite). **Not live HTTP callers** — purely file-path strings.

### 2. `refactored/frontend_standalone/**` — zero matches

```
rg -F "analytics"   # *.ts, *.tsx, *.js, *.jsx, *.json
```

No SPA source file, no build script, no manifest inside
`frontend_standalone/web/`, `frontend_standalone/scripts/`, or
`frontend_standalone/.github/` references any analytics endpoint. The SPA
dist (`web/dist/`) is intentionally not committed so it cannot introduce
clients we did not expect.

### 3. `refactored/DataServer/internal/handlers/server/youtube/youtube_channels_v1_bulk.go:259`

```
"message": "Channel analytics available via /api/v1/analytics endpoints"
```

This is a **response body string**, surfaced by the bulk YouTube routes
when a client asks for channel-level analytics. **Not a consumer**, but a
**producer**: the channel route acknowledges that the (live, registered
in-source) `/api/v1/analytics` endpoints handle the request, then refers
the client to them. Once Step 0 removes `/api/v1/analytics`, this string
must be updated to either drop the message or point at the new owner
(SQLite-backed analytics, if any).

### 4. n8n / external automation configs — zero matches

```
find . -maxdepth 6 -name 'n8n*'
find . -name '*.json' -path '*workflows*'
```

No n8n workflow files, no Make targets wrapping curl-to-analytics, and no
`docs/runbooks/*.sh` calling `/api/v1/analytics`. The user-mentioned "n8n
consumers" do not exist in this repo at `98b3606`.

## Conclusion

The `/api/v1/analytics*` and `/api/youtube/manager/analytics*` endpoint
families have **zero live HTTP consumers** in the local repo at `98b3606`.
The only references are:

1. **Filename strings** inside `dod_check.sh`'s JSON-cleanup array (`scripts/`).
2. **A response-body message** in `youtube_channels_v1_bulk.go:259`.
3. **The analytics handlers themselves** (which Step 0 will remove).

## Migration guidance for Step 0

- After pulling `d0e8a2ff` (which removes `/api/v1/analytics*`), update
  `youtube_channels_v1_bulk.go:259` to drop the "available via /api/v1/analytics"
  hint (or reword as "channel analytics currently unavailable").
- `dod_check.sh`'s filename list can be tightened once the data layer
  confirms analytics_cache.json is migrated/dropped.
- No frontend, n8n, or external script fallout expected.
