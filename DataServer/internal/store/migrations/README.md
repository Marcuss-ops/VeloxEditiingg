# Migrations

This directory holds the forward-only migration runner and the embedded
SQL files consumed by it at boot.

## Layout

```
sqlite/    — SQLite-cumulative .sql files (production boot path:
             internal/store/sqlite.go::NewSQLiteStoreFromHandle).
postgres/  — Postgres-native .sql files.
testdata/  — Canonical fixtures consumed by the discovery / discovery
             tests. Must mirror the production migrations exactly.
```

Discovery and ordering live in `discovery.go`; the apply loop and
checksum gate live in `runner.go` / `apply.go`. **Never modify an
applied migration** — `runner.go` performs a strict checksum check and
will refuse to boot on mismatch (see `runner.go` error path:
"Never modify an applied migration — create a new one instead").

## Legacy YouTube migrations (deprecated, kept for chain integrity)

The YouTube domain (channels, groups, OAuth tokens, niches, videos,
API cache, analytics, quota) is no longer a Velox concern: ownership
moved to the external **Social API** repo. Velox now ships a thin
`socialclient` HTTP adapter and a generic `social_gateway` delivery
provider; the delivery runner talks to the Social service.

The Go-side YouTube files (`internal/store/youtube_*.go`,
`internal/store/youtubetypes/`, `internal/integrations/youtube/`,
`internal/handlers/server/youtube/`,
`internal/deliveries/providers/youtube.go`) have been dropped.
However the historical Create-Table migrations **cannot** be deleted
or replaced with `no-op` contents: that would break the checksum gate
on every Velox instance that already applied them. Instead, they are
retained verbatim and explicitly superseded by the official **Drop**
migration(s) listed below.

### Deprecated CREATE migrations (kept verbatim, do NOT edit)

| Dialect  | Files |
| -------- | ----- |
| SQLite   | `sqlite/003_youtube_canonical.sql`<br>`sqlite/011_youtube_oauth_tokens.sql`<br>`sqlite/012_youtube_groups_rename.sql`<br>`testdata/003_youtube_canonical.sql`<br>`testdata/011_youtube_oauth_tokens.sql`<br>`testdata/012_youtube_groups_rename.sql` |
| Postgres | `postgres/009_youtube.sql` |

### Authoritative DROP migrations (canonical ownership)

| Dialect  | Drop |
| -------- | ---- |
| SQLite   | `sqlite/090_drop_youtube_domain.sql` (+ `testdata/090_drop_youtube_domain.sql`) — drops `youtube_channels`, `youtube_groups`, `youtube_group_channels`, `youtube_oauth_tokens`, `youtube_tracked_niches`, plus YouTube metrics/cache tables, **and** the columns `calendar_events.youtube_group`, `calendar_events.youtube_links_json`, `dark_editor_folders.youtube_group`. |
| Postgres | `postgres/010_drop_youtube_domain.sql` (+ `testdata/010_drop_youtube_domain.sql`) — drops `youtube_videos`, `youtube_oauth_tokens`, `youtube_group_channels`, `youtube_groups`, `youtube_channels`. |

### Migration chain outcome

DB state after applying the chain (cumulative current source) on a
fresh database:

1. CREATE YouTube tables (003, 011, 012 / postgres 009) — temporary,
   only live long enough to be dropped.
2. Domain work continues to bring Velox forward (`...070..085..`).
3. Step 90 (SQLite) / 10 (Postgres) drops the entire YouTube domain.
4. End state has **zero** YouTube tables or columns.

For installs already past step 90 / 010 with old live YouTube tables,
step 90 / 010 itself performs the cleanup. Older installs upgrading
must keep applying migrations in numerical order; the checksum gate
ensures both old and new hosts converge to the same final schema.

### Adding new YouTube-related migrations

There are **no** new YouTube migrations. Future work that needs
YouTube concepts must live in:

* `socialclient/` (HTTP client surface, errors, request/response
  shapes), or
* the external Social API repo.

A new migration in this directory touching anything under
`youtube_*`, `oauth_youtube_*` or any other YouTube-only artifact is
a **regression** and must be rejected in review.

## Forward-only invariant

`runner.go` enforces this invariant: applied migrations are checksum
pinned, and any modification (even a comment or whitespace tweak) will
fail boot. If a logical change is needed after a migration was
shipped, create a new migration with a new version. Never edit
existing SQL files.
