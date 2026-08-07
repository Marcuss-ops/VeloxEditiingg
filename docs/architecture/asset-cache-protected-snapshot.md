# Asset Cache & Protected-Asset Snapshot

> Master-side service that publishes the "next N dispatchable jobs'
> Drive clip IDs" as a polling-friendly snapshot, plus the worker-side
> durable index that keeps those clips on disk between jobs. The
> feature is the answer to "Google Drive costs us 30 s per download;
> we re-download the same Tyson.mp4 ten times an hour."

**Status**: 12/12 passes shipped. Owner: Velox Maintainer.
**Last review**: 2026-07-27.

## 1. Topology at a glance

```
                   ┌─────────────────────────┐
                   │ MASTER (velox-server)   │
                   │  ┌───────────────────┐  │
                   │  │ protectedasset.   │  │   ◀── Pass 5
                   │  │   Service         │  │
                   │  │   (RWMutex,       │  │
                   │  │    monotonic      │  │
                   │  │    Version)       │  │
                   │  └─────────┬─────────┘  │
   every 30 s ─────▶  Service.Run ticker
                   │       │
                   │       ▼
                   │  ListNextDispatchableJobs(db, 10)
                   │       │                       ◀── Pass 3 (shared SQL)
                   │       ▼
                   │  assetref.ExtractDriveFileIDs(◀── Pass 4 (shared)
                   │       payload each
                   │       │
                   │       ▼
                   │  in-memory Snapshot
                   │  {Version, GeneratedAt,
                   │   LookaheadJobs,
                   │   DriveFileIDs[]}
                   │       ▲
                   │       │                ◀── Pass 6
   GET  /api/v1/agent/cache/protected-assets   Handler{200 | 503}
                   └───────┼─────────────────┘
                           │ every 30-60 s
                           ▼
   ┌─────────────────────────────────────────────┐
   │ WORKER (velox-worker-agent)                  │
   │                                             │
   │  ┌──────────────────────┐  ◀── Pass 2 (SQLite WAL)
   │  │ workercache.Cache    │  Acquire/Release per row
   │  └──────────┬───────────┘
   │             │
   │             ▼
   │  ┌──────────────────────┐  ◀── Pass 10 (atomic .part → rename)
   │  │ Downloader           │  DriveSource → .part → verify → final
   │  └──────────┬───────────┘
   │             │
   │             ▼
   │  ┌──────────────────────┐  ◀── Pass 11 (TODO)
   │  │ DownloadCoalescer    │  singleflight, key = driveID
   │  └──────────┬───────────┘
   │             │
   │             ▼
   │  CleanupWithPolicy ◀── Pass 12 (lease + in-flight +
   │  (ticker every 5 m)          protected + grace + staleness)
   └─────────────────────────────────────────────┘
```

The cache is split: the master owns the snapshot (read-only, no DV
contention with workers); each worker owns its own SQLite cache
durable across restarts. The two communicate via an HTTP poll
(planned: Pass 8 worker-side polling client, deferred).

## 2. Lifecycle of a Drive clip

```
fetch   ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
        │ workerResolver │  │ workerDownloader│  │ workercache    │
        │ (Pass 2)       │  │ (Pass 10)        │  │ (Pass 2)       │
        └─────────┬──────┘  └────────┬─────────┘  └────────┬───────┘
                  │                  │                     │
                  ▼                  ▼                     ▼
1. Find/Store  row(.part)   ───▶  Stream to .part     Row: download_complete=0
                                       │
                                       ▼
                                 verifyMedia
                                       │
                                       ▼
                                 os.Rename(.part → final, ATOMIC)
                                       │
                                       ▼
                                 MarkDownloadComplete(final, size)
                                       │
                                       ▼
                            Row: download_complete=1

protect ┌────────────────┐
        │ Worker with    │  Dispatch arrives
        │ clip cache     │
        │ + protected-   │  1. AcquireJobClips(jobID, driveIDs)
        │ asset polling  │  ─── Pass 9
        │ (Pass 7 → 8)   │       set active_job_id per row
        └────────────────┘  2. dispatchTaskRunner runs
                                  Chronon render reads the files
                              3. defer lease.ReleaseAll(ctx)
                                  active_job_id cleared + last_used_at bumped
```

The protected-asset snapshot is a *hint*: workers always retry on
cache miss (downloads re-trigger), but if the snapshot covers the
clip, the worker skips the Cleanup eviction.

## 3. Pass-by-pass deliverables

| Pass | Module                | What's new                                         |
|------|-----------------------|----------------------------------------------------|
| 1    | `shared/assetref`     | `DriveFileID(rawURL)` — canonical parser           |
| 2    | `worker/workercache`  | SQLite `cached_assets` + Acquire/Release           |
| 3    | `shared/dispatchable` | `ListNextDispatchableJobs` shared SQL              |
| 4    | `shared/assetref`     | `ExtractDriveFileIDs(payload)` + cross-module move |
| 5    | `protectedasset`      | `Service` with RWMutex + monotonic Version         |
| 6    | HTTP handler (`api`)  | `GET /api/v1/agent/cache/protected-assets`       |
| 7    | Master wiring         | (deferred — boot order + bootstrap_protectedasset) |
| 8    | Worker polling        | (deferred — pkg/api `GetProtectedAssets`)          |
| 9    | worker/clip_lease     | `AcquireJobClips` + dispatchTaskRunner integration |
| 10   | workercache/downloader| atomic `.part` → verify → rename → MarkComplete   |
| 11   | DownloadCoalescer     | (deferred — singleflight, key = driveID)           |
| 12   | config + docs + race  | this file + CleanupPolicy + race-condition test    |

## 4. Concurrency model

### Master side
- `protectedasset.Service.Snapshot()` returns a struct by value
  under `sync.RWMutex`. Many concurrent readers + one atomic-swap
  writer. Writer holds the lock froms `s.snapshot.Version + 1`
  read through struct replacement (no torn read).
- Snapshot is in-memory only; no DB query per request.

### Worker side
- `workercache.Cache` serialises through `*sql.DB` (WAL +
  `_busy_timeout=5000`).
- `Acquire` / `Release` predicates are per-row atomic SQL
  statements — no transaction or two-phase commitment.
- `Cleanup` is single-goroutine (one ticker loop per worker).
  Internal goroutines race on individual rows but never on the
  Cleanup loop itself.

### Race windows (documented, not bugs)
- **T0 snapshot → T0+5s new job → T0+10s cleaner**: the snapshot
  has shifted but the new job's `active_job_id` keeps the clip
  alive. Even without the lease, the grace rule (Pass 12) keeps
  the clip for `RecentUseGrace` after `last_used_at`.
- **Snapshot older than `SnapshotMaxAge`**: the cleaner DOES NOT
  delete anything. Better to grow the cache temporarily than to
  wipe active in-flight data under the false assumption that
  "nothing is protected". Operators see `ErrSnapshotStale` and
  resync the master snapshot loop.

## 5. Failure modes

| Failure                              | Behaviour                                |
|--------------------------------------|------------------------------------------|
| Source Open fails                    | No `.part` written. `ErrSourceOpen`.     |
| `io.Copy` fails mid-stream           | `.part` removed. Error wrapped.           |
| `verifyMedia` fails                  | `.part` removed, NO rename. `ErrVerifyFailed`. |
| `os.Rename` fails (EXDEV cross-fs)   | `.part` removed. `ErrRename`. No fallback. |
| `cache.MarkDownloadComplete` fails   | `final` removed. Row stays at d_c=0.      |
| Master snapshot loop dies            | Workers skip Cleanup pass, log `ErrSnapshotStale`. |
| Two jobs enter `AcquireJobClips` concurrently with same `driveID` | Both acquire; second overwrites lease. Singleflight (Pass 11) collapses to one download. |

## 6. Env vars

### Master (`DataServer/internal/protectedasset/config.go`)

| Var                              | Default | Effect on `Service`                       |
|----------------------------------|---------|-------------------------------------------|
| `VELOX_CACHE_LOOKAHEAD_JOBS`     | 10      | `Service.NewService(repo, $LOOKAHEAD)`    |
| `VELOX_CACHE_SNAPSHOT_INTERVAL`  | 30s     | `Service.Run(ctx, $INTERVAL)` ticker      |

### Worker (`worker/workercache/cleanup_policy.go`)

| Var                              | Default | Effect on `CleanupWithPolicy`              |
|----------------------------------|---------|--------------------------------------------|
| `VELOX_CACHE_CLEANUP_INTERVAL`   | 5m      | Cleanup loop ticker (daemon wiring only)   |
| `VELOX_CACHE_RECENT_USE_GRACE`   | 3m      | Keep row if `last_used_at` newer than this |
| `VELOX_CACHE_SNAPSHOT_MAX_AGE`   | 2m      | Stale snapshots → skip pass                |

Recommended initial values:

```bash
# Master
export VELOX_CACHE_LOOKAHEAD_JOBS=10
export VELOX_CACHE_SNAPSHOT_INTERVAL=30s

# Worker
export VELOX_CACHE_CLEANUP_INTERVAL=5m
export VELOX_CACHE_RECENT_USE_GRACE=3m
export VELOX_CACHE_SNAPSHOT_MAX_AGE=2m
```

## 7. Operational playbooks

### Recovering a poisoned cache
The cache SQLite is the durable truth — never delete the `.db` file
without first inspecting what's in it:
```bash
sqlite3 /var/lib/velox-worker/assets_cache/cache.db \
    "SELECT drive_file_id, download_complete, active_job_id, last_used_at FROM cached_assets"
```
- Rows with `download_complete=0` and no physical file → safe to
  `DELETE FROM cached_assets WHERE download_complete=0` (the
  resolver will retry on the next request).
- Stale rows (`last_used_at` older than `RecentUseGrace`) → safe
  to delete; this is what the cleanup loop would do.
- Leased rows (`active_job_id != ''`) → **NEVER** delete while a
  job is in flight; `cancelJob(jobID)` first.

### Master snapshot loop is stalled
If `CleanupWithPolicy` returns `ErrSnapshotStale` repeatedly:
1. Check `cmd server logs` for `Service.Refresh` errors during Run.
2. Check `dispatchable.ListNextDispatchableJobs` directly — it
   uses the same SELECT as the scheduler; if that fails, the
   scheduler is broken too.
3. As a short-term fix, raise `VELOX_CACHE_SNAPSHOT_MAX_AGE` to
   e.g. 10m so the cleaner keeps more rows during the stall.

### Disk pressure spikes
Cleanup cycle is bounded by row count (typically ≪10k). If
`cache.db` ever grows larger than a few MB, the worker is likely
capturing logs in `last_used_at` writes — see `cleanup.go` for the
SQL indexes that bound the cost of `List`.

## 8. Future work / known gaps

- Pass 7 (master bootstrap wiring) and Pass 8 (worker polling
  client) are not yet in repo; the cleanup policy test (Pass 12)
  documents the contract until full end-to-end snapshot polling is
  shipped.
- Pass 11 singleflight is a stale-Pass 9 todo. Two concurrent
  jobs can race the `Acquire` overwrite until singleflight ships.
- Metrics (`cleanup_total{reason}`, `acquire_count`,
  `release_count`) are not yet emitted; the `CleanupStats` and
  `Lease.ReleaseAll` return values are observability-ready but
  not yet wired to Prometheus.
