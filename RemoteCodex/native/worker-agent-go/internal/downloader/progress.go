// Package downloader — canonical worker-side asset download manager.
//
// Velox delivers media assets to workers through a single pipeline:
// asset resolution → cache check → download → size+SHA-256 verification →
// atomic promotion → ASSET_READY. No transcoding, no FFmpeg, no proxy
// belongs in this phase.
//
// This package is the "DownloadManager" layer of that pipeline. It does NOT
// own the byte transport or the on-disk atomic write: those are delegated to
// a pluggable Transferer (the worker wires its master-bridge transferer in
// production; tests wire byte fakes). What the manager owns — and what no
// other component owns — is:
//
//   - one Transfer per asset key, shared across every job that requests it
//     (TransferRegistry);
//   - a precise download state machine (DownloadState) that can never reach
//     READY before verification;
//   - a concurrency-bounded, priority-stable pool (scheduler.go);
//   - per-transfer and per-job progress snapshots (DownloadSnapshot /
//     JobDownloadSnapshot) with waiter/subscriber plumbing;
//   - transfer contexts owned by the transfer itself, so cancelling the job
//     that first requested an asset never tears down the shared download
//     (each job is only a waiter).
//
// The public surface is AssetDownloadManager:
//
//	type AssetDownloadManager interface {
//	    Resolve(ctx, DownloadRequest) (DownloadedAsset, error)
//	    Snapshot(assetKey) (DownloadSnapshot, bool)
//	    Subscribe(assetKey) (<-chan DownloadSnapshot, func())
//	    JobSnapshot(jobID) JobDownloadSnapshot
//	}
package downloader

import (
	"sort"
	"time"

	"velox-shared/assetref"
)

// Progress throttles. Subscriber snapshots are published at least once per
// ProgressPublishInterval or per ProgressPublishBytes of newly downloaded
// bytes, whichever comes first; state changes and terminal transitions always
// publish immediately (never more than one event per 32 KB buffer).
//
// Checkpoints (the durable OnCheckpoint hook) are coarser by design: at least
// one per ProgressCheckpointInterval or per ProgressCheckpointBytes,
// whichever comes first, plus always one for the terminal transition.
const (
	ProgressPublishInterval    = 500 * time.Millisecond
	ProgressPublishBytes       = 4 << 20 // 4 MiB
	ProgressCheckpointInterval = 2 * time.Second
	ProgressCheckpointBytes    = 16 << 20 // 16 MiB
)

// AssetRole classifies the semantic role of an asset inside a job. Roles are
// low-cardinality strings used for diagnostics and future scheduling; they
// never participate in asset identity.
type AssetRole string

const (
	AssetRoleClip      AssetRole = "clip"
	AssetRoleStock     AssetRole = "stock"
	AssetRoleVoiceover AssetRole = "voiceover"
	AssetRoleMusic     AssetRole = "music"
	AssetRoleImage     AssetRole = "image"
	AssetRoleSubtitle  AssetRole = "subtitle"
	// AssetRoleAsset is the generic fallback for fields that do not map to a
	// more specific role (mirrors the worker's legacy "asset" classifier).
	AssetRoleAsset AssetRole = "asset"
)

// RoleFromString maps a free-form role label (e.g. from a payload field
// classifier) onto the closed AssetRole set. Unknown labels become
// AssetRoleAsset.
func RoleFromString(s string) AssetRole {
	switch AssetRole(s) {
	case AssetRoleClip, AssetRoleStock, AssetRoleVoiceover, AssetRoleMusic,
		AssetRoleImage, AssetRoleSubtitle:
		return AssetRole(s)
	default:
		return AssetRoleAsset
	}
}

// DownloadState is the transfer state machine. Valid transitions:
//
//	QUEUED → CACHE_CHECK → CACHE_HIT → READY
//	QUEUED → CACHE_CHECK → QUEUED → DOWNLOADING → VERIFYING → READY
//	DOWNLOADING → RETRY_WAIT → DOWNLOADING     (resilience commit)
//	VERIFYING → FAILED                          (hash/size mismatch)
//	any non-terminal → CANCELLED                (last waiter left)
//
// A file can never become READY before verification: READY is only reachable
// from CACHE_HIT (the cached file was already verified when it was written)
// or from VERIFYING (verification completed in the Transferer).
type DownloadState string

const (
	DownloadQueued     DownloadState = "QUEUED"
	DownloadCacheCheck DownloadState = "CACHE_CHECK"
	DownloadCacheHit   DownloadState = "CACHE_HIT"
	DownloadRunning    DownloadState = "DOWNLOADING"
	DownloadVerifying  DownloadState = "VERIFYING"
	DownloadReady      DownloadState = "READY"
	DownloadRetryWait  DownloadState = "RETRY_WAIT"
	DownloadFailed     DownloadState = "FAILED"
	DownloadCancelled  DownloadState = "CANCELLED"
)

// Terminal reports whether s is a final state. Terminal transfers are kept in
// the registry for snapshot visibility but are never reused for new Resolve
// calls (a fresh transfer is created instead, which re-runs the cheap cache
// check).
func (s DownloadState) Terminal() bool {
	switch s {
	case DownloadReady, DownloadFailed, DownloadCancelled:
		return true
	default:
		return false
	}
}

// DefaultPriority is the priority every asset of a job receives until the
// plan's tiered priorities (intro > current job > future job) are wired.
const DefaultPriority = 100

// DefaultAssetConcurrency is the number of simultaneous byte transfers per
// worker when VELOX_ASSET_DOWNLOAD_CONCURRENCY is unset.
const DefaultAssetConcurrency = 4

// DownloadRequest is the canonical, per-file download request. Every file
// reaches the manager as an explicit asset; URL, Drive ID and filename are
// never used interchangeably as identity (AssetKey is the single identity).
type DownloadRequest struct {
	JobID    string
	TaskID   string
	AssetKey assetref.AssetKey
	// AssetID is retained only as a legacy transport/read-model field; it
	// must not be used for transfer identity or cache ownership.
	AssetID  string
	Role     AssetRole
	SceneIDs []string

	// Source is the semantic origin of the asset (e.g. "master_asset_bridge").
	// Informational; the transferer decides how to fetch it.
	Source string
	// SHA256 and SizeBytes are the integrity contract. A hit is valid only
	// when both are present and match; with both absent the file is
	// downloaded and verified for media-like content only.
	SHA256    assetref.ContentHash
	SizeBytes int64
	MIMEType  string

	Priority int
}

// DownloadedAsset is the successful outcome of Resolve: a local filesystem
// path whose bytes were verified (or a previously verified cache hit).
type DownloadedAsset struct {
	AssetKey  assetref.AssetKey
	AssetID   string
	LocalPath string
	SHA256    assetref.ContentHash
	SizeBytes int64
	CacheHit  bool
	ReadyAt   time.Time
	// Outcome is the canonical classification from the lookup point
	// (Transferer.Check), carried through the transfer so the resolver
	// boundary never re-derives hit/miss. Empty only for legacy transferers.
	Outcome CacheOutcome
}

// DownloadJobReference preserves the per-job ownership metadata for a
// shared physical transfer. It prevents one job's scene/task metadata from
// being projected onto another job's job_asset_refs row.
type DownloadJobReference struct {
	JobID    string
	TaskID   string
	SceneIDs []string
}

// DownloadSnapshot is the observable state of one transfer. The manager keeps
// snapshots cheap to produce: they are read under the transfer mutex and never
// reference live io state that could block.
type DownloadSnapshot struct {
	TransferID string
	AssetKey   assetref.AssetKey
	// AssetID remains a legacy wire compatibility field.
	AssetID string
	Role    AssetRole
	State   DownloadState

	BytesDownloaded int64
	BytesTotal      int64
	ProgressPercent float64

	ThroughputBytesPerSecond float64
	ETASeconds               int64

	Attempt       int
	QueuePosition int
	SharedWaiters int

	QueuedAt    time.Time
	StartedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt time.Time

	CacheHit bool

	ErrorCode          string
	ErrorDetail        string
	CheckpointSequence int64
	TransferGeneration int64

	// JobIDs is the durable read-model projection of the jobs currently
	// referencing this shared physical transfer. It is sorted for stable
	// wire output; it does not affect transfer identity.
	JobIDs   []string
	JobRefs  []DownloadJobReference
	TaskID   string
	SceneIDs []string
	MIMEType string
	SHA256   assetref.ContentHash
}

// JobDownloadSnapshot aggregates every transfer a job is waiting on. Progress
// is weighted on bytes — never on file counts (1 MB complete + one 5 GB
// empty is NOT 50%).
// sortedJobIDs returns a deterministic copy for wire/read-model snapshots.
func mergeStrings(existing, incoming []string) []string {
	seen := make(map[string]struct{}, len(existing)+len(incoming))
	out := make([]string, 0, len(existing)+len(incoming))
	for _, values := range [][]string{existing, incoming} {
		for _, value := range values {
			if value == "" {
				continue
			}
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			out = append(out, value)
		}
	}
	return out
}

func sortedJobIDs(refs map[string]DownloadJobReference) []string {
	ids := make([]string, 0, len(refs))
	for id := range refs {
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func sortedSceneIDs(refs map[string]struct{}) []string {
	ids := make([]string, 0, len(refs))
	for id := range refs {
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func snapshotJobRefs(refs map[string]DownloadJobReference) []DownloadJobReference {
	ids := sortedJobIDs(refs)
	out := make([]DownloadJobReference, 0, len(ids))
	for _, id := range ids {
		ref := refs[id]
		ref.SceneIDs = append([]string(nil), ref.SceneIDs...)
		out = append(out, ref)
	}
	return out
}

type JobDownloadSnapshot struct {
	JobID string

	AssetsTotal       int
	AssetsQueued      int
	AssetsDownloading int
	AssetsVerifying   int
	AssetsReady       int
	AssetsFailed      int
	CacheHits         int
	CacheMisses       int

	BytesDownloaded int64
	BytesTotal      int64
	ProgressPercent float64

	ActiveTransfers int
	QueuedTransfers int

	EstimatedRemainingSeconds int64
}
