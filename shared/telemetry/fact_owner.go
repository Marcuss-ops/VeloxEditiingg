package telemetry

// fact_owner.go encodes the Fact Owner rule at the receipt/benchmark level:
// every observed fact has EXACTLY ONE authoritative producer, and no other
// component may reconstruct it. The table below is the closed mapping used
// by PerformanceReceiptV1 and its assembler — clip count and expected
// duration come from the CompiledRenderPlan, downloaded bytes from the
// Downloader, packets read from the Media backend, and so on. An owner-less
// or double-owned fact is a taxonomy violation, not a benchmark.
//
// The same owner vocabulary (ComponentOwner) is stamped on every canonical
// event descriptor in catalog.json; this file covers the facts that are not
// single events (aggregate/derived observations such as "downloaded bytes"
// or "task status").

// Canonical fact names. These are the stable keys of the Fact Owner table.
const (
	// FactClipCount is the number of timeline clips the attempt rendered.
	FactClipCount = "clip_count"
	// FactExpectedDuration is the intended media duration of the job.
	FactExpectedDuration = "expected_duration"
	// FactAssetSHA is the cryptographic identity of a bound asset.
	FactAssetSHA = "asset_sha"
	// FactCacheHitMiss is the cache hit/miss outcome of an asset lookup.
	FactCacheHitMiss = "cache_hit_miss"
	// FactDownloadedBytes is the byte count fetched from remote storage.
	FactDownloadedBytes = "downloaded_bytes"
	// FactProcessSpawn is the spawn of an engine/external subprocess.
	FactProcessSpawn = "process_spawn"
	// FactPacketsRead is the media-backend packet read count.
	FactPacketsRead = "packets_read"
	// FactFramesDecoded is the decoder frame output count.
	FactFramesDecoded = "frames_decoded"
	// FactFramesEncoded is the encoder frame input count.
	FactFramesEncoded = "frames_encoded"
	// FactMuxBytes is the muxer's written byte count.
	FactMuxBytes = "mux_bytes"
	// FactArtifactSHA is the final artifact SHA256 produced/published.
	FactArtifactSHA = "artifact_sha"
	// FactCPURamDisk is the attempt's CPU/RAM/disk observation.
	FactCPURamDisk = "cpu_ram_disk"
	// FactTaskStatus is the task lifecycle status.
	FactTaskStatus = "task_status"
	// FactWorkerStatus is the worker state-machine status.
	FactWorkerStatus = "worker_status"
)

// canonicalFactOwners is loaded from the language-neutral catalog.json.
// Every fact appears exactly once with exactly one authoritative producer.
var canonicalFactOwners = loadCanonicalFactOwners()

// FactOwner returns the single authoritative producer for a canonical fact
// name. ok is false for unknown facts: an unknown fact must be added to the
// table above, never assigned an owner ad hoc.
func FactOwner(fact string) (ComponentOwner, bool) {
	owner, ok := canonicalFactOwners[fact]
	return owner, ok
}

// IsCanonicalFactName reports whether fact is a member of the closed Fact
// Owner table.
func IsCanonicalFactName(fact string) bool {
	_, ok := canonicalFactOwners[fact]
	return ok
}

// FactOwnerCount returns the number of facts in the closed table.
func FactOwnerCount() int { return len(canonicalFactOwners) }

// AllFactOwners returns a defensive copy of the Fact Owner table.
func AllFactOwners() map[string]ComponentOwner {
	out := make(map[string]ComponentOwner, len(canonicalFactOwners))
	for fact, owner := range canonicalFactOwners {
		out[fact] = owner
	}
	return out
}
