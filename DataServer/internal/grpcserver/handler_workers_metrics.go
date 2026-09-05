// Scorecard v1 / F2 — typed proto→Go conversion for the worker's
// periodic Heartbeat.resources payload.
//
// Background. The wire-format Heartbeat carries a typed
// *controlpb.WorkerResourceCounters (proto v3). The master had no path
// for these counters before — HandleHeartbeat persisted the Heartbeat
// into the registry's `extra` map as a few loose string fields but
// the Prometheus projection never received them, so the worker
// utilization + iowait + steal + RAM + disk + net gauges stayed at 0.
//
// Two clean jobs for this file:
//
//  1. Decode pb.WorkerResourceCounters → metrics.ResourceSnapshot and
//     forward into the WorkerResourceSink interface. Prometheus wants
//     counter INCR-by-deltas; Heartbeat carries TOTALS. So we keep a
//     per-worker LastSeenResources (MicrosafeLast-style) and compute
//     deltas at the call site.
//
//  2. Materialize the typed counters into a map[string]interface{} that
//     the registry.Heartbeat(...) call can merge into its own `extra`
//     map without breaking backward compat with consumers reading the
//     JSON via the legacy HTTP /admin/workers path.
//
// Cardinality discipline: every key written below is SAFE under the
// metrics safe-label allowlist (worker_class, executor_id, etc.). The
// `worker_id` key in the extra map is intentional — registry snapshots
// are not Prometheus TSDB rows and don't participate in label cardinality.
package grpcserver

import (
	"log"
	"sort"
	"sync"
	"time"

	velmetrics "velox-server/internal/metrics"

	pb "velox-shared/controltransport/pb"
)

// LastSeenResources caches the last-seen cumulative counters on a
// per-worker basis so the next Heartbeat can compute Prom-counter
// deltas without round-tripping to the registry or re-sending total
// values worth NaN-rates on first contact. Guarded by mu; reads/writes
// happen on the gRPC stream goroutine, so contention is bounded by the
// per-worker heartbeat rate (15 s default).
type LastSeenResources struct {
	mu                                                           sync.Mutex
	rxBytesTotal, txBytesTotal, evictionsTotal, corruptionsTotal uint64
	lastUpdate                                                   time.Time // UTC; maintained by Snapshot for stale eviction
}

// Snapshot records the cumulative values seen this beat and returns
// the per-delta fields that ResourceSnapshot expects from the worker.
// On first contact (no prior beat) the deltas ARE the totals — counting
// "since worker start" is the canonical Prometheus fallback for cumulatives
// without an origin timestamp, and matches what the worker emits today.
func (l *LastSeenResources) Snapshot(rx, tx, evictions, corruptions uint64) (rxDelta, txDelta, evDelta, corDelta uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastUpdate = time.Now().UTC()
	rxDelta = rx - l.rxBytesTotal
	txDelta = tx - l.txBytesTotal
	evDelta = evictions - l.evictionsTotal
	corDelta = corruptions - l.corruptionsTotal
	// Saturate at 0 on counter wrap / out-of-order arrivals; race-safe.
	if l.rxBytesTotal > rx {
		rxDelta = 0
	}
	if l.txBytesTotal > tx {
		txDelta = 0
	}
	if l.evictionsTotal > evictions {
		evDelta = 0
	}
	if l.corruptionsTotal > corruptions {
		corDelta = 0
	}
	l.rxBytesTotal = rx
	l.txBytesTotal = tx
	l.evictionsTotal = evictions
	l.corruptionsTotal = corruptions
	return
}

// lastSeenByWorker is the per-worker LastSeenResources pool. Synced
// only via handleHeartbeat (single writer per worker at a time). Map
// presence vs absence distinguishes "never seen" from "freshly evicted
// worker" so we never silently reset the cumulative baseline.
var lastSeenByWorker sync.Map // worker_id string → *LastSeenResources

// lastSeenByWorker growth bounds. Entries are NOT deleted on session
// teardown: presence is what prevents a counter-baseline reset (and a
// spurious Prometheus spike) on worker reconnect. Instead the map is swept
// lazily when a NEW worker entry appears, evicting entries that have not
// observed a heartbeat for lastSeenStaleAfter (evicted / decommissioned
// workers) and, if still over lastSeenMaxEntries, the oldest entries.
// A worker resurrected after eviction re-baselines via the documented
// "first contact reports totals as deltas" fallback.
const (
	lastSeenMaxEntries = 4096
	lastSeenStaleAfter = 24 * time.Hour
)

// sweepLastSeenByWorker bounds the lastSeenByWorker map. Called only when a
// new worker entry is created (worker first contact), so the O(n) Range walk
// runs at connection rate, not heartbeat rate.
func sweepLastSeenByWorker() {
	now := time.Now().UTC()
	size := 0
	type agedEntry struct {
		id      string
		last    time.Time
		current bool
	}
	entries := make([]agedEntry, 0, 64)
	lastSeenByWorker.Range(func(key, value any) bool {
		size++
		ls, ok := value.(*LastSeenResources)
		if !ok {
			entries = append(entries, agedEntry{id: key.(string), current: false})
			return true
		}
		ls.mu.Lock()
		last := ls.lastUpdate
		ls.mu.Unlock()
		entries = append(entries, agedEntry{id: key.(string), last: last, current: now.Sub(last) <= lastSeenStaleAfter})
		return true
	})
	if size <= lastSeenMaxEntries {
		return
	}
	evicted := 0
	// Pass 1: drop stale entries (no heartbeat within the staleness window).
	for _, e := range entries {
		if e.current {
			continue
		}
		lastSeenByWorker.Delete(e.id)
		evicted++
	}
	if size-evicted <= lastSeenMaxEntries {
		return
	}
	// Pass 2: still over cap — evict oldest-first regardless of staleness.
	oldest := make([]agedEntry, 0, len(entries))
	for _, e := range entries {
		if lastSeenByWorkerHas(e.id) {
			oldest = append(oldest, e)
		}
	}
	sort.Slice(oldest, func(i, j int) bool { return oldest[i].last.Before(oldest[j].last) })
	for _, e := range oldest {
		if size-evicted <= lastSeenMaxEntries {
			break
		}
		lastSeenByWorker.Delete(e.id)
		evicted++
	}
	if evicted > 0 {
		log.Printf("[GRPC] lastSeenByWorker sweep: evicted %d stale resource-baseline entries (size was %d, cap %d)", evicted, size, lastSeenMaxEntries)
	}
}

// lastSeenByWorkerHas reports whether the map still holds the given worker id.
func lastSeenByWorkerHas(id string) bool {
	_, ok := lastSeenByWorker.Load(id)
	return ok
}

// decodeWorkerResources converts the typed Heartbeat.resources into the
// ResourceSnapshot the metrics sink expects. The returned snapshot's
// counter-delta fields are byte-/event-counts INCR'd by Prometheus in
// this beat's window. Gauge fields overwrite recent values.
//
// safe-NULL behaviour: nil r returns (nil, prev) so the sink can
// short-circuit RecordWorker without touching the registry — protects
// pre-PR-3 workers that haven't shipped the resource sampler yet.
func decodeWorkerResources(workerID string, r *pb.WorkerResourceCounters) *velmetrics.ResourceSnapshot {
	if r == nil {
		return nil
	}

	lsIface, loaded := lastSeenByWorker.LoadOrStore(workerID, &LastSeenResources{})
	if !loaded {
		sweepLastSeenByWorker()
	}
	ls := lsIface.(*LastSeenResources)
	// PR-2 / F2 — cumulative→delta discipline:
	//   * NetworkRx/Tx: typed proto carries cumulative bytes — track
	//     diff so Prometheus sees the per-beat INCR amount.
	//   * CacheEvictions/Corruptions: NOT yet on the typed proto.
	//     Pass 0; future PR-3 (worker-agent-go resource sampler) will
	//     add cache_stats.evictions_total / corruptions_total and these
	//     fields will start tracking real deltas.
	//   * The reviewer's critical finding is explicit here: do NOT
	//     feed temp_bytes_written into the eviction-delta path —
	//     velox_cache_evictions_total would then Inc by temp bytes
	//     and produce a misleading scorecard.
	rxDelta, txDelta, evDelta, corDelta := ls.Snapshot(
		positiveI64ToU64(r.GetNetworkReceiveBytesTotal()),
		positiveI64ToU64(r.GetNetworkTransmitBytesTotal()),
		0, // cache Ejections total not yet on proto (PR-3 follow-up)
		0, // cache Corruptions total not yet on proto (PR-3 follow-up)
	)

	sampled := time.Now().UTC()
	if r.GetSampledAt() != nil {
		sampled = r.GetSampledAt().AsTime()
	}

	return &velmetrics.ResourceSnapshot{
		CPUUtilRatio:         r.GetCpuUtilizationRatio(),
		CPUIOWaitRatio:       r.GetCpuIowaitRatio(),
		CPUStealRatio:        r.GetCpuStealRatio(),
		CPUUserRatio:         r.GetCpuUserRatio(),
		CPUSystemRatio:       r.GetCpuSystemRatio(),
		EffectiveCpuCores:    r.GetEffectiveCpuCores(),
		ProcessRSSBytes:      r.GetProcessRssBytes(),
		ProcessRSSPeakBytes:  r.GetProcessRssPeakBytes(),
		MemoryUsedBytes:      r.GetMemoryUsedBytes(),
		MemoryAvailableBytes: r.GetMemoryAvailableBytes(),
		MemoryTotalBytes:     r.GetMemoryTotalBytes(),
		PageCacheBytes:       r.GetPageCacheBytes(),
		DiskFreeBytes:        r.GetDiskFreeBytes(),
		TempBytesWritten:     r.GetTempBytesWritten(),
		ScratchCurrentBytes:  r.GetScratchCurrentBytes(),
		ScratchPeakBytes:     r.GetScratchPeakBytes(),
		DiskReadMbps:         r.GetDiskReadMbps(),
		DiskWriteMbps:        r.GetDiskWriteMbps(),
		DiskIoWaitMs:         r.GetDiskIoWaitMs(),
		ActiveTasks:          r.GetActiveTasks(),
		TaskSlots:            r.GetTaskSlots(),
		RenderJobsActive:     r.GetRenderJobsActive(),
		PrefetchJobsActive:   r.GetPrefetchJobsActive(),
		PublisherJobsActive:  r.GetPublisherJobsActive(),
		Load1:                r.GetLoad1(),
		RunQueue:             r.GetRunQueue(),
		// NetworkRxBytesDelta / TxBytesDelta MUST stay under uint64 +
		// use lastSeen.Snapshot to convert cumulative→delta.
		NetworkRxBytesDelta: rxDelta,
		NetworkTxBytesDelta: txDelta,
		DownloadMbps:        r.GetDownloadMbps(),
		UploadMbps:          r.GetUploadMbps(),
		NetworkRetransmits:  r.GetNetworkRetransmitsTotal(),
		OpenFileDescriptors: r.GetOpenFileDescriptors(),
		MaxFileDescriptors:  r.GetMaxFileDescriptors(),
		FDUtilizationRatio:  r.GetFileDescriptorUtilizationRatio(),
		// Cache counts not yet on the proto — keep 0 until PR-3 surfaces
		// them. (PR-3 worker-agent-go resource sampler follow-up — F4.)
		CacheEntries:          0,
		CacheBytesUsed:        0,
		CacheEvictionsDelta:   evDelta,
		CacheCorruptionsDelta: corDelta,
		SampledAt:             sampled,
	}
}

// positiveI64ToU64 mirrors an int64 proto cumulative to uint64 with a
// zero-floor — surfaces a malformed negative cumulatives case by
// clamping to 0 so ResourceSnapshot deltas stay monotonic and
// Prometheus-friendly. Network cumulative counters are the only ones
// going through this path today (network bytes are guaranteed positive
// on the wire; the clamp guards against the rare eBPF-reader overflow).
func positiveI64ToU64(v int64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v)
}

// ResourcesToExtra maps the typed counters into the registry.extras
// shape (map[string]interface{}) so the persistent worker_registry row
// surfaces them via the legacy HTTP /admin/workers introspection path.
// Keys are snake_case to match the rest of the extras ecosystem; values
// are the raw scalars so existing JSON consumers can read them without
// a new schema negotiation. Network cumulative counters are included
// as TOTAL (not delta) — the registry's view is "where the worker is
// now", not "rate of change".
func ResourcesToExtra(r *pb.WorkerResourceCounters) map[string]interface{} {
	if r == nil {
		return nil
	}
	return map[string]interface{}{
		"cpu_utilization_ratio":        r.GetCpuUtilizationRatio(),
		"cpu_iowait_ratio":             r.GetCpuIowaitRatio(),
		"cpu_steal_ratio":              r.GetCpuStealRatio(),
		"cpu_user_ratio":               r.GetCpuUserRatio(),
		"cpu_system_ratio":             r.GetCpuSystemRatio(),
		"effective_cpu_cores":          r.GetEffectiveCpuCores(),
		"process_rss_bytes":            r.GetProcessRssBytes(),
		"process_rss_peak_bytes":       r.GetProcessRssPeakBytes(),
		"memory_used_bytes":            r.GetMemoryUsedBytes(),
		"memory_available_bytes":       r.GetMemoryAvailableBytes(),
		"memory_total_bytes":           r.GetMemoryTotalBytes(),
		"swap_used_bytes":              r.GetSwapUsedBytes(),
		"major_page_faults_total":      r.GetMajorPageFaultsTotal(),
		"page_cache_bytes":             r.GetPageCacheBytes(),
		"disk_read_bytes_total":        r.GetDiskReadBytesTotal(),
		"disk_write_bytes_total":       r.GetDiskWriteBytesTotal(),
		"disk_free_bytes":              r.GetDiskFreeBytes(),
		"temp_bytes_written":           r.GetTempBytesWritten(),
		"temp_files_open":              r.GetTempFilesOpen(),
		"scratch_current_bytes":        r.GetScratchCurrentBytes(),
		"scratch_peak_bytes":           r.GetScratchPeakBytes(),
		"disk_read_mbps":               r.GetDiskReadMbps(),
		"disk_write_mbps":              r.GetDiskWriteMbps(),
		"disk_io_wait_ms":              r.GetDiskIoWaitMs(),
		"network_receive_bytes_total":  r.GetNetworkReceiveBytesTotal(),
		"network_transmit_bytes_total": r.GetNetworkTransmitBytesTotal(),
		"network_retransmits_total":    r.GetNetworkRetransmitsTotal(),
		"download_mbps":                r.GetDownloadMbps(),
		"upload_mbps":                  r.GetUploadMbps(),
		"open_file_descriptors":        r.GetOpenFileDescriptors(),
		"max_file_descriptors":         r.GetMaxFileDescriptors(),
		"fd_utilization_ratio":         r.GetFileDescriptorUtilizationRatio(),
		"active_tasks":                 r.GetActiveTasks(),
		"task_slots":                   r.GetTaskSlots(),
		"render_jobs_active":           r.GetRenderJobsActive(),
		"prefetch_jobs_active":         r.GetPrefetchJobsActive(),
		"publisher_jobs_active":        r.GetPublisherJobsActive(),
		"load1":                        r.GetLoad1(),
		"run_queue":                    r.GetRunQueue(),
		"sampled_at":                   timeOrZero(r.GetSampledAt()),
	}
}

// timeOrZero stringifies the optional proto Timestamp into RFC3339
// if present, else returns "" so the JSON has a stable shape and
// downstream consumers don't branch on key presence.
func timeOrZero(ts interface{ AsTime() time.Time }) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339Nano)
}
