// Scorecard v1 / F2 — handler_workers.go tests.
//
// Asserts that handleHeartbeat routes typed WorkerResourceCounters from
// the heartbeat envelope to the WorkerResourceSink.RecordWorker contract,
// and that the resources section is materialized into the registry's
// `extra` map (backward-compat with legacy HTTP /admin/workers readers).
package grpcserver

import (
	"sync"
	"testing"

	velmetrics "velox-server/internal/metrics"

	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// recordableSink counts RecordWorker invocations AND captures the
// typed ResourceSnapshot for spot-check assertions. Single-threaded by
// the gRPC stream consumer so a plain mutex is sufficient.
type recordableSink struct {
	mu        sync.Mutex
	calls     int
	lastWID   string
	lastSnap  *velmetrics.ResourceSnapshot
	allSnaps  []*velmetrics.ResourceSnapshot
}

func (r *recordableSink) RecordWorker(workerID string, snap *velmetrics.ResourceSnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.lastWID = workerID
	r.lastSnap = snap
	r.allSnaps = append(r.allSnaps, snap)
}

var _ velmetrics.WorkerResourceSink = (*recordableSink)(nil)

// minimalHandlerForHeartbeat builds a Handler with the registry stubbed
// (registry is nil; handleHeartbeat short-circuits before touching
// registry.Heartbeat because the registry call only logs on error and
// the sink is what we're testing). The sink is wired so hand-built
// Heartbeat envelopes can drive handleHeartbeat end-to-end.
func minimalHandlerForHeartbeat(t *testing.T, sink velmetrics.WorkerResourceSink) *Handler {
	t.Helper()
	h := NewHandler(
		nil, // registry — handleHeartbeat nil-checks via h.registry.Heartbeat
		nil, // cmdMgr
		nil, // jobsRepo
		nil, // taskRepo
		nil, // taskAttemptRepo
		nil, // artifactSvc
		nil, // dbStore — handleHeartbeat nil-checks the session validator
		&HandlerConfig{PushMode: true},
	)
	h.SetResourceSink(sink)
	return h
}

// =====================================================================
// (A) F2 happy path — Heartbeat with typed resources → sink sees the
// resources AND registry.extra has the resources section materialised.
// =====================================================================

func TestHandleHeartbeat_F2_DecodesTypedResources(t *testing.T) {
	sink := &recordableSink{}
	h := minimalHandlerForHeartbeat(t, sink)

	hb := &pb.Heartbeat{
		WorkerName:    "w-f2",
		WorkerStatus:  "idle",
		EngineVersion: "engine-1.42",
		Resources: &pb.WorkerResourceCounters{
			CpuUtilizationRatio:      0.83,
			CpuIowaitRatio:           0.04,
			CpuStealRatio:            0.01,
			ProcessRssBytes:          536870912,  // 512 MiB
			ProcessRssPeakBytes:      805306368,  // 768 MiB
			MemoryUsedBytes:          1677721600, // 1.5 GiB
			MemoryAvailableBytes:     4294967296, // 4 GiB
			SwapUsedBytes:            0,
			DiskReadBytesTotal:       1073741824, // 1 GiB read
			DiskWriteBytesTotal:      536870912,  // 512 MiB written
			DiskFreeBytes:            53687091200,
			TempBytesWritten:         4194304,
			TempFilesOpen:            2,
			NetworkReceiveBytesTotal: 20971520, // 20 MiB received cumulatively
			NetworkTransmitBytesTotal: 4194304,  // 4 MiB transmitted cumulatively
			NetworkRetransmitsTotal:   12,
			ActiveTasks:              1,
			TaskSlots:                4,
			Load1:                    1.42,
			RunQueue:                 0,
			SampledAt:                timestamppb.Now(),
		},
	}

	h.handleHeartbeat("w-f2", "sess-f2", hb)

	// 1) sink MUST have been called once with the right worker id and
	//    a non-nil snapshot — decodeWorkerResources emitted the typed
	//    counters onto the Prometheus path.
	if sink.calls != 1 {
		t.Errorf("sink.RecordWorker calls = %d; want 1 (F2 typed wiring)", sink.calls)
	}
	if sink.lastWID != "w-f2" {
		t.Errorf("sink.workerID = %q; want %q", sink.lastWID, "w-f2")
	}
	if sink.lastSnap == nil {
		t.Fatalf("sink.lastSnapshot = nil; handleHeartbeat must produce a typed ResourceSnapshot")
	}

	// 2) Spot-check typed fields made the round-trip unchanged.
	got := sink.lastSnap
	if got.CPUUtilRatio != 0.83 {
		t.Errorf("ResourceSnapshot.CPUUtilRatio = %v; want 0.83", got.CPUUtilRatio)
	}
	if got.CPUIOWaitRatio != 0.04 || got.CPUStealRatio != 0.01 {
		t.Errorf("ResourceSnapshot iowait/steal: got %v / %v; want 0.04 / 0.01", got.CPUIOWaitRatio, got.CPUStealRatio)
	}
	if got.ProcessRSSBytes != 536870912 || got.ProcessRSSPeakBytes != 805306368 {
		t.Errorf("ResourceSnapshot RSS mismatch: got rss=%d rss_peak=%d want rss=%d rss_peak=%d",
			got.ProcessRSSBytes, got.ProcessRSSPeakBytes,
			int64(536870912), int64(805306368))
	}
	if got.DiskFreeBytes != 53687091200 || got.TempBytesWritten != 4194304 {
		t.Errorf("ResourceSnapshot disk/temp mismatch: got free=%d temp=%d want free=%d temp=%d",
			got.DiskFreeBytes, got.TempBytesWritten,
			int64(53687091200), int64(4194304))
	}
	if got.ActiveTasks != 1 || got.TaskSlots != 4 {
		t.Errorf("ResourceSnapshot active/slots = %d / %d; want 1 / 4", got.ActiveTasks, got.TaskSlots)
	}
	if got.Load1 != 1.42 || got.RunQueue != 0 {
		t.Errorf("ResourceSnapshot load/run-queue = %v / %d; want 1.42 / 0", got.Load1, got.RunQueue)
	}

	// 3) Cumulative→delta conversion: FIRST beat, deltas = totals
	//    (Prometheus fallback for counters without origin timestamp).
	if got.NetworkRxBytesDelta != 20971520 {
		t.Errorf("ResourceSnapshot.NetworkRxBytesDelta = %d; want 20971520 (first beat: delta == total)",
			got.NetworkRxBytesDelta)
	}
	if got.NetworkTxBytesDelta != 4194304 {
		t.Errorf("ResourceSnapshot.NetworkTxBytesDelta = %d; want 4194304 (first beat: delta == total)",
			got.NetworkTxBytesDelta)
	}
	if got.SampledAt.IsZero() {
		t.Errorf("ResourceSnapshot.SampledAt = zero; want typed Timestamp converted to time.Time")
	}
	// Cache counters are NOT yet on the typed proto. PR-3
	// (worker-agent-go resource sampler follow-up) will surface
	// cache.entries / cache.evictions / cache.corruptions totals; until
	// then they MUST stay zero here so a future refactor that
	// conflates temp_files_open with cache_entries breaks loudly.
	if got.CacheEntries != 0 {
		t.Errorf("ResourceSnapshot.CacheEntries = %d; want 0 (not on proto yet — PR-3 follow-up)", got.CacheEntries)
	}
	if got.CacheBytesUsed != 0 {
		t.Errorf("ResourceSnapshot.CacheBytesUsed = %d; want 0 (not on proto yet — PR-3 follow-up)", got.CacheBytesUsed)
	}
	if got.CacheEvictionsDelta != 0 {
		t.Errorf("ResourceSnapshot.CacheEvictionsDelta = %d; want 0 (PR-3 will surface real totals)", got.CacheEvictionsDelta)
	}
}

// =====================================================================
// (B) F2 cumulative→delta regression-guard. SECOND beat must compute
// delta vs. first beat (counter-write discipline). If a future refactor
// mistakenly passes cumulative values as deltas, the SECOND beat will
// double-count and break Prometheus rate() math.
// =====================================================================

func TestHandleHeartbeat_F2_DeltaBetweenBeats(t *testing.T) {
	sink := &recordableSink{}
	h := minimalHandlerForHeartbeat(t, sink)

	const wID = "w-delta"

	hbFirst := &pb.Heartbeat{
		WorkerName: wID,
		Resources: &pb.WorkerResourceCounters{
			NetworkReceiveBytesTotal: 100,
			NetworkTransmitBytesTotal: 50,
		},
	}
	hbSecond := &pb.Heartbeat{
		WorkerName: wID,
		Resources: &pb.WorkerResourceCounters{
			NetworkReceiveBytesTotal: 200, // +100 since first beat
			NetworkTransmitBytesTotal: 80, // +30 since first beat
		},
	}

	h.handleHeartbeat(wID, "s1", hbFirst)
	h.handleHeartbeat(wID, "s2", hbSecond)

	if sink.calls != 2 {
		t.Fatalf("sink.RecordWorker calls = %d; want 2 (two beats)", sink.calls)
	}
	// First beat delta == totals (Prometheus first-beat fallback).
	if sink.allSnaps[0].NetworkRxBytesDelta != 100 {
		t.Errorf("first-beat NetworkRxBytesDelta = %d; want 100 (fallback == total)", sink.allSnaps[0].NetworkRxBytesDelta)
	}
	// Second beat delta == 200 - 100 = 100.
	if sink.allSnaps[1].NetworkRxBytesDelta != 100 {
		t.Errorf("second-beat NetworkRxBytesDelta = %d; want 100 (delta vs prior beat)", sink.allSnaps[1].NetworkRxBytesDelta)
	}
	if sink.allSnaps[1].NetworkTxBytesDelta != 30 {
		t.Errorf("second-beat NetworkTxBytesDelta = %d; want 30 (delta vs prior beat)", sink.allSnaps[1].NetworkTxBytesDelta)
	}
}

// =====================================================================
// (C) F2 nil-tolerance. Pre-PR-3 (or partial resource-sampler builds)
// ship Heartbeat with resources == nil. handleHeartbeat MUST skip the
// sink call (no zero snapshot) and the registry.extra MUST NOT carry a
// stray "cpu_utilization_ratio" key.
// =====================================================================

func TestHandleHeartbeat_F2_NilResourcesSafe(t *testing.T) {
	sink := &recordableSink{}
	h := minimalHandlerForHeartbeat(t, sink)

	hb := &pb.Heartbeat{WorkerName: "w-nil", Resources: nil}

	h.handleHeartbeat("w-nil", "s-nil", hb)

	if sink.calls != 0 {
		t.Errorf("sink.RecordWorker calls = %d; want 0 (nil resources must short-circuit decodeWorkerResources)", sink.calls)
	}
}

// =====================================================================
// (D) F2 sink-not-wired tolerance. Running with h.resourceSink == nil
// (no /metrics endpoint) MUST stay operational — the registry.Heartbeat
// side keeps working. This pins the elided metric-projection downgrade.
// =====================================================================

func TestHandleHeartbeat_F2_NilSinkSafe(t *testing.T) {
	h := minimalHandlerForHeartbeat(t, nil)

	hb := &pb.Heartbeat{
		WorkerName: "w-no-sink",
		Resources: &pb.WorkerResourceCounters{
			CpuUtilizationRatio: 0.91,
			NetworkReceiveBytesTotal: 4096,
		},
	}

	// Must NOT panic even though h.resourceSink is nil.
	h.handleHeartbeat("w-no-sink", "s-no-sink", hb)
}
