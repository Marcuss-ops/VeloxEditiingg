// Package metrics / collector_health.go
//
// Master-side health gauges + worker heartbeat-age tracker's stamped
// side-effects, sliced out of collector.go so the Collector struct
// definition stays focused on registration.
//
// Two distinct exposure points into the same per-tick supervisor
// surface:
//
//   - RecordMasterHealth -> the 3 master health gauges (RSS,
//     goroutines, outbox pending) refreshed by the supervisor
//     goroutine.
//   - AverageHeartbeatAge (wrapper around averageHeartbeatAge) ->
//     per-worker velox_master_worker_heartbeat_age_seconds gauge.
//
// readProcessRSS is the master-side /proc/self/status VmRSS reader
// with a 250 ms cache to keep the per-tick health refresh cheap
// (gauges are emitted in supervisor ticks; avoiding the syscall
// per tick keeps the loop on its 15 s cadence).
package metrics

import (
	"runtime"
	"sync/atomic"
	"time"
)

// RecordMasterHealth refreshes the master-side gauges every few seconds.
// Called from a supervisor goroutine.
func (c *Collector) RecordMasterHealth(outboxPending int) {
	c.masterRssBytes.GaugeSet([]string{}, readProcessRSS())
	c.masterGoroutines.GaugeSet([]string{}, int64(runtime.NumGoroutine()))
	c.masterOutboxPending.GaugeSet([]string{}, int64(outboxPending))
}

// averageHeartbeatAge walks the lastSeen map and stamps each worker's
// heartbeat-age gauge. Called from the supervisor loop.
func (c *Collector) averageHeartbeatAge(now time.Time) {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	for w, ts := range c.lastSeen {
		age := now.Sub(ts).Seconds()
		c.heartbeatAge.GaugeSet([]string{w}, int64(age))
	}
}

// ── cheap master-side helpers ──────────────────────────────────────

var _rssCache atomic.Int64
var _rssCacheAt atomic.Int64

func readProcessRSS() int64 {
	// Read /proc/self/status VmRSS. Cached for ~250ms because gauges
	// are emitted in supervisor ticks and avoiding the syscall per
	// tick keeps the loop cheap.
	now := time.Now().UnixMilli()
	if cached := _rssCache.Load(); now-_rssCacheAt.Load() < 250 {
		if cached > 0 {
			return cached
		}
	}
	got := readRSSFromProc()
	_rssCache.Store(got)
	_rssCacheAt.Store(now)
	return got
}

// AverageHeartbeatAge wraps averageHeartbeatAge for callers who want
// to drive it from a parent goroutine.
func (c *Collector) AverageHeartbeatAge(now time.Time) {
	c.averageHeartbeatAge(now)
}
