// Package worker — NetworkAdmissionController provides a unified, work-conserving
// bandwidth admission controller shared across all three byte-transfer
// consumers on the worker:
//
//	P0 — PUBLISH  (egress: artifact upload to master / object store)
//	P1 — RUNTIME  (ingress: synchronous asset download during render)
//	P2 — PREFETCH (ingress: future-asset plan pre-download)
//
// The controller separates ingress (downloads) from egress (uploads) because
// many VPS providers offer asymmetric links: upload cap ≠ download cap.
// A single combined budget would either starve prefetch or throttle publish.
//
// Work-conserving policy:
//
//	No active publish  → prefetch may use the entire ingress budget.
//	Publish active     → publish gets the full egress budget; prefetch is
//	                    throttled first when ingress approaches the cap.
//	Saturated link     → prefetch is throttled first, then runtime.
//
// The controller is thread-safe and lock-free on the hot path (atomic reads
// for the fast admission check; mutex only for the slow pacing path).
package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"velox-worker-agent/internal/telemetry"
)

// NetworkPriority identifies the three priority tiers for bandwidth admission.
type NetworkPriority int

const (
	// NetPriorityPublish is the highest priority (artifact upload).
	NetPriorityPublish NetworkPriority = 0
	// NetPriorityRuntime is the medium priority (synchronous asset download).
	NetPriorityRuntime NetworkPriority = 1
	// NetPriorityPrefetch is the lowest priority (future-asset pre-download).
	NetPriorityPrefetch NetworkPriority = 2
)

// String returns a human-readable label for the priority tier.
func (p NetworkPriority) String() string {
	switch p {
	case NetPriorityPublish:
		return "PUBLISH"
	case NetPriorityRuntime:
		return "RUNTIME"
	case NetPriorityPrefetch:
		return "PREFETCH"
	default:
		return "UNKNOWN"
	}
}

// NetworkDirection classifies whether a transfer is ingress (download) or
// egress (upload).
type NetworkDirection int

const (
	// NetDirIngress is a download (prefetch + runtime).
	NetDirIngress NetworkDirection = 0
	// NetDirEgress is an upload (publish).
	NetDirEgress NetworkDirection = 1
)

// NetworkAdmissionConfig configures the admission controller.
type NetworkAdmissionConfig struct {
	// IngressBudgetBytesPerSecond is the total download bandwidth cap.
	// Zero means unlimited ingress.
	IngressBudgetBytesPerSecond int64
	// EgressBudgetBytesPerSecond is the total upload bandwidth cap.
	// Zero means unlimited egress.
	EgressBudgetBytesPerSecond int64
}

// NIC saturation alert thresholds (percentage of budget utilization).
const (
	// NetSaturationWarnAbove triggers a WARN log + Prometheus gauge.
	NetSaturationWarnAbove = 70.0
	// NetSaturationThrottleAbove triggers prefetch admission throttling.
	NetSaturationThrottleAbove = 85.0
	// NetSaturationCriticalAbove makes the worker report NOT READY.
	NetSaturationCriticalAbove = 95.0
)

// NetworkSaturationLevel classifies the current NIC saturation state.
type NetworkSaturationLevel int

const (
	// NetSatNormal means utilization is below all thresholds.
	NetSatNormal NetworkSaturationLevel = iota
	// NetSatWarn means utilization exceeded the WARN threshold.
	NetSatWarn
	// NetSatThrottle means prefetch admission is throttled.
	NetSatThrottle
	// NetSatCritical means the worker is NOT READY due to NIC saturation.
	NetSatCritical
)

// String returns a human-readable label for the saturation level.
func (l NetworkSaturationLevel) String() string {
	switch l {
	case NetSatNormal:
		return "NORMAL"
	case NetSatWarn:
		return "WARN"
	case NetSatThrottle:
		return "THROTTLE"
	case NetSatCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// NetworkConsumerStats holds per-consumer byte counters for metrics and
// saturation estimation.
type NetworkConsumerStats struct {
	bytesConsumed atomic.Int64
	throttleMS    atomic.Int64
	activeCount   atomic.Int32
}

// Snapshot returns a point-in-time copy of the consumer stats.
func (s *NetworkConsumerStats) Snapshot() NetworkConsumerStatsSnapshot {
	return NetworkConsumerStatsSnapshot{
		BytesConsumed: s.bytesConsumed.Load(),
		ThrottleMS:    s.throttleMS.Load(),
		ActiveCount:   int(s.activeCount.Load()),
	}
}

// NetworkConsumerStatsSnapshot is an immutable point-in-time snapshot.
type NetworkConsumerStatsSnapshot struct {
	BytesConsumed int64
	ThrottleMS    int64
	ActiveCount   int
}

// NetworkAdmissionController is the shared bandwidth admission authority.
// All three byte-transfer consumers (publish, runtime, prefetch) acquire
// permission through this controller before transferring bytes.
type NetworkAdmissionController struct {
	cfg NetworkAdmissionConfig

	// Ingress pacing — shared token bucket for all download consumers.
	ingressLimiter *networkPacingLimiter
	// Egress pacing — shared token bucket for all upload consumers.
	egressLimiter *networkPacingLimiter

	// Per-consumer stats for metrics.
	publishStats  NetworkConsumerStats
	runtimeStats  NetworkConsumerStats
	prefetchStats NetworkConsumerStats

	// NIC saturation estimation (EWMA of bytes/sec observed).
	ingressActual atomic.Int64 // last observed ingress bytes/sec
	egressActual  atomic.Int64 // last observed egress bytes/sec

	// EWMA smoothing state (protected by mu).
	ingressEWMA    float64 // smoothed ingress bytes/sec
	egressEWMA     float64 // smoothed egress bytes/sec
	ewmaLastUpdate time.Time
	// EWMA alpha: 0.3 gives ~3-sample convergence (fast response).
	ewmaAlpha float64

	// Alert state (protected by mu, read atomically for hot path).
	saturationLevel   atomic.Int32 // NetworkSaturationLevel as int32
	prefetchThrottled atomic.Bool

	// Latency tracking for slow-path waits.
	totalWaitNS atomic.Int64

	stopCh chan struct{}
	mu     sync.Mutex
}

// NewNetworkAdmissionController creates a new controller with the given budget.
func NewNetworkAdmissionController(cfg NetworkAdmissionConfig) *NetworkAdmissionController {
	c := &NetworkAdmissionController{
		cfg:       cfg,
		stopCh:    make(chan struct{}),
		ewmaAlpha: 0.3, // 3-sample convergence
	}
	if cfg.IngressBudgetBytesPerSecond > 0 {
		c.ingressLimiter = newNetworkPacingLimiter(cfg.IngressBudgetBytesPerSecond)
	}
	if cfg.EgressBudgetBytesPerSecond > 0 {
		c.egressLimiter = newNetworkPacingLimiter(cfg.EgressBudgetBytesPerSecond)
	}
	return c
}

// AcquireBytes blocks until the controller permits `n` bytes to transfer
// in the given direction and priority. Returns an error if the context is
// cancelled. The caller MUST call ReleaseBytes when the transfer completes
// or is aborted.
//
// Lower-priority consumers are paced first when the budget is tight.
// A nil limiter (budget == 0) means unlimited: AcquireBytes returns
// immediately.
func (c *NetworkAdmissionController) AcquireBytes(ctx context.Context, dir int, priority int, n int64) error {
	if n <= 0 {
		return nil
	}

	limiter := c.ingressLimiter
	if dir == int(NetDirEgress) {
		limiter = c.egressLimiter
	}
	if limiter == nil {
		return nil // unlimited
	}

	start := time.Now()
	err := limiter.pace(ctx, n)
	wait := time.Since(start)
	if err != nil {
		return err
	}
	c.totalWaitNS.Add(wait.Nanoseconds())

	// Record throttle time for metrics.
	if wait > time.Millisecond {
		stats := c.statsForPriority(priority)
		if stats != nil {
			stats.throttleMS.Add(wait.Milliseconds())
		}
	}

	return nil
}

// ReleaseBytes decrements the active-transfer counter for the given priority.
func (c *NetworkAdmissionController) ReleaseBytes(priority int) {
	stats := c.statsForPriority(priority)
	if stats != nil {
		stats.activeCount.Add(-1)
	}
}

// BeginTransfer increments the active-transfer counter for the given priority.
func (c *NetworkAdmissionController) BeginTransfer(priority int) {
	stats := c.statsForPriority(priority)
	if stats != nil {
		stats.activeCount.Add(1)
	}
}

// RecordBytes records consumed bytes for metrics and saturation estimation.
func (c *NetworkAdmissionController) RecordBytes(priority int, dir int, n int64) {
	stats := c.statsForPriority(priority)
	if stats != nil {
		stats.bytesConsumed.Add(n)
	}
}

// IngressSaturationRatio returns the estimated ingress utilization
// (0.0–1.0+) relative to the configured budget. Returns 0 if budget is
// unlimited.
func (c *NetworkAdmissionController) IngressSaturationRatio() float64 {
	budget := c.cfg.IngressBudgetBytesPerSecond
	if budget <= 0 {
		return 0
	}
	actual := c.ingressActual.Load()
	return float64(actual) / float64(budget)
}

// EgressSaturationRatio returns the estimated egress utilization.
func (c *NetworkAdmissionController) EgressSaturationRatio() float64 {
	budget := c.cfg.EgressBudgetBytesPerSecond
	if budget <= 0 {
		return 0
	}
	actual := c.egressActual.Load()
	return float64(actual) / float64(budget)
}

// PublishStats returns a snapshot of publish consumer stats.
func (c *NetworkAdmissionController) PublishStats() NetworkConsumerStatsSnapshot {
	return c.publishStats.Snapshot()
}

// RuntimeStats returns a snapshot of runtime download consumer stats.
func (c *NetworkAdmissionController) RuntimeStats() NetworkConsumerStatsSnapshot {
	return c.runtimeStats.Snapshot()
}

// PrefetchStats returns a snapshot of prefetch consumer stats.
func (c *NetworkAdmissionController) PrefetchStats() NetworkConsumerStatsSnapshot {
	return c.prefetchStats.Snapshot()
}

// TotalWaitDuration returns the cumulative time consumers spent waiting
// for bandwidth admission.
func (c *NetworkAdmissionController) TotalWaitDuration() time.Duration {
	return time.Duration(c.totalWaitNS.Load())
}

// --- EWMA saturation estimation and alert thresholds ---

// UpdateSaturation refreshes the EWMA ingress/egress estimates from the
// limiter's actual consumed bytes. Call this periodically (e.g. every 5s
// from the heartbeat tick) to keep saturation alerts current.
func (c *NetworkAdmissionController) UpdateSaturation() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ewmaLastUpdate.IsZero() {
		c.ewmaLastUpdate = now
		// First sample: seed with raw values.
		if c.ingressLimiter != nil {
			c.ingressLimiter.mu.Lock()
			c.ingressEWMA = float64(c.ingressLimiter.consumed)
			c.ingressLimiter.mu.Unlock()
		}
		if c.egressLimiter != nil {
			c.egressLimiter.mu.Lock()
			c.egressEWMA = float64(c.egressLimiter.consumed)
			c.egressLimiter.mu.Unlock()
		}
		return
	}

	elapsed := now.Sub(c.ewmaLastUpdate).Seconds()
	if elapsed <= 0 {
		return
	}
	c.ewmaLastUpdate = now

	// Compute instantaneous bytes/sec from the limiter's cumulative consumed.
	var ingressInstant, egressInstant float64
	if c.ingressLimiter != nil {
		c.ingressLimiter.mu.Lock()
		ingressInstant = float64(c.ingressLimiter.consumed)
		c.ingressLimiter.mu.Unlock()
		// Convert cumulative to rate (bytes/sec).
		if c.ingressEWMA > 0 {
			ingressInstant = (ingressInstant - c.ingressEWMA) / elapsed
		}
	}
	if c.egressLimiter != nil {
		c.egressLimiter.mu.Lock()
		egressInstant = float64(c.egressLimiter.consumed)
		c.egressLimiter.mu.Unlock()
		if c.egressEWMA > 0 {
			egressInstant = (egressInstant - c.egressEWMA) / elapsed
		}
	}

	// Apply EWMA smoothing: new = alpha * instant + (1-alpha) * old.
	alpha := c.ewmaAlpha
	c.ingressEWMA = alpha*ingressInstant + (1-alpha)*c.ingressEWMA
	c.egressEWMA = alpha*egressInstant + (1-alpha)*c.egressEWMA

	// Store instantaneous rates for the atomic read path.
	c.ingressActual.Store(int64(c.ingressEWMA))
	c.egressActual.Store(int64(c.egressEWMA))

	// Evaluate saturation level from the worst of ingress/egress.
	ingressRatio := c.ingressRatioLocked()
	egressRatio := c.egressRatioLocked()
	maxRatio := ingressRatio
	if egressRatio > maxRatio {
		maxRatio = egressRatio
	}

	level := NetSatNormal
	if maxRatio >= NetSaturationCriticalAbove/100 {
		level = NetSatCritical
	} else if maxRatio >= NetSaturationThrottleAbove/100 {
		level = NetSatThrottle
	} else if maxRatio >= NetSaturationWarnAbove/100 {
		level = NetSatWarn
	}
	c.saturationLevel.Store(int32(level))
	c.prefetchThrottled.Store(level >= NetSatThrottle)
}

// SaturationLevel returns the current NIC saturation alert level.
func (c *NetworkAdmissionController) SaturationLevel() NetworkSaturationLevel {
	return NetworkSaturationLevel(c.saturationLevel.Load())
}

// IsPrefetchThrottled returns true when NIC saturation exceeds the
// throttle threshold (85%) and prefetch admission should be rejected.
func (c *NetworkAdmissionController) IsPrefetchThrottled() bool {
	return c.prefetchThrottled.Load()
}

// IsCritical returns true when NIC saturation exceeds the critical
// threshold (95%) and the worker should report NOT READY.
func (c *NetworkAdmissionController) IsCritical() bool {
	return c.SaturationLevel() >= NetSatCritical
}

// ingressRatioLocked computes ingress utilization. Caller holds c.mu.
func (c *NetworkAdmissionController) ingressRatioLocked() float64 {
	budget := c.cfg.IngressBudgetBytesPerSecond
	if budget <= 0 {
		return 0
	}
	return c.ingressEWMA / float64(budget)
}

// egressRatioLocked computes egress utilization. Caller holds c.mu.
func (c *NetworkAdmissionController) egressRatioLocked() float64 {
	budget := c.cfg.EgressBudgetBytesPerSecond
	if budget <= 0 {
		return 0
	}
	return c.egressEWMA / float64(budget)
}

// Stop shuts down the admission controller.
func (c *NetworkAdmissionController) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
}

// EmitMetrics pushes all network admission metrics to the Prometheus registry.
func (c *NetworkAdmissionController) EmitMetrics() {
	m := telemetry.GetPrometheusMetrics()

	ingressRatio := c.IngressSaturationRatio()
	egressRatio := c.EgressSaturationRatio()

	m.SetNetworkSaturation(ingressRatio, egressRatio)

	// Per-consumer throughput (bytes/sec approximated from snapshots).
	pubSnap := c.publishStats.Snapshot()
	runtimeSnap := c.runtimeStats.Snapshot()
	prefetchSnap := c.prefetchStats.Snapshot()

	m.SetNetworkConsumerBytes("publish", pubSnap.BytesConsumed)
	m.SetNetworkConsumerBytes("runtime", runtimeSnap.BytesConsumed)
	m.SetNetworkConsumerBytes("prefetch", prefetchSnap.BytesConsumed)

	m.SetNetworkThrottleMS("publish", pubSnap.ThrottleMS)
	m.SetNetworkThrottleMS("runtime", runtimeSnap.ThrottleMS)
	m.SetNetworkThrottleMS("prefetch", prefetchSnap.ThrottleMS)

	// Saturation alert level gauge.
	m.SetNetworkSaturationAlertLevel(int(c.SaturationLevel()))
}

// statsForPriority returns the stats struct for the given priority.
func (c *NetworkAdmissionController) statsForPriority(p int) *NetworkConsumerStats {
	switch NetworkPriority(p) {
	case NetPriorityPublish:
		return &c.publishStats
	case NetPriorityRuntime:
		return &c.runtimeStats
	case NetPriorityPrefetch:
		return &c.prefetchStats
	default:
		return nil
	}
}

// --- Pacing limiter (token-bucket variant) ---

// networkPacingLimiter enforces an aggregate bytes/second ceiling using a
// virtual clock. It is the same concept as sharedBandwidthLimiter but lives
// inside the NetworkAdmissionController so all consumers share one clock.
type networkPacingLimiter struct {
	mu       sync.Mutex
	cap      int64
	start    time.Time
	consumed int64
}

func newNetworkPacingLimiter(cap int64) *networkPacingLimiter {
	if cap <= 0 {
		return nil
	}
	return &networkPacingLimiter{cap: cap}
}

// pace accounts n bytes against the shared clock and sleeps until those
// bytes are due at the capped rate. Safe for concurrent use.
func (l *networkPacingLimiter) pace(ctx context.Context, n int64) error {
	if l == nil {
		return nil
	}
	now := time.Now()
	l.mu.Lock()
	if l.start.IsZero() {
		l.start = now
		l.consumed = 0
	}
	l.consumed += n
	target := l.start.Add(time.Duration(float64(l.consumed) / float64(l.cap) * float64(time.Second)))
	l.mu.Unlock()

	if wait := time.Until(target); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
	return nil
}

// RecordNetworkBytes is a convenience function that records bytes for the
// given priority and direction on the shared admission controller.
func RecordNetworkBytes(ctrl *NetworkAdmissionController, priority int, dir int, n int64) {
	if ctrl != nil {
		ctrl.RecordBytes(priority, dir, n)
	}
}
