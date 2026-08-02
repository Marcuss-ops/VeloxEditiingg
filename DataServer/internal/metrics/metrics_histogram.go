package metrics

// metrics_histogram.go: the histogramData backing type for
// HistogramFamily. Split out of metrics.go; the Family/Registry core
// lives in metrics.go and the exposition helpers in metrics_write.go.

import "sync"

// ── histogram helpers ─────────────────────────────────────────────────────

type histogramData struct {
	mu      sync.RWMutex
	count   uint64
	sum     float64
	buckets []float64
	counts  []uint64 // cumulative bucket counts (≤ b)
}

func newHistogramData(buckets []float64) *histogramData {
	return &histogramData{
		buckets: append([]float64(nil), buckets...),
		counts:  make([]uint64, len(buckets)),
	}
}

func (h *histogramData) observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
		}
	}
}

// bucketLE returns the cumulative count with `v <= b`. Iterates and
// returns the first bucket whose upper bound is ≥ b (Prometheus
// convention: cumulative counts are reported against `le`). Falls
// through to `count` (the implicit +Inf bucket) when `b` exceeds all
// explicit bucket boundaries.
func (h *histogramData) bucketLE(b float64) uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for i, ub := range h.buckets {
		if b <= ub {
			return h.counts[i]
		}
	}
	return h.count
}

func (h *histogramData) snapshot() *histogramData {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := &histogramData{
		count:   h.count,
		sum:     h.sum,
		buckets: append([]float64(nil), h.buckets...),
		counts:  append([]uint64(nil), h.counts...),
	}
	return out
}
