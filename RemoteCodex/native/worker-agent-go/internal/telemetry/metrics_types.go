// Package telemetry provides Prometheus metrics collection for the worker agent.
package telemetry

import (
	"fmt"
	"sync"
)

// HistogramVec represents a Prometheus histogram metric.
type HistogramVec struct {
	Name    string
	Help    string
	Buckets []float64
	mu      sync.RWMutex
	values  map[string]*histogramData
}

type histogramData struct {
	count   int64
	sum     float64
	buckets map[float64]int64
}

// CounterVec represents a Prometheus counter metric.
type CounterVec struct {
	Name   string
	Help   string
	mu     sync.RWMutex
	values map[string]float64
}

// GaugeVec represents a Prometheus gauge metric.
type GaugeVec struct {
	Name   string
	Help   string
	mu     sync.RWMutex
	values map[string]float64
}

// HistogramVec methods

func (h *HistogramVec) observe(label string, value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.values[label] == nil {
		h.values[label] = &histogramData{buckets: make(map[float64]int64)}
	}
	data := h.values[label]
	data.count++
	data.sum += value
	for _, bucket := range h.Buckets {
		if value <= bucket {
			data.buckets[bucket]++
		}
	}
}

func (h *HistogramVec) percentile(p float64) float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var totalCount int64
	var totalSum float64
	for _, data := range h.values {
		totalCount += data.count
		totalSum += data.sum
	}
	if totalCount == 0 {
		return 0
	}

	targetCount := int64(float64(totalCount) * p)
	var cumulative int64
	for _, bucket := range h.Buckets {
		for _, data := range h.values {
			cumulative += data.buckets[bucket]
		}
		if cumulative >= targetCount {
			return bucket
		}
	}
	return totalSum / float64(totalCount)
}

func (h *HistogramVec) average() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var totalCount int64
	var totalSum float64
	for _, data := range h.values {
		totalCount += data.count
		totalSum += data.sum
	}
	if totalCount == 0 {
		return 0
	}
	return totalSum / float64(totalCount)
}

func (h *HistogramVec) export() string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var output string
	output += fmt.Sprintf("# HELP %s %s\n", h.Name, h.Help)
	output += fmt.Sprintf("# TYPE %s histogram\n", h.Name)
	for label, data := range h.values {
		for _, bucket := range h.Buckets {
			output += fmt.Sprintf("%s_bucket{le=\"%g\",label=\"%s\"} %d\n", h.Name, bucket, label, data.buckets[bucket])
		}
		output += fmt.Sprintf("%s_bucket{le=\"+Inf\",label=\"%s\"} %d\n", h.Name, label, data.count)
		output += fmt.Sprintf("%s_sum{label=\"%s\"} %g\n", h.Name, label, data.sum)
		output += fmt.Sprintf("%s_count{label=\"%s\"} %d\n", h.Name, label, data.count)
	}
	return output
}

// CounterVec methods

func (c *CounterVec) inc(label string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[label]++
}

func (c *CounterVec) get(label string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.values[label]
}

func (c *CounterVec) total() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var total float64
	for _, v := range c.values {
		total += v
	}
	return total
}

func (c *CounterVec) export() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var output string
	output += fmt.Sprintf("# HELP %s %s\n", c.Name, c.Help)
	output += fmt.Sprintf("# TYPE %s counter\n", c.Name)
	for label, value := range c.values {
		if c.Name == "velox_fallback_count_total" || c.Name == "velox_python_emergency_path_total" {
			output += fmt.Sprintf("%s{reason=\"%s\"} %g\n", c.Name, label, value)
		} else {
			output += fmt.Sprintf("%s{label=\"%s\"} %g\n", c.Name, label, value)
		}
	}
	return output
}

// GaugeVec methods

func (g *GaugeVec) set(label string, value float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.values[label] = value
}

func (g *GaugeVec) export() string {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var output string
	output += fmt.Sprintf("# HELP %s %s\n", g.Name, g.Help)
	output += fmt.Sprintf("# TYPE %s gauge\n", g.Name)
	for label, value := range g.values {
		output += fmt.Sprintf("%s{label=\"%s\"} %g\n", g.Name, label, value)
	}
	return output
}
