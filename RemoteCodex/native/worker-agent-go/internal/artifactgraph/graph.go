// Package artifactgraph — per-attempt intermediate-file telemetry (Fase E2).
//
// AttemptArtifactGraph is the canonical ledger for every intermediate file a
// render attempt produces. Each record carries:
//
//	path, created_at, size, written_bytes, read_bytes, lifetime,
//	producer_phase, consumer_phase
//
// The purpose is PROFILING BEFORE OPTIMIZATION: once real jobs flow through
// the graph, Candidates() surfaces files that are written and then read
// back (e.g. a 340 MB segment written and immediately consumed by concat),
// so intermediate-file elimination is driven by evidence — never removed a
// priori. This package is intentionally free of executor knowledge: it
// records what the executor reports through TrackWriter/TrackReader or the
// plain Create/RecordWrite/RecordRead calls.
package artifactgraph

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// Record is the immutable snapshot of one intermediate file's lifecycle.
type Record struct {
	// Path is the resolved file path (or the executor's canonical key).
	Path string `json:"path"`
	// ProducerPhase is the phase that created the file (e.g. "segment_render",
	// "audio_encode", "concat", "mux").
	ProducerPhase string `json:"producer_phase"`
	// ConsumerPhase is the FIRST phase that read the file back; empty until
	// the first read. Read bytes from later consumers still accumulate.
	ConsumerPhase string `json:"consumer_phase"`
	// CreatedAt is when the file was first registered.
	CreatedAt time.Time `json:"created_at"`
	// ClosedAt is when the file was finalized (Close); zero if never closed.
	ClosedAt time.Time `json:"closed_at,omitempty"`
	// SizeBytes is the final size: the producer-supplied expected size when
	// given, otherwise written_bytes at Close.
	SizeBytes int64 `json:"size_bytes"`
	// WrittenBytes is the cumulative bytes written (write-once intermediates
	// equal SizeBytes; rewritten files accumulate).
	WrittenBytes int64 `json:"written_bytes"`
	// ReadBytes is the cumulative bytes read back by all consumers.
	ReadBytes int64 `json:"read_bytes"`
	// Lifetime is ClosedAt-CreatedAt; zero until Close. The prime candidate
	// signature is a large ReadBytes with a tiny Lifetime.
	Lifetime time.Duration `json:"lifetime_ns"`
}

// Candidate is a write-then-read file surfaced for profiling. ReReadBytes is
// the avoidable I/O: the bytes written that were subsequently read back
// (min(written, read) when the consumer re-read the whole file).
type Candidate struct {
	Record
	// ReReadBytes is min(WrittenBytes, ReadBytes) — the I/O that could be
	// avoided if the producer handed bytes straight to the consumer.
	ReReadBytes int64 `json:"reread_bytes"`
	// Reason is a short human explanation of why this file ranked.
	Reason string `json:"reason"`
}

// immediateReadThreshold is the heuristic for "written then immediately read
// back": candidates with a shorter lifetime get the stronger reason tag.
// It is a profiling signal, not a contract — tune per fleet.
const immediateReadThreshold = 2 * time.Minute

// Graph is the per-attempt ledger. One instance per attempt — never shared
// across attempts, never global (same rule as the AttemptEventMachine). All
// methods are safe for concurrent use.
type Graph struct {
	mu    sync.Mutex
	files map[string]*Record
	order []string
	now   func() time.Time
}

// New creates an empty graph using time.Now.
func New() *Graph {
	return NewWithClock(time.Now)
}

// NewWithClock creates a graph with an injectable clock (deterministic tests).
func NewWithClock(now func() time.Time) *Graph {
	if now == nil {
		now = time.Now
	}
	return &Graph{files: make(map[string]*Record), now: now}
}

// Create registers a file produced by producerPhase. Idempotent per path:
// the first producer owns the record; a later Create for the same path is a
// no-op so re-truncated files keep one record.
func (g *Graph) Create(path, producerPhase string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.files[path]; ok {
		return
	}
	g.files[path] = &Record{
		Path:          path,
		ProducerPhase: producerPhase,
		CreatedAt:     g.now(),
	}
	g.order = append(g.order, path)
}

// CreateWithSize registers a file with a producer-known expected size (e.g.
// the sizeBytes the storage resolver used for placement). Negative sizes are
// clamped to zero; SizeBytes still falls back to written_bytes at Close.
func (g *Graph) CreateWithSize(path, producerPhase string, sizeBytes int64) {
	if sizeBytes < 0 {
		sizeBytes = 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.files[path]; ok {
		return
	}
	g.files[path] = &Record{
		Path:          path,
		ProducerPhase: producerPhase,
		CreatedAt:     g.now(),
		SizeBytes:     sizeBytes,
	}
	g.order = append(g.order, path)
}

// RecordWrite accumulates written bytes for an already-registered path.
// Reads/writes for unregistered paths are silently ignored (the producer
// registers the file; readers never invent records).
func (g *Graph) RecordWrite(path string, n int64) {
	if n <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if rec := g.files[path]; rec != nil {
		rec.WrittenBytes += n
	}
}

// RecordRead accumulates read bytes for an already-registered path and pins
// the first consumer phase.
func (g *Graph) RecordRead(path string, n int64, consumerPhase string) {
	if n <= 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	rec := g.files[path]
	if rec == nil {
		return
	}
	if rec.ConsumerPhase == "" && consumerPhase != "" {
		rec.ConsumerPhase = consumerPhase
	}
	rec.ReadBytes += n
}

// Close finalizes a file: stamps ClosedAt, derives Lifetime, and falls back
// to SizeBytes=WrittenBytes when no expected size was given. Idempotent.
func (g *Graph) Close(path string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	rec := g.files[path]
	if rec == nil {
		return
	}
	if rec.ClosedAt.IsZero() {
		rec.ClosedAt = g.now()
	}
	if rec.SizeBytes == 0 {
		rec.SizeBytes = rec.WrittenBytes
	}
	if !rec.ClosedAt.IsZero() && !rec.CreatedAt.IsZero() {
		rec.Lifetime = rec.ClosedAt.Sub(rec.CreatedAt)
	}
}

// Snapshot returns an immutable copy of all records ordered by creation
// time (stable).
func (g *Graph) Snapshot() []Record {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]Record, 0, len(g.order))
	for _, path := range g.order {
		out = append(out, *g.files[path])
	}
	return out
}

// Candidates returns write-then-read files ranked by avoidable I/O. A file
// qualifies only when it was BOTH written and read back. Ranking: ReReadBytes
// descending (340 MB re-read beats 1 MB), then Lifetime ascending (a file
// written and immediately re-read is the prime candidate).
func (g *Graph) Candidates() []Candidate {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.candidatesLocked()
}

// candidatesLocked computes the candidate list; caller holds g.mu.
func (g *Graph) candidatesLocked() []Candidate {
	var out []Candidate
	for _, path := range g.order {
		rec := g.files[path]
		if rec.WrittenBytes <= 0 || rec.ReadBytes <= 0 {
			continue
		}
		reread := rec.ReadBytes
		if rec.WrittenBytes < reread {
			reread = rec.WrittenBytes
		}
		reason := "written then read back"
		if rec.Lifetime > 0 && rec.Lifetime < immediateReadThreshold {
			reason = "written then immediately read back"
		}
		out = append(out, Candidate{Record: *rec, ReReadBytes: reread, Reason: reason})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ReReadBytes != out[j].ReReadBytes {
			return out[i].ReReadBytes > out[j].ReReadBytes
		}
		return out[i].Lifetime < out[j].Lifetime
	})
	return out
}

// Summary aggregates the ledger for logging and heartbeat exposure.
type Summary struct {
	// GraphVersion is the ledger schema version (bump on field changes).
	GraphVersion      int         `json:"attempt_graph_version"`
	FileCount         int         `json:"file_count"`
	TotalWrittenBytes int64       `json:"total_written_bytes"`
	TotalReadBytes    int64       `json:"total_read_bytes"`
	TotalReReadBytes  int64       `json:"total_reread_bytes"`
	Candidates        []Candidate `json:"candidates,omitempty"`
}

// Summary returns the aggregate view of the ledger. Totals, the candidate
// list and the re-read total are computed under a SINGLE lock acquisition so
// the summary is internally consistent even if the executor is still
// mutating the graph concurrently.
func (g *Graph) Summary() Summary {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := Summary{GraphVersion: 1, FileCount: len(g.order)}
	for _, path := range g.order {
		rec := g.files[path]
		s.TotalWrittenBytes += rec.WrittenBytes
		s.TotalReadBytes += rec.ReadBytes
	}
	s.Candidates = g.candidatesLocked()
	for _, c := range s.Candidates {
		s.TotalReReadBytes += c.ReReadBytes
	}
	return s
}

// MarshalJSON serializes the full ledger (paths + lifecycle) for debugging.
func (g *Graph) MarshalJSON() ([]byte, error) {
	return json.Marshal(g.Snapshot())
}
