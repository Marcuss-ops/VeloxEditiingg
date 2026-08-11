package artifactgraph

import (
	"io"
	"sync"
)

// TrackedWriter wraps an io.Writer, registering the file on first Write and
// accounting every byte written. Close finalizes the record (lifetime +
// size) and forwards to the underlying writer when it is an io.Closer. It
// is the canonical adapter for producer phases (segment render, concat,
// audio encode, mux): wrap the output stream once at phase start.
type TrackedWriter struct {
	graph  *Graph
	path   string
	phase  string
	w      io.Writer
	closer io.Closer
	once   sync.Once
}

// TrackWriter wraps w so writes to path are attributed to producerPhase.
func (g *Graph) TrackWriter(path, producerPhase string, w io.Writer) *TrackedWriter {
	tw := &TrackedWriter{graph: g, path: path, phase: producerPhase, w: w}
	if c, ok := w.(io.Closer); ok {
		tw.closer = c
	}
	return tw
}

// Write forwards to the underlying writer and records the bytes.
func (t *TrackedWriter) Write(p []byte) (int, error) {
	if t == nil || t.w == nil {
		return 0, io.ErrClosedPipe
	}
	t.once.Do(func() {
		if t.graph != nil {
			t.graph.Create(t.path, t.phase)
		}
	})
	n, err := t.w.Write(p)
	if t.graph != nil {
		t.graph.RecordWrite(t.path, int64(n))
	}
	return n, err
}

// Close finalizes the record and closes the underlying writer when it
// implements io.Closer.
func (t *TrackedWriter) Close() error {
	if t == nil {
		return nil
	}
	if t.graph != nil {
		t.graph.Close(t.path)
	}
	if t.closer != nil {
		return t.closer.Close()
	}
	return nil
}

// TrackedReader wraps an io.Reader, accounting every byte read from an
// already-registered file. It NEVER registers files: the producer owns the
// record; reads of unregistered paths are silently ignored. consumerPhase is
// pinned on the first read (the file's first consumer).
type TrackedReader struct {
	graph *Graph
	path  string
	phase string
	r     io.Reader
}

// TrackReader wraps r so reads from path are attributed to consumerPhase.
func (g *Graph) TrackReader(path, consumerPhase string, r io.Reader) *TrackedReader {
	return &TrackedReader{graph: g, path: path, phase: consumerPhase, r: r}
}

// Read forwards to the underlying reader and records the bytes.
func (t *TrackedReader) Read(p []byte) (int, error) {
	if t == nil || t.r == nil {
		return 0, io.EOF
	}
	n, err := t.r.Read(p)
	if t.graph != nil {
		t.graph.RecordRead(t.path, int64(n), t.phase)
	}
	return n, err
}
