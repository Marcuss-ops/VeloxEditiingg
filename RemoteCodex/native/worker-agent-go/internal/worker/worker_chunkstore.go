package worker

import (
	"velox-worker-agent/internal/chunkfactory"
)

// AttachChunkStore is used only by the composition root while the worker is
// still stopped. It wires the W5-precursor content-addressed chunk store
// (internal/chunkfactory) as an optional capability: a nil store is a valid
// DISABLED state (the chunk pipeline has no consumer on the default render
// path yet), while a non-nil store is READY for the manifest-first delivery
// path. Mirrors the AttachClipCache seam pattern.
func (w *Worker) AttachChunkStore(store *chunkfactory.Store) {
	if w == nil {
		return
	}
	w.chunkStore = store
}

// ChunkStore returns the attached chunk store, or nil when the capability is
// DISABLED (no store wired by the composition root).
func (w *Worker) ChunkStore() *chunkfactory.Store {
	if w == nil {
		return nil
	}
	return w.chunkStore
}
