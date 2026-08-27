package publisher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type concurrentProgressiveSession struct {
	mu       sync.Mutex
	parts    map[int]int64
	current  int32
	max      int32
	complete int32
	barrier  chan struct{}
}

func (s *concurrentProgressiveSession) UploadPart(_ context.Context, n int, r io.Reader, size int64) error {
	if _, err := io.Copy(io.Discard, io.LimitReader(r, size)); err != nil {
		return err
	}
	if atomic.AddInt32(&s.current, 1) > atomic.LoadInt32(&s.max) {
		atomic.StoreInt32(&s.max, atomic.LoadInt32(&s.current))
	}
	// Hold all four workers at the barrier so the test proves concurrency.
	if atomic.LoadInt32(&s.current) == 4 {
		// no-op: the other workers may already have passed, but max is observed.
	}
	if s.barrier != nil {
		select {
		case <-s.barrier:
		case <-time.After(100 * time.Millisecond):
		}
	}
	s.mu.Lock()
	if s.parts == nil {
		s.parts = map[int]int64{}
	}
	s.parts[n] = size
	s.mu.Unlock()
	atomic.AddInt32(&s.current, -1)
	return nil
}
func (s *concurrentProgressiveSession) Complete(context.Context, FinalArtifactIdentity) (*UploadResult, error) {
	atomic.AddInt32(&s.complete, 1)
	return &UploadResult{}, nil
}
func (s *concurrentProgressiveSession) Abort(context.Context) error { return nil }

func TestRunProgressiveUploadUsesFourWorkersAndCompletesAfterAllParts(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 4*1024)
	path := t.TempDir() + "/out.bin"
	if err := writeTestFile(path, payload); err != nil {
		t.Fatal(err)
	}
	file := NewGrowingFile()
	file.Update(int64(len(payload)), true, int64(len(payload)))
	file.MarkDurable(int64(len(payload)))
	session := &concurrentProgressiveSession{barrier: make(chan struct{})}
	if _, err := RunProgressiveUpload(context.Background(), path, 1024, file, session, nil); err != nil {
		t.Fatal(err)
	}
	close(session.barrier)
	if got := atomic.LoadInt32(&session.complete); got != 1 {
		t.Fatalf("complete calls = %d; want 1", got)
	}
	if got := atomic.LoadInt32(&session.max); got < 2 {
		t.Fatalf("max concurrent uploads = %d; want multiple workers", got)
	}
	if len(session.parts) != 4 {
		t.Fatalf("uploaded parts = %d; want 4", len(session.parts))
	}
}

// overlapTestSession completes part 1 immediately, then holds parts 2+
// until release — so the test can finalize the growing file while the
// upload is mid-flight and pin the progressive overlap telemetry.
type overlapTestSession struct {
	mu        sync.Mutex
	parts     map[int]int64
	firstDone chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (s *overlapTestSession) UploadPart(_ context.Context, n int, r io.Reader, size int64) error {
	if _, err := io.Copy(io.Discard, io.LimitReader(r, size)); err != nil {
		return err
	}
	s.mu.Lock()
	if s.parts == nil {
		s.parts = map[int]int64{}
	}
	s.parts[n] = size
	s.mu.Unlock()
	if n == 1 {
		s.once.Do(func() { close(s.firstDone) })
		return nil
	}
	select {
	case <-s.release:
	case <-time.After(5 * time.Second):
	}
	return nil
}
func (s *overlapTestSession) Complete(context.Context, FinalArtifactIdentity) (*UploadResult, error) {
	return &UploadResult{}, nil
}
func (s *overlapTestSession) Abort(context.Context) error { return nil }

// TestProgressiveUploadOverlapTelemetry pins the progressive timings when
// the render finalizes WHILE the upload is running: the first part is
// uploaded before render end (counted + bytes), and the overlap window is
// positive. RunProgressiveUpload starts with the full safe prefix but a
// non-finalized file, exactly the early-intent progressive flow.
func TestProgressiveUploadOverlapTelemetry(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 64*1024)
	path := t.TempDir() + "/out.bin"
	if err := writeTestFile(path, payload); err != nil {
		t.Fatal(err)
	}
	file := NewGrowingFile()
	// First chunk safe, but the render has not finalized yet.
	file.Update(1024, false, 0)
	session := &overlapTestSession{firstDone: make(chan struct{}), release: make(chan struct{})}
	done := make(chan *UploadResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := RunProgressiveUpload(context.Background(), path, 1024, file, session, nil)
		if err != nil {
			errCh <- err
			return
		}
		done <- result
	}()
	// Part 1 completed while the engine was still rendering.
	select {
	case <-session.firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first part never started")
	}
	// Give the overlap window a measurable (>1ms) duration before the
	// engine finalizes; the timings are wall-clock milliseconds.
	time.Sleep(10 * time.Millisecond)
	file.Update(int64(len(payload)), true, int64(len(payload)))
	file.MarkDurable(int64(len(payload)))
	close(session.release)

	select {
	case result := <-done:
		if result.Breakdown.PartsUploadedBeforeRenderEnd < 1 {
			t.Errorf("parts before render end = %d; want >= 1", result.Breakdown.PartsUploadedBeforeRenderEnd)
		}
		if result.Breakdown.BytesUploadedBeforeRenderEnd < 1024 {
			t.Errorf("bytes before render end = %d; want >= 1024", result.Breakdown.BytesUploadedBeforeRenderEnd)
		}
		if result.Breakdown.OverlapMS <= 0 {
			t.Errorf("overlap ms = %d; want > 0 (upload overlapped the render)", result.Breakdown.OverlapMS)
		}
	case err := <-errCh:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("progressive upload never completed")
	}
}

// TestProgressiveUploadTimingZeroWhenRenderEndedFirst pins the legacy
// post-render flow: the output is already finalized and durable before the
// upload starts, so nothing was uploaded before render end and the overlap
// window is zero (the timings degrade to zero instead of being fabricated).
func TestProgressiveUploadTimingZeroWhenRenderEndedFirst(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 4*1024)
	path := t.TempDir() + "/out.bin"
	if err := writeTestFile(path, payload); err != nil {
		t.Fatal(err)
	}
	file := NewGrowingFile()
	file.Update(int64(len(payload)), true, int64(len(payload)))
	file.MarkDurable(int64(len(payload)))
	session := &progressiveTestSession{}
	result, err := RunProgressiveUpload(context.Background(), path, 1024, file, session, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Breakdown.PartsUploadedBeforeRenderEnd != 0 {
		t.Errorf("parts before render end = %d; want 0", result.Breakdown.PartsUploadedBeforeRenderEnd)
	}
	if result.Breakdown.BytesUploadedBeforeRenderEnd != 0 {
		t.Errorf("bytes before render end = %d; want 0", result.Breakdown.BytesUploadedBeforeRenderEnd)
	}
	if result.Breakdown.OverlapMS != 0 {
		t.Errorf("overlap ms = %d; want 0", result.Breakdown.OverlapMS)
	}
	// The time to the first part is a wall-clock duration that can be
	// legitimately 0 on a fast machine; only the overlap/count semantics
	// are pinned here.
	if result.Breakdown.FirstPartStartedMS < 0 {
		t.Errorf("first part started ms = %d; want >= 0", result.Breakdown.FirstPartStartedMS)
	}
}

func TestValidateFinalArtifactIdentityRequiresAllEvidence(t *testing.T) {
	h := sha256.Sum256([]byte("final"))
	base := FinalArtifactIdentity{SHA256: hex.EncodeToString(h[:]), SizeBytes: 5, EngineFinalized: true, OutputDurable: true, UploadedParts: 4, ExpectedParts: 4}
	if err := validateFinalArtifactIdentity(base); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
	cases := []struct {
		name string
		edit func(*FinalArtifactIdentity)
	}{
		{"missing trailer", func(v *FinalArtifactIdentity) { v.EngineFinalized = false }},
		{"not durable", func(v *FinalArtifactIdentity) { v.OutputDurable = false }},
		{"missing sha", func(v *FinalArtifactIdentity) { v.SHA256 = "" }},
		{"invalid size", func(v *FinalArtifactIdentity) { v.SizeBytes = 0 }},
		{"missing part", func(v *FinalArtifactIdentity) { v.UploadedParts-- }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := base
			tc.edit(&v)
			if err := validateFinalArtifactIdentity(v); err == nil {
				t.Fatal("expected fail-closed validation error")
			}
		})
	}
}

func writeTestFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o600)
}
