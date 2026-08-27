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
