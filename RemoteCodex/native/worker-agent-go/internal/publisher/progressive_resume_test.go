package publisher

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type progressiveTestSession struct {
	mu        sync.Mutex
	parts     map[int][]byte
	completes int
	aborts    int
}

func (s *progressiveTestSession) UploadPart(_ context.Context, n int, r io.Reader, size int64) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if int64(len(b)) != size {
		return io.ErrUnexpectedEOF
	}
	s.mu.Lock()
	if s.parts == nil {
		s.parts = map[int][]byte{}
	}
	s.parts[n] = b
	s.mu.Unlock()
	return nil
}
func (s *progressiveTestSession) Complete(_ context.Context, f FinalArtifactIdentity) (*UploadResult, error) {
	s.mu.Lock()
	s.completes++
	s.mu.Unlock()
	return &UploadResult{UploadedBytes: f.SizeBytes}, nil
}
func (s *progressiveTestSession) Abort(context.Context) error {
	s.mu.Lock()
	s.aborts++
	s.mu.Unlock()
	return nil
}

func TestProgressiveUploadResumeSkipsJournaledParts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "video.partial")
	payload := []byte("abcdefghijkl")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatal(err)
	}
	journalPath := filepath.Join(dir, "video.progressive.json")
	if err := saveProgressiveJournal(journalPath, progressiveJournal{ChunkSize: 4, Parts: []progressiveJournalPart{{Number: 1, Size: 4}}}); err != nil {
		t.Fatal(err)
	}
	file := NewGrowingFile()
	file.Update(int64(len(payload)), true, int64(len(payload)))
	file.MarkDurable(int64(len(payload)))
	session := &progressiveTestSession{}
	if _, err := RunProgressiveUploadWithJournal(context.Background(), path, 4, file, session, journalPath, nil); err != nil {
		t.Fatal(err)
	}
	if len(session.parts) != 2 {
		t.Fatalf("uploaded parts = %d; want 2 after resuming part 1", len(session.parts))
	}
	j, err := loadProgressiveJournal(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Parts) != 3 || j.FinalSHA256 == "" || !j.Finalized {
		t.Fatalf("journal after resume = %+v", j)
	}
	if session.completes != 1 || session.aborts != 0 {
		t.Fatalf("session lifecycle = completes=%d aborts=%d", session.completes, session.aborts)
	}
}

func TestProgressiveUploadRejectsNonDurableOutput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "video.partial")
	if err := os.WriteFile(path, []byte("abcd"), 0o644); err != nil {
		t.Fatal(err)
	}
	file := NewGrowingFile()
	file.Update(4, true, 4)
	session := &progressiveTestSession{}
	if _, err := RunProgressiveUploadWithJournal(context.Background(), path, 4, file, session, filepath.Join(dir, "j.json"), nil); err == nil {
		t.Fatal("expected non-durable output rejection")
	}
	if session.aborts != 1 {
		t.Fatalf("aborts = %d; want 1", session.aborts)
	}
}
