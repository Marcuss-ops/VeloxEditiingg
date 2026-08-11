package artifactgraph

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// fixedClock returns a mutable fake clock.
func fakeClock() (func() time.Time, *time.Time) {
	now := time.Unix(1_700_000_000, 0).UTC()
	return func() time.Time { return now }, &now
}

// ── ledger lifecycle ──────────────────────────────────────────────────────

func TestGraph_CreateWriteReadClose_Lifecycle(t *testing.T) {
	clock, now := fakeClock()
	g := NewWithClock(clock)
	*now = now.Add(0)

	g.CreateWithSize("/tmp/job/seg_001.mp4", "segment_render", 340*1024*1024)
	g.RecordWrite("/tmp/job/seg_001.mp4", 100*1024*1024)
	g.RecordWrite("/tmp/job/seg_001.mp4", 240*1024*1024)
	g.RecordRead("/tmp/job/seg_001.mp4", 340*1024*1024, "concat")

	*now = now.Add(1500 * time.Millisecond)
	g.Close("/tmp/job/seg_001.mp4")

	snap := g.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("snapshot len = %d, want 1", len(snap))
	}
	rec := snap[0]
	if rec.Path != "/tmp/job/seg_001.mp4" || rec.ProducerPhase != "segment_render" {
		t.Errorf("path/phase = %q/%q", rec.Path, rec.ProducerPhase)
	}
	if rec.SizeBytes != 340*1024*1024 {
		t.Errorf("size = %d, want 340 MiB (producer-supplied)", rec.SizeBytes)
	}
	if rec.WrittenBytes != 340*1024*1024 {
		t.Errorf("written = %d, want 340 MiB", rec.WrittenBytes)
	}
	if rec.ReadBytes != 340*1024*1024 {
		t.Errorf("read = %d, want 340 MiB", rec.ReadBytes)
	}
	if rec.ConsumerPhase != "concat" {
		t.Errorf("consumer = %q, want concat", rec.ConsumerPhase)
	}
	if rec.Lifetime != 1500*time.Millisecond {
		t.Errorf("lifetime = %s, want 1.5s", rec.Lifetime)
	}
	if rec.ClosedAt.IsZero() {
		t.Error("closed_at should be set after Close")
	}
}

func TestGraph_SizeFallsBackToWrittenAtClose(t *testing.T) {
	g := New()
	g.Create("/tmp/job/manifest.txt", "concat")
	g.RecordWrite("/tmp/job/manifest.txt", 4096)
	g.Close("/tmp/job/manifest.txt")
	rec := g.Snapshot()[0]
	if rec.SizeBytes != 4096 {
		t.Errorf("size fallback = %d, want 4096", rec.SizeBytes)
	}
}

func TestGraph_CreateIdempotent_AndUnknownPathsIgnored(t *testing.T) {
	g := New()
	g.Create("/f", "producer-a")
	g.Create("/f", "producer-b") // no-op: first producer owns the record
	g.RecordWrite("/f", 10)
	g.RecordWrite("/unknown", 999) // ignored
	g.RecordRead("/unknown", 5, "consumer") // ignored
	if got := g.Snapshot()[0].ProducerPhase; got != "producer-a" {
		t.Errorf("producer = %q, want producer-a (first wins)", got)
	}
	if got := g.Snapshot()[0].WrittenBytes; got != 10 {
		t.Errorf("written = %d, want 10 (unknown path writes ignored)", got)
	}
}

func TestGraph_FirstConsumerWins_ReadBytesAccumulate(t *testing.T) {
	g := New()
	g.Create("/f.mp4", "mux")
	g.RecordWrite("/f.mp4", 100)
	g.RecordRead("/f.mp4", 60, "artifact_verify")
	g.RecordRead("/f.mp4", 40, "upload")
	rec := g.Snapshot()[0]
	if rec.ConsumerPhase != "artifact_verify" {
		t.Errorf("consumer = %q, want artifact_verify (first)", rec.ConsumerPhase)
	}
	if rec.ReadBytes != 100 {
		t.Errorf("read = %d, want 100 (accumulated across consumers)", rec.ReadBytes)
	}
}

// ── tracked adapters ──────────────────────────────────────────────────────

func TestTrackedWriter_RegistersRecordsCloses(t *testing.T) {
	clock, now := fakeClock()
	g := NewWithClock(clock)
	var buf bytes.Buffer
	tw := g.TrackWriter("/tmp/job/concat.txt", "concat", &buf)

	if _, err := tw.Write([]byte("file 'seg_001.mp4'\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("file 'seg_002.mp4'\n")); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(300 * time.Millisecond)
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(buf.String(), "seg_001") {
		t.Error("underlying writer did not receive bytes")
	}
	rec := g.Snapshot()[0]
	if rec.WrittenBytes != int64(len("file 'seg_001.mp4'\nfile 'seg_002.mp4'\n")) {
		t.Errorf("written = %d", rec.WrittenBytes)
	}
	if rec.ProducerPhase != "concat" {
		t.Errorf("producer = %q", rec.ProducerPhase)
	}
	if rec.Lifetime != 300*time.Millisecond {
		t.Errorf("lifetime = %s, want 300ms (Close finalizes)", rec.Lifetime)
	}
}

func TestTrackedWriter_ClosesUnderlyingCloser(t *testing.T) {
	g := New()
	called := false
	closer := &closeRecorder{Buffer: &bytes.Buffer{}, onClose: func() { called = true }}
	tw := g.TrackWriter("/f", "producer", closer)
	_, _ = tw.Write([]byte("x"))
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("Close should forward to underlying io.Closer")
	}
}

type closeRecorder struct {
	*bytes.Buffer
	onClose func()
}

func (c *closeRecorder) Close() error {
	c.onClose()
	return nil
}

func TestTrackedAdapters_NilSafe(t *testing.T) {
	var tw *TrackedWriter
	if n, err := tw.Write([]byte("x")); n != 0 || err == nil {
		t.Errorf("nil writer Write = %d, %v; want 0, error", n, err)
	}
	if err := tw.Close(); err != nil {
		t.Errorf("nil writer Close = %v", err)
	}
	// A nil reader must fail loudly — io.EOF would make io.Copy report a
	// clean 0-byte copy, masking the missing stream.
	var tr *TrackedReader
	if n, err := tr.Read([]byte("x")); n != 0 || err == nil {
		t.Errorf("nil reader Read = %d, %v; want 0, error", n, err)
	}
}

func TestGraph_CreateWithSize_ClampsNegative(t *testing.T) {
	g := New()
	g.CreateWithSize("/f", "producer", -42)
	g.RecordWrite("/f", 7)
	g.Close("/f")
	rec := g.Snapshot()[0]
	if rec.SizeBytes != 7 {
		t.Errorf("size = %d, want 7 (negative clamped, fallback to written)", rec.SizeBytes)
	}
}

func TestTrackedReader_AccountsReads_DoesNotRegister(t *testing.T) {
	g := New()
	// File produced by a real producer phase.
	g.Create("/tmp/job/video_temp.mp4", "concat")
	tw := g.TrackWriter("/tmp/job/video_temp.mp4", "concat", &bytes.Buffer{})
	_, _ = tw.Write(make([]byte, 4096))

	tr := g.TrackReader("/tmp/job/video_temp.mp4", "mux", strings.NewReader(strings.Repeat("x", 4096)))
	var dst bytes.Buffer
	if _, err := io.Copy(&dst, tr); err != nil {
		t.Fatal(err)
	}

	rec := g.Snapshot()[0]
	if rec.ReadBytes != 4096 {
		t.Errorf("read = %d, want 4096", rec.ReadBytes)
	}
	if rec.ConsumerPhase != "mux" {
		t.Errorf("consumer = %q, want mux", rec.ConsumerPhase)
	}

	// Reads of unregistered files do not create records.
	tr2 := g.TrackReader("/tmp/job/never_registered.bin", "mux", strings.NewReader("data"))
	_, _ = io.Copy(io.Discard, tr2)
	if len(g.Snapshot()) != 1 {
		t.Errorf("graph len = %d, want 1 (reader must not register files)", len(g.Snapshot()))
	}
}

// ── candidate discovery ───────────────────────────────────────────────────

func TestCandidates_RankingAndFiltering(t *testing.T) {
	clock, now := fakeClock()
	g := NewWithClock(clock)

	// 340 MB written then immediately re-read → top candidate.
	g.Create("/tmp/job/big_seg.mp4", "segment_render")
	g.RecordWrite("/tmp/job/big_seg.mp4", 340*1024*1024)
	g.RecordRead("/tmp/job/big_seg.mp4", 340*1024*1024, "concat")
	*now = now.Add(900 * time.Millisecond)
	g.Close("/tmp/job/big_seg.mp4")

	// 1 MB re-read after a long lifetime → candidate but lower rank.
	g.Create("/tmp/job/small.txt", "audio_encode")
	g.RecordWrite("/tmp/job/small.txt", 1024*1024)
	*now = now.Add(30 * time.Minute)
	g.RecordRead("/tmp/job/small.txt", 1024*1024, "mux")
	g.Close("/tmp/job/small.txt")

	// Written, never read → NOT a candidate (harmless intermediate).
	g.Create("/tmp/job/written_only.txt", "probe")
	g.RecordWrite("/tmp/job/written_only.txt", 500)
	g.Close("/tmp/job/written_only.txt")

	// Read, never written → NOT a candidate (input asset, not intermediate).
	g.Create("/tmp/job/asset.mp4", "download")
	g.RecordRead("/tmp/job/asset.mp4", 1000, "segment_render")

	cands := g.Candidates()
	if len(cands) != 2 {
		t.Fatalf("candidates = %d, want 2", len(cands))
	}
	if cands[0].Path != "/tmp/job/big_seg.mp4" {
		t.Errorf("top candidate = %q, want big_seg (reread bytes dominate)", cands[0].Path)
	}
	if cands[0].ReReadBytes != 340*1024*1024 {
		t.Errorf("top reread = %d", cands[0].ReReadBytes)
	}
	if cands[1].ReReadBytes != 1024*1024 {
		t.Errorf("second reread = %d", cands[1].ReReadBytes)
	}
	if !strings.Contains(cands[0].Reason, "immediately") {
		t.Errorf("short-lifetime reason = %q, want immediate-read signal", cands[0].Reason)
	}
	if strings.Contains(cands[1].Reason, "immediately") {
		t.Errorf("long-lifetime reason = %q, must not say immediately", cands[1].Reason)
	}
}

// ── summary ───────────────────────────────────────────────────────────────

func TestSummary_Totals(t *testing.T) {
	g := New()
	g.CreateWithSize("/a", "p", 100)
	g.RecordWrite("/a", 100)
	g.RecordRead("/a", 100, "c")
	g.Create("/b", "p2")
	g.RecordWrite("/b", 50) // never read

	s := g.Summary()
	if s.GraphVersion != 1 {
		t.Errorf("graph version = %d, want 1", s.GraphVersion)
	}
	if s.FileCount != 2 {
		t.Errorf("file count = %d, want 2", s.FileCount)
	}
	if s.TotalWrittenBytes != 150 {
		t.Errorf("total written = %d, want 150", s.TotalWrittenBytes)
	}
	if s.TotalReadBytes != 100 {
		t.Errorf("total read = %d, want 100", s.TotalReadBytes)
	}
	if s.TotalReReadBytes != 100 {
		t.Errorf("total reread = %d, want 100", s.TotalReReadBytes)
	}
	if len(s.Candidates) != 1 {
		t.Errorf("candidates = %d, want 1", len(s.Candidates))
	}
}

func TestGraph_MarshalJSON(t *testing.T) {
	g := New()
	g.Create("/f", "producer")
	g.RecordWrite("/f", 3)
	data, err := g.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"producer_phase":"producer"`) {
		t.Errorf("JSON missing producer phase: %s", data)
	}
}

// ── context seam ──────────────────────────────────────────────────────────

func TestContext_WithGraphAndFromContext(t *testing.T) {
	if GraphFromContext(nil) != nil {
		t.Error("nil context must yield nil graph")
	}
	if got := GraphFromContext(context.Background()); got != nil {
		t.Error("empty context must yield nil graph")
	}
	g := New()
	ctx := WithGraph(context.Background(), g)
	if got := GraphFromContext(ctx); got != g {
		t.Error("GraphFromContext must return the injected graph")
	}
	// nil-safe injection.
	if WithGraph(context.Background(), nil) == nil {
		t.Error("WithGraph(nil graph) should return the original context")
	}
}

// ── concurrency smoke (run with -race) ────────────────────────────────────

func TestGraph_ConcurrentAccess(t *testing.T) {
	g := New()
	const files = 8
	const ops = 200
	var wg sync.WaitGroup
	for i := 0; i < files; i++ {
		path := "/tmp/job/f" + string(rune('a'+i)) + ".mp4"
		g.Create(path, "segment_render")
		wg.Add(2)
		go func(p string) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				g.RecordWrite(p, 1)
			}
		}(path)
		go func(p string) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				g.RecordRead(p, 1, "concat")
			}
		}(path)
	}
	wg.Wait()
	for _, p := range g.Snapshot() {
		if p.WrittenBytes != ops || p.ReadBytes != ops {
			t.Errorf("%s written=%d read=%d, want %d/%d", p.Path, p.WrittenBytes, p.ReadBytes, ops, ops)
		}
	}
}

// ── error paths ───────────────────────────────────────────────────────────

func TestTrackedReader_PropagatesErrors(t *testing.T) {
	g := New()
	g.Create("/f", "p")
	want := errors.New("boom")
	tr := g.TrackReader("/f", "c", errReader{err: want})
	var buf [4]byte
	if _, err := tr.Read(buf[:]); !errors.Is(err, want) {
		t.Errorf("read error = %v, want %v", err, want)
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }
