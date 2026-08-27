package publisher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
)

const progressiveUploadWorkers = 4

type ProgressivePublishState string

const (
	ProgressiveRendering        ProgressivePublishState = "RENDERING"
	ProgressiveUploading        ProgressivePublishState = "PROGRESSIVE_UPLOADING"
	ProgressiveEngineFinalized  ProgressivePublishState = "ENGINE_FINALIZED"
	ProgressiveOutputDurable    ProgressivePublishState = "OUTPUT_DURABLE"
	ProgressiveUploadFinalizing ProgressivePublishState = "UPLOAD_FINALIZING"
	ProgressiveUploaded         ProgressivePublishState = "UPLOADED"
	ProgressiveCommitWait       ProgressivePublishState = "COMMIT_WAIT"
	ProgressiveCommitted        ProgressivePublishState = "COMMITTED"
)

type ProgressiveStateStore interface {
	MarkUploaded(context.Context, string) error
}

type ArtifactPublishState struct {
	State            ProgressivePublishState
	EngineFinalized  bool
	OutputDurable    bool
	FinalSHA256      string
	FinalSizeBytes   int64
	AllPartsUploaded bool
	UploadedParts    int
	ExpectedParts    int
}

func (s ArtifactPublishState) CanComplete() bool {
	return s.EngineFinalized &&
		s.OutputDurable &&
		s.FinalSHA256 != "" &&
		s.FinalSizeBytes > 0 &&
		s.ExpectedParts > 0 &&
		s.UploadedParts == s.ExpectedParts &&
		s.AllPartsUploaded
}

func validateFinalArtifactIdentity(final FinalArtifactIdentity) error {
	if !final.EngineFinalized {
		return fmt.Errorf("progressive upload: engine trailer/finalization not confirmed")
	}
	if !final.OutputDurable {
		return fmt.Errorf("progressive upload: durable output not confirmed")
	}
	if final.SHA256 == "" || !isLowerHex64(final.SHA256) {
		return fmt.Errorf("progressive upload: final SHA-256 is missing or invalid")
	}
	if final.SizeBytes <= 0 {
		return fmt.Errorf("progressive upload: final size is missing or invalid")
	}
	if final.ExpectedParts <= 0 || final.UploadedParts != final.ExpectedParts {
		return fmt.Errorf("progressive upload: incomplete parts: uploaded=%d expected=%d", final.UploadedParts, final.ExpectedParts)
	}
	return nil
}

// GrowingFile tracks the prefix safe to read and the separate durability fact.
type GrowingFile struct {
	mu        sync.Mutex
	cond      *sync.Cond
	safeBytes int64
	finalSize int64
	finalized bool
	durable   bool
	aborted   error
}

func NewGrowingFile() *GrowingFile { g := &GrowingFile{}; g.cond = sync.NewCond(&g.mu); return g }

func (g *GrowingFile) Update(safeBytes int64, finalized bool, finalSize int64) {
	g.mu.Lock()
	if safeBytes > g.safeBytes {
		g.safeBytes = safeBytes
	}
	if finalized {
		g.finalized = true
		if finalSize > 0 {
			g.finalSize = finalSize
		}
	}
	g.cond.Broadcast()
	g.mu.Unlock()
}

// MarkDurable records the explicit fsync/rename durability confirmation.
func (g *GrowingFile) MarkDurable(finalSize int64) {
	g.mu.Lock()
	g.durable = true
	if finalSize > 0 {
		g.finalSize = finalSize
	}
	g.cond.Broadcast()
	g.mu.Unlock()
}

func (g *GrowingFile) Abort(err error) {
	g.mu.Lock()
	g.aborted = err
	g.cond.Broadcast()
	g.mu.Unlock()
}

func (g *GrowingFile) snapshot() (safe, finalSize int64, finalized, durable bool, aborted error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.safeBytes, g.finalSize, g.finalized, g.durable, g.aborted
}

func (g *GrowingFile) WaitForRange(ctx context.Context, start, length int64) error {
	if start < 0 || length <= 0 {
		return fmt.Errorf("progressive upload: invalid range")
	}
	for {
		safe, finalSize, finalized, _, aborted := g.snapshot()
		if aborted != nil {
			return aborted
		}
		if safe >= start+length {
			return nil
		}
		if finalized && finalSize < start+length {
			return io.ErrUnexpectedEOF
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-waitSignal(g):
		}
	}
}

func waitSignal(g *GrowingFile) <-chan struct{} {
	ch := make(chan struct{})
	go func() { g.mu.Lock(); g.cond.Wait(); g.mu.Unlock(); close(ch) }()
	return ch
}

// RunProgressiveUpload preserves the original API and runs without a journal.
func RunProgressiveUpload(ctx context.Context, path string, chunkSize int64, file *GrowingFile, session ProgressiveSession, onProgress func(int64)) (*UploadResult, error) {
	return RunProgressiveUploadWithJournal(ctx, path, chunkSize, file, session, "", onProgress)
}

// RunProgressiveUploadWithJournal resumes confirmed parts from journalPath.
// The caller must create session against the same remote upload_id stored in
// the journal; a missing or corrupt journal is handled fail-closed.
func RunProgressiveUploadWithJournal(ctx context.Context, path string, chunkSize int64, file *GrowingFile, session ProgressiveSession, journalPath string, onProgress func(int64)) (*UploadResult, error) {
	return runProgressiveUploadWithJournal(ctx, path, chunkSize, file, session, journalPath, nil, onProgress)
}

func RunProgressiveUploadWithJournalAndStore(ctx context.Context, path string, chunkSize int64, file *GrowingFile, session ProgressiveSession, journalPath string, store ProgressiveStateStore, spoolID string, onProgress func(int64)) (*UploadResult, error) {
	return runProgressiveUploadWithJournal(ctx, path, chunkSize, file, session, journalPath, func(ctx context.Context) error { return store.MarkUploaded(ctx, spoolID) }, onProgress)
}

func runProgressiveUploadWithJournal(ctx context.Context, path string, chunkSize int64, file *GrowingFile, session ProgressiveSession, journalPath string, markUploaded func(context.Context) error, onProgress func(int64)) (*UploadResult, error) {
	if path == "" || chunkSize <= 0 || file == nil || session == nil {
		return nil, fmt.Errorf("progressive upload: invalid configuration")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	journal, err := loadProgressiveJournal(journalPath)
	if err != nil {
		return nil, err
	}
	if journal.ChunkSize != 0 && journal.ChunkSize != chunkSize {
		return nil, fmt.Errorf("progressive journal: chunk size changed")
	}
	journal.ChunkSize = chunkSize
	if journal.UploadID == "" {
		journal.UploadID = "progressive-local"
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type part struct {
		number      int
		start, size int64
	}
	parts := make(chan part, progressiveUploadWorkers)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	var mu, journalMu sync.Mutex
	var uploaded int64
	var uploadedParts int
	worker := func() {
		defer wg.Done()
		for p := range parts {
			if err := file.WaitForRange(ctx, p.start, p.size); err != nil {
				select {
				case errs <- err:
					cancel()
				default:
				}
				return
			}
			if err := session.UploadPart(ctx, p.number, io.NewSectionReader(f, p.start, p.size), p.size); err != nil {
				select {
				case errs <- err:
					cancel()
				default:
				}
				return
			}
			journalMu.Lock()
			journal.addPart(p.number, p.size)
			saveErr := saveProgressiveJournal(journalPath, journal)
			journalMu.Unlock()
			if saveErr != nil {
				select {
				case errs <- saveErr:
					cancel()
				default:
				}
				return
			}
			mu.Lock()
			uploaded += p.size
			uploadedParts++
			n := uploaded
			mu.Unlock()
			if onProgress != nil {
				onProgress(n)
			}
		}
	}
	for i := 0; i < progressiveUploadWorkers; i++ {
		wg.Add(1)
		go worker()
	}
	var number int
	var start int64
	expectedParts := 0
	for {
		_, finalSize, finalized, _, _ := file.snapshot()
		if finalized && start >= finalSize {
			break
		}
		size := chunkSize
		if finalized && start+size > finalSize {
			size = finalSize - start
		}
		if size <= 0 {
			break
		}
		expectedParts++
		number++
		journalMu.Lock()
		already := journal.hasPart(number, size)
		journalMu.Unlock()
		if already {
			uploaded += size
			uploadedParts++
			start += size
			continue
		}
		parts <- part{number: number, start: start, size: size}
		start += size
	}
	close(parts)
	wg.Wait()
	select {
	case err := <-errs:
		_ = session.Abort(context.Background())
		return nil, err
	default:
	}
	_, finalSize, finalized, durable, _ := file.snapshot()
	if !finalized || !durable {
		_ = session.Abort(context.Background())
		return nil, fmt.Errorf("progressive upload: output is not finalized and durable")
	}
	mu.Lock()
	doneParts := uploadedParts
	doneBytes := uploaded
	mu.Unlock()
	sha, err := hashFile(path, finalSize)
	if err != nil {
		_ = session.Abort(context.Background())
		return nil, err
	}
	journalMu.Lock()
	journal.Finalized = true
	journal.FinalSize = finalSize
	journal.FinalSHA256 = sha
	saveErr := saveProgressiveJournal(journalPath, journal)
	journalMu.Unlock()
	if saveErr != nil {
		_ = session.Abort(context.Background())
		return nil, saveErr
	}
	state := ArtifactPublishState{State: ProgressiveUploadFinalizing, EngineFinalized: finalized, OutputDurable: durable, FinalSHA256: sha, FinalSizeBytes: finalSize, AllPartsUploaded: doneParts == expectedParts && doneBytes == finalSize, UploadedParts: doneParts, ExpectedParts: expectedParts}
	if !state.CanComplete() {
		_ = session.Abort(context.Background())
		return nil, fmt.Errorf("progressive upload: complete prerequisites not satisfied: %+v", state)
	}
	state.State = ProgressiveUploaded
	final := FinalArtifactIdentity{SHA256: state.FinalSHA256, SizeBytes: state.FinalSizeBytes, EngineFinalized: state.EngineFinalized, OutputDurable: state.OutputDurable, UploadedParts: state.UploadedParts, ExpectedParts: state.ExpectedParts}
	if err := validateFinalArtifactIdentity(final); err != nil {
		_ = session.Abort(context.Background())
		return nil, err
	}
	result, err := session.Complete(ctx, final)
	if err != nil {
		return nil, err
	}
	if markUploaded != nil {
		if err := markUploaded(ctx); err != nil {
			return nil, fmt.Errorf("progressive upload: persist UPLOADED state: %w", err)
		}
	}
	return result, nil
}

func hashFile(path string, size int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.CopyN(h, f, size); err != nil && err != io.EOF {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
