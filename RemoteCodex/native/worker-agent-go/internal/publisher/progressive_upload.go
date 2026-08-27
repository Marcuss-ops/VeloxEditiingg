package publisher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const progressiveUploadWorkers = 4

// ProgressiveUploadOptions tunes concurrent immutable parts within one
// artifact. Artifact-level concurrency remains owned by worker.PublisherPool.
type ProgressiveUploadOptions struct {
	Workers int
}

func (o ProgressiveUploadOptions) workers() int {
	if o.Workers <= 0 {
		return progressiveUploadWorkers
	}
	return o.Workers
}

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
	// finalizedAt is the wall-clock moment the engine declared the output
	// finalized (the C++ trailer / final artifact_write_progress event). It
	// is the render-end reference for the progressive overlap telemetry.
	finalizedAt time.Time
	durable     bool
	aborted     error
}

func NewGrowingFile() *GrowingFile { g := &GrowingFile{}; g.cond = sync.NewCond(&g.mu); return g }

func (g *GrowingFile) Update(safeBytes int64, finalized bool, finalSize int64) {
	g.mu.Lock()
	if safeBytes > g.safeBytes {
		g.safeBytes = safeBytes
	}
	if finalized {
		if !g.finalized {
			g.finalizedAt = time.Now()
		}
		g.finalized = true
		if finalSize > 0 {
			g.finalSize = finalSize
		}
	}
	g.cond.Broadcast()
	g.mu.Unlock()
}

// FinalizedAt returns the wall-clock moment the output was declared
// finalized, or the zero time when it has not been finalized yet.
func (g *GrowingFile) FinalizedAt() time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.finalizedAt
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
	return runProgressiveUploadWithJournal(ctx, path, chunkSize, file, session, journalPath, nil, ProgressiveUploadOptions{}, onProgress)
}

func RunProgressiveUploadWithJournalAndStore(ctx context.Context, path string, chunkSize int64, file *GrowingFile, session ProgressiveSession, journalPath string, store ProgressiveStateStore, spoolID string, onProgress func(int64)) (*UploadResult, error) {
	return RunProgressiveUploadWithJournalAndStoreOptions(ctx, path, chunkSize, file, session, journalPath, store, spoolID, ProgressiveUploadOptions{}, onProgress)
}

// RunProgressiveUploadWithJournalAndStoreOptions is the configurable form of
// the journalized upload API. Existing callers retain the safe four-worker
// default through RunProgressiveUploadWithJournalAndStore.
func RunProgressiveUploadWithJournalAndStoreOptions(ctx context.Context, path string, chunkSize int64, file *GrowingFile, session ProgressiveSession, journalPath string, store ProgressiveStateStore, spoolID string, options ProgressiveUploadOptions, onProgress func(int64)) (*UploadResult, error) {
	var markUploaded func(context.Context) error
	if store != nil {
		markUploaded = func(ctx context.Context) error { return store.MarkUploaded(ctx, spoolID) }
	}
	return runProgressiveUploadWithJournal(ctx, path, chunkSize, file, session, journalPath, markUploaded, options, onProgress)
}

func runProgressiveUploadWithJournal(ctx context.Context, path string, chunkSize int64, file *GrowingFile, session ProgressiveSession, journalPath string, markUploaded func(context.Context) error, options ProgressiveUploadOptions, onProgress func(int64)) (*UploadResult, error) {
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
	workers := options.workers()
	parts := make(chan part, workers)
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	var mu, journalMu sync.Mutex
	var uploaded int64
	var uploadedParts int
	// Progressive telemetry: the render-end reference is the moment the
	// engine declared the output finalized (GrowingFile.FinalizedAt).
	// Parts whose UploadPart completed BEFORE that moment were uploaded
	// while the render was still running — the overlap window.
	runStartedAt := time.Now()
	var firstPartStartedAt time.Time
	var partsBeforeRenderEnd, bytesBeforeRenderEnd int64
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
			mu.Lock()
			if firstPartStartedAt.IsZero() {
				firstPartStartedAt = time.Now()
			}
			mu.Unlock()
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
			// Count this part as uploaded before render end when the
			// engine had not yet finalized the output at completion time.
			completedAt := time.Now()
			if renderEnd := file.FinalizedAt(); renderEnd.IsZero() || completedAt.Before(renderEnd) {
				partsBeforeRenderEnd++
				bytesBeforeRenderEnd += p.size
			}
			n := uploaded
			mu.Unlock()
			if onProgress != nil {
				onProgress(n)
			}
		}
	}
	for i := 0; i < workers; i++ {
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
	if result == nil {
		result = &UploadResult{}
	}
	// Progressive overlap telemetry: how much of the upload ran while the
	// render was still writing. first_part_started_ms is measured from the
	// upload run start; overlap_ms is the render/upload overlap window
	// (render end minus first part start, zero when the upload started
	// after the render had already finalized).
	firstPartStartedMS := int64(0)
	overlapMS := int64(0)
	if !firstPartStartedAt.IsZero() {
		firstPartStartedMS = firstPartStartedAt.Sub(runStartedAt).Milliseconds()
		if renderEnd := file.FinalizedAt(); !renderEnd.IsZero() && renderEnd.After(firstPartStartedAt) {
			overlapMS = renderEnd.Sub(firstPartStartedAt).Milliseconds()
		}
	}
	result.Breakdown.FirstPartStartedMS = firstPartStartedMS
	result.Breakdown.PartsUploadedBeforeRenderEnd = partsBeforeRenderEnd
	result.Breakdown.BytesUploadedBeforeRenderEnd = bytesBeforeRenderEnd
	result.Breakdown.OverlapMS = overlapMS
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
