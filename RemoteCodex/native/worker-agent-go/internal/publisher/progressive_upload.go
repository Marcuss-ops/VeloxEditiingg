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
	// FirstPartSize optionally overrides the first immutable part. It is
	// useful for progressive renderers: a small first request starts the
	// overlap quickly while later requests use the negotiated chunk size.
	FirstPartSize int64
	// AdaptivePartSize selects a bounded final-size-derived part size for
	// parts scheduled after finalization. The producer still uses the
	// negotiated size while the output is growing, so it never waits for the
	// trailer before starting the bulk transfer.
	AdaptivePartSize bool
}

func (o ProgressiveUploadOptions) workers() int {
	if o.Workers <= 0 {
		return progressiveUploadWorkers
	}
	return o.Workers
}

const (
	progressiveMinimumPartSize = 256 * 1024
	progressiveMaximumPartSize = 2 * 1024 * 1024
	progressiveHashReadSize    = 1 * 1024 * 1024
)

func adaptiveProgressivePartSize(finalSize, negotiated int64) int64 {
	if negotiated <= 0 {
		negotiated = progressiveMaximumPartSize
	}
	if finalSize <= 0 {
		return negotiated
	}
	size := finalSize / 64
	if size < progressiveMinimumPartSize {
		size = progressiveMinimumPartSize
	}
	if size > progressiveMaximumPartSize {
		size = progressiveMaximumPartSize
	}
	if size > negotiated {
		size = negotiated
	}
	return size
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
	signal    chan struct{}
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

func NewGrowingFile() *GrowingFile { return &GrowingFile{signal: make(chan struct{})} }

// signalLocked closes the current generation and installs the next one.
// Waiters capture a generation while holding mu, so an Update cannot be
// missed and no waiter goroutine needs to park on a sync.Cond.
func (g *GrowingFile) signalLocked() {
	if g.signal == nil {
		g.signal = make(chan struct{})
	}
	close(g.signal)
	g.signal = make(chan struct{})
}

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
	g.signalLocked()
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
	g.signalLocked()
	g.mu.Unlock()
}

func (g *GrowingFile) Abort(err error) {
	g.mu.Lock()
	g.aborted = err
	g.signalLocked()
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
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.signal == nil {
		g.signal = make(chan struct{})
	}
	for {
		safe, finalSize, finalized, _, aborted := g.safeBytes, g.finalSize, g.finalized, g.durable, g.aborted
		if aborted != nil {
			return aborted
		}
		if safe >= start+length {
			return nil
		}
		if finalized && finalSize < start+length {
			return io.ErrUnexpectedEOF
		}
		signal := g.signal
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			g.mu.Lock()
			return ctx.Err()
		case <-signal:
			g.mu.Lock()
		}
	}
}

// waitForReadableRange closes the small race between the native progress
// watermark and the filesystem's visible size. The mux callback can publish
// a safe offset immediately after a buffered write while the reader still
// observes the old file length; waiting on the watermark alone then lets an
// HTTP request advertise Content-Length and receive EOF mid-part.
func waitForReadableRange(ctx context.Context, f *os.File, file *GrowingFile, start, length int64) error {
	if err := file.WaitForRange(ctx, start, length); err != nil {
		return err
	}
	end := start + length
	for {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		if info.Size() >= end {
			return nil
		}
		_, finalSize, finalized, _, _ := file.snapshot()
		if finalized && finalSize < end {
			return io.ErrUnexpectedEOF
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
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
	hashCtx, cancelHash := context.WithCancel(ctx)
	defer cancelHash()
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
	type hashOutcome struct {
		sha string
		err error
	}
	hashDone := make(chan hashOutcome, 1)
	go func() {
		sha, err := hashGrowingFile(hashCtx, path, file)
		hashDone <- hashOutcome{sha: sha, err: err}
	}()
	worker := func() {
		defer wg.Done()
		for p := range parts {
			if err := waitForReadableRange(ctx, f, file, p.start, p.size); err != nil {
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
	var producerErr error
	for {
		_, finalSize, finalized, _, aborted := file.snapshot()
		if aborted != nil {
			producerErr = aborted
			break
		}
		if finalized && start >= finalSize {
			break
		}
		nextPartSize := chunkSize
		if number == 0 && options.FirstPartSize > 0 && options.FirstPartSize < nextPartSize {
			nextPartSize = options.FirstPartSize
		}
		if options.AdaptivePartSize && number > 0 && finalized {
			nextPartSize = adaptiveProgressivePartSize(finalSize, chunkSize)
		}
		size := nextPartSize
		if finalized {
			if start+size > finalSize {
				size = finalSize - start
			}
		} else if err := file.WaitForRange(ctx, start, size); err != nil {
			// Do not speculate past the final byte count. While rendering,
			// queue only complete ranges that the native safe watermark has
			// actually published. If finalization races this wait, convert
			// the last speculative full chunk into the real short tail.
			_, finalSize, finalized, _, aborted = file.snapshot()
			if aborted != nil {
				producerErr = aborted
				break
			}
			if !finalized {
				producerErr = err
				break
			}
			if start >= finalSize {
				break
			}
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
		select {
		case parts <- part{number: number, start: start, size: size}:
		case <-ctx.Done():
			producerErr = ctx.Err()
		}
		if producerErr != nil {
			break
		}
		start += size
	}
	if producerErr != nil {
		select {
		case errs <- producerErr:
		default:
		}
		cancel()
	}
	close(parts)
	wg.Wait()
	select {
	case err := <-errs:
		cancelHash()
		_ = session.Abort(context.Background())
		return nil, err
	default:
	}
	_, finalSize, finalized, durable, _ := file.snapshot()
	if !finalized || !durable {
		cancelHash()
		_ = session.Abort(context.Background())
		return nil, fmt.Errorf("progressive upload: output is not finalized and durable")
	}
	mu.Lock()
	doneParts := uploadedParts
	doneBytes := uploaded
	mu.Unlock()
	hashed := <-hashDone
	if hashed.err != nil {
		_ = session.Abort(context.Background())
		return nil, hashed.err
	}
	sha := hashed.sha
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

// hashGrowingFile computes the final digest while the progressive uploader is
// consuming the growing output. The reader is independent from the upload
// file descriptor and uses ReadAt-backed section readers, so hashing never
// seeks the descriptor used by concurrent UploadPart calls. This removes the
// old serial full-file read after the upload/finalize boundary.
func hashGrowingFile(ctx context.Context, path string, file *GrowingFile) (string, error) {
	if path == "" || file == nil {
		return "", fmt.Errorf("progressive upload: invalid incremental hash input")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	var offset int64
	for {
		_, finalSize, finalized, _, aborted := file.snapshot()
		if aborted != nil {
			return "", aborted
		}
		if finalized {
			if finalSize <= offset {
				break
			}
			length := int64(progressiveHashReadSize)
			if remaining := finalSize - offset; remaining < length {
				length = remaining
			}
			if _, err := io.CopyN(h, io.NewSectionReader(f, offset, length), length); err != nil {
				return "", err
			}
			offset += length
			continue
		}

		length := int64(progressiveHashReadSize)
		if err := waitForReadableRange(ctx, f, file, offset, length); err != nil {
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			continue
		}
		if _, err := io.CopyN(h, io.NewSectionReader(f, offset, length), length); err != nil {
			return "", err
		}
		offset += length
	}
	if offset <= 0 {
		return "", fmt.Errorf("progressive upload: incremental hash saw no bytes")
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func hashFile(path string, size int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return hashOpenFile(f, size)
}

func hashOpenFile(f *os.File, size int64) (string, error) {
	if f == nil || size <= 0 {
		return "", fmt.Errorf("progressive upload: invalid hash input")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := io.CopyN(h, f, size); err != nil && err != io.EOF {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
