package publisher

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MasterStreamTransport streams a local file to the master's chunked upload
// HTTP endpoint. It is intended for development, small uploads, and E2E tests.
type MasterStreamTransport struct {
	// HTTPClient is the per-call HTTP client. A five-minute timeout is used
	// when it is nil.
	HTTPClient *http.Client
}

func (t *MasterStreamTransport) ID() string { return TransportIDMasterStream }

// chunkSize is the fallback per-request chunk size for the master-stream
// transport. Uploads are streamed directly from the file; only this many
// bytes are exposed to a request at a time.
const chunkSize int64 = 8 * 1024 * 1024

// masterStreamConcurrency bounds in-flight requests. Four 8 MiB readers keep
// the transfer parallel without buffering the video or putting pressure on
// the worker's memory budget.
const masterStreamConcurrency = 4

// Upload implements Transport.Upload for MasterStreamTransport.
func (t *MasterStreamTransport) Upload(ctx context.Context, req UploadRequest) (*UploadResult, error) {
	if req.LocalPath == "" {
		return nil, fmt.Errorf("master-stream: LocalPath empty")
	}
	if req.Target.UploadURL == "" {
		return nil, fmt.Errorf("master-stream: UploadURL empty (no plan received yet?)")
	}

	client := t.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}

	f, err := os.Open(req.LocalPath)
	if err != nil {
		return nil, fmt.Errorf("master-stream: open %s: %w", req.LocalPath, err)
	}
	defer f.Close()

	if _, err := f.Stat(); err != nil {
		return nil, fmt.Errorf("master-stream: stat: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("master-stream: stat: %w", err)
	}
	perChunk := req.Target.ChunkSize
	if perChunk <= 0 {
		perChunk = chunkSize
	}
	chunkCount := (info.Size() + perChunk - 1) / perChunk
	if info.Size() == 0 {
		chunkCount = 0
	}

	// Each request owns a SectionReader, so chunks can be sent out of order
	// while the master persists them by index. The final /complete request is
	// intentionally held until every worker has finished.
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int64)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	var uploadedMu sync.Mutex
	uploaded := int64(0)
	workerCount := masterStreamConcurrency
	if chunkCount < int64(workerCount) {
		workerCount = int(chunkCount)
	}
	uploadChunk := func(index int64) error {
		start := index * perChunk
		length := perChunk
		if remaining := info.Size() - start; remaining < length {
			length = remaining
		}
		chunkURL := strings.TrimRight(req.Target.UploadURL, "/") + "/" + strconv.FormatInt(index, 10)
		body := io.NewSectionReader(f, start, length)
		httpReq, err := http.NewRequestWithContext(workCtx, http.MethodPost, chunkURL, body)
		if err != nil {
			return fmt.Errorf("master-stream: build chunk request: %w", err)
		}
		httpReq.ContentLength = length
		httpReq.Header.Set("Content-Type", "application/octet-stream")
		httpReq.Header.Set("X-Upload-Id", req.Target.UploadID)
		httpReq.Header.Set("X-Worker-SHA256", req.WorkerSHA256)
		httpReq.Header.Set("X-Artifact-Commit-Token", req.CommitToken)
		resp, err := client.Do(httpReq)
		if err != nil {
			return fmt.Errorf("%w: master-stream chunk %d: %v", ErrUploadFailed, index, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("%w: master-stream chunk %d: HTTP %d", ErrUploadFailed, index, resp.StatusCode)
		}
		uploadedMu.Lock()
		uploaded += length
		current := uploaded
		uploadedMu.Unlock()
		if req.Progress != nil {
			req.Progress(current)
		}
		return nil
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				if err := uploadChunk(index); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
						cancel()
					}
					errMu.Unlock()
				}
			}
		}()
	}
	for index := int64(0); index < chunkCount; index++ {
		select {
		case jobs <- index:
		case <-workCtx.Done():
			break
		}
		select {
		case <-workCtx.Done():
			index = chunkCount
		default:
		}
	}
	close(jobs)
	wg.Wait()
	errMu.Lock()
	uploadErr := firstErr
	errMu.Unlock()
	if uploadErr != nil {
		return nil, uploadErr
	}

	completeURL := strings.TrimRight(req.Target.UploadURL, "/") + "/complete"
	compReq, err := http.NewRequestWithContext(ctx, http.MethodPost, completeURL, nil)
	if err != nil {
		return nil, fmt.Errorf("master-stream: build complete request: %w", err)
	}
	compReq.Header.Set("X-Upload-Id", req.Target.UploadID)
	compReq.Header.Set("X-Worker-SHA256", req.WorkerSHA256)
	compReq.Header.Set("X-Artifact-Commit-Token", req.CommitToken)
	resp, err := client.Do(compReq)
	if err != nil {
		return nil, fmt.Errorf("%w: master-stream complete: %v", ErrUploadFailed, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%w: master-stream complete: HTTP %d body=%s",
			ErrUploadFailed, resp.StatusCode, string(body))
	}

	// A missing server-side SHA must remain empty. The master must not advance
	// an artifact to COMPLETED using only a worker self-report.
	serverSHA := ""
	if s := extractJSONString(body, `"sha256"`); s != "" && isLowerHex64(s) {
		serverSHA = s
	} else if s := extractJSONString(body, `"output_sha256"`); s != "" && isLowerHex64(s) {
		serverSHA = s
	}
	if serverSHA != "" && serverSHA != req.WorkerSHA256 {
		return nil, fmt.Errorf("%w: worker=%s server=%s",
			ErrChecksumMismatch, req.WorkerSHA256, serverSHA)
	}

	return &UploadResult{
		UploadID:      req.Target.UploadID,
		UploadedBytes: uploaded,
		ServerSHA256:  serverSHA,
	}, nil
}
