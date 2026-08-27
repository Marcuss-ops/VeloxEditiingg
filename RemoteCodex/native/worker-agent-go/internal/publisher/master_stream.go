package publisher

import (
	"velox-shared/controltransport"

	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
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

func (t *MasterStreamTransport) Capabilities() CapabilitySet {
	return CapabilitySet{controltransport.CapabilityArtifactProgressiveUploadV1}
}

func (t *MasterStreamTransport) BeginProgressive(ctx context.Context, req ProgressiveUploadRequest) (ProgressiveSession, error) {
	return t.ResumeProgressive(ctx, req, nil)
}

func (t *MasterStreamTransport) ResumeProgressive(ctx context.Context, req ProgressiveUploadRequest, completed []int) (ProgressiveSession, error) {
	if req.Target.UploadURL == "" {
		return nil, fmt.Errorf("master-stream: progressive UploadURL empty")
	}
	return &masterStreamProgressiveSession{transport: t, request: req, completed: append([]int(nil), completed...)}, nil
}

type masterStreamProgressiveSession struct {
	transport *MasterStreamTransport
	request   ProgressiveUploadRequest
	completed []int
}

func (s *masterStreamProgressiveSession) UploadPart(ctx context.Context, partNumber int, reader io.Reader, size int64) error {
	body, err := io.ReadAll(io.LimitReader(reader, size))
	if err != nil {
		return err
	}
	if int64(len(body)) != size {
		return fmt.Errorf("master-stream: progressive part size mismatch")
	}
	client := s.transport.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	url := strings.TrimRight(s.request.Target.UploadURL, "/") + "/" + strconv.Itoa(partNumber)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Upload-Id", s.request.Target.UploadID)
	req.Header.Set("X-Artifact-Commit-Token", s.request.CommitToken)
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: progressive part %d: %v", ErrUploadFailed, partNumber, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%w: progressive part %d: HTTP %d", ErrUploadFailed, partNumber, resp.StatusCode)
	}
	return nil
}

func (s *masterStreamProgressiveSession) Complete(ctx context.Context, final FinalArtifactIdentity) (*UploadResult, error) {
	if err := validateFinalArtifactIdentity(final); err != nil {
		return nil, err
	}
	if !isLowerHex64(final.SHA256) {
		return nil, fmt.Errorf("master-stream: final SHA-256 is invalid")
	}
	client := s.transport.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	url := strings.TrimRight(s.request.Target.UploadURL, "/") + "/complete"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Upload-Id", s.request.Target.UploadID)
	req.Header.Set("X-Worker-SHA256", final.SHA256)
	req.Header.Set("X-Artifact-Commit-Token", s.request.CommitToken)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: progressive complete: %v", ErrUploadFailed, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%w: progressive complete: HTTP %d", ErrUploadFailed, resp.StatusCode)
	}
	serverSHA := extractJSONString(body, `"sha256"`)
	if serverSHA != "" && serverSHA != final.SHA256 {
		return nil, fmt.Errorf("%w: worker=%s server=%s", ErrChecksumMismatch, final.SHA256, serverSHA)
	}
	return &UploadResult{UploadID: s.request.Target.UploadID, UploadedBytes: final.SizeBytes, ServerSHA256: serverSHA}, nil
}

func (s *masterStreamProgressiveSession) Abort(ctx context.Context) error { return nil }

// chunkSize is the per-request chunk size for the master-stream transport.
const chunkSize int64 = 8 * 1024 * 1024

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

	uploaded := int64(0)
	chunkIndex := 0
	measurement := &uploadTelemetry{started: time.Now()}
	buf := make([]byte, chunkSize)
	for {
		n, rerr := io.ReadFull(f, buf)
		if n > 0 {
			chunkURL := strings.TrimRight(req.Target.UploadURL, "/") +
				"/" + strconv.Itoa(chunkIndex)
			httpReq, err := http.NewRequestWithContext(ctx,
				http.MethodPost, chunkURL, bytes.NewReader(buf[:n]))
			if err != nil {
				return nil, fmt.Errorf("master-stream: build chunk request: %w", err)
			}
			httpReq.Header.Set("Content-Type", "application/octet-stream")
			httpReq.Header.Set("X-Upload-Id", req.Target.UploadID)
			httpReq.Header.Set("X-Worker-SHA256", req.WorkerSHA256)
			httpReq.Header.Set("X-Artifact-Commit-Token", req.CommitToken)
			resp, err := client.Do(httpReq)
			if err != nil {
				return nil, fmt.Errorf("%w: master-stream chunk %d: %v",
					ErrUploadFailed, chunkIndex, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 400 {
				return nil, fmt.Errorf("%w: master-stream chunk %d: HTTP %d",
					ErrUploadFailed, chunkIndex, resp.StatusCode)
			}
			uploaded += int64(n)
			measurement.ChunkCompleted(int64(n))
			if req.Telemetry != nil {
				req.Telemetry.ChunkCompleted(int64(n))
			}
			if req.Progress != nil {
				req.Progress(uploaded)
			}
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return nil, fmt.Errorf("master-stream: read chunk %d: %w", chunkIndex, rerr)
		}
		chunkIndex++
	}

	finalizeStarted := time.Now()
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
	measurement.FinalizeCompleted(time.Since(finalizeStarted))
	if req.Telemetry != nil {
		req.Telemetry.FinalizeCompleted(time.Since(finalizeStarted))
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

	result := &UploadResult{
		UploadID:      req.Target.UploadID,
		UploadedBytes: uploaded,
		ServerSHA256:  serverSHA,
		Breakdown:     measurement.Snapshot().UploadBreakdown,
	}
	return result, nil
}
