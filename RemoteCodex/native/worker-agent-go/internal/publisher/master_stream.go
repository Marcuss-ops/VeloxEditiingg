package publisher

import (
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

	completeURL := strings.TrimRight(req.Target.UploadURL, "/") + "/complete"
	compReq, err := http.NewRequestWithContext(ctx, http.MethodPost, completeURL, nil)
	if err != nil {
		return nil, fmt.Errorf("master-stream: build complete request: %w", err)
	}
	compReq.Header.Set("X-Upload-Id", req.Target.UploadID)
	compReq.Header.Set("X-Worker-SHA256", req.WorkerSHA256)
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
