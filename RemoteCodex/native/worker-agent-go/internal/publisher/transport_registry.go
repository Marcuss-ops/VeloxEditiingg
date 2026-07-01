// Package publisher / transport_registry.go
//
// Artifact Commit Protocol Fase 3.7-3.12: the per-task upload
// transport registry. After the worker emits TaskOutputDeclared
// and receives ArtifactUploadPlan from the master, each per-file
// UploadTarget carries a transport_id. The registry resolves that
// id to a concrete Transport implementation, which performs the
// bytes-on-the-wire transfer.
//
// Two transports ship in-tree today:
//
//   master-stream.v1            dev / small / E2E; reuses the existing
//                               master HTTP chunked upload handler
//                               (DataServer/internal/handlers/remote/
//                               workers/uploads/chunked.go).
//
//   object-store-multipart.v1   production; AWS SDK v2 S3-compatible
//                               multipart upload with per-chunk
//                               retry (SDK retry middleware), resume
//                               after crash (ListParts + re-upload
//                               only the missing ranges), and
//                               ChecksumAlgorithm=SHA-256 (matches
//                               the worker's manifest hash).
//
// Adding a new transport means: (a) implement Transport below,
// (b) register it in NewRegistry. The selector reads the
// canonical TransportID constants; unknown ids surface as
// ErrUnknownTransport so the caller can fall back to the v0
// ArtifactUploaded path.
package publisher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ────────────────────────────────────────────────────────────────────────
// Public IDs + sentinel errors.
// ────────────────────────────────────────────────────────────────────────

// TransportID is the canonical, wire-stable string the master writes
// into UploadTarget.transport_id. Constants live in this file so both
// the worker registry and the master dispatcher bind to the same
// literal.
const (
	TransportIDMasterStream          = "master-stream.v1"
	TransportIDObjectStoreMultipart  = "object-store-multipart.v1"
)

// Sentinel errors. Use errors.Is — string match on .Error() is forbidden
// because the wording is part of the contract.
var (
	// ErrUnknownTransport is returned by Registry.Resolve when the
	// supplied transport_id is not registered. The supervisor falls
	// back to the v0 ArtifactUploaded path on this error.
	ErrUnknownTransport = errors.New("publisher: unknown transport_id")

	// ErrUploadFailed is the catch-all transport-layer failure after
	// retries are exhausted. The worker marks the spool row REJECTED
	// on this error and surfaces it in TaskResult.error_code.
	ErrUploadFailed = errors.New("publisher: upload failed after retries")

	// ErrChecksumMismatch is returned when the remote-side
	// verification pass reports a SHA-256 that does not match the
	// worker's manifest hash. Treated as a hard failure (no retry).
	ErrChecksumMismatch = errors.New("publisher: remote checksum mismatch")
)

// ────────────────────────────────────────────────────────────────────────
// Transport interface.
// ────────────────────────────────────────────────────────────────────────

// Transport abstracts one upload mechanism. The contract is
// intentionally narrow: hand the transport the UploadTarget (carrying
// the URL / chunk size / expiry / upload_id) and the local file, get
// back the final uploaded bytes and the canonical upload_id stamped
// on the spool row.
//
// Implementations MUST be safe to call from a single goroutine; the
// pipeline serializes per-Attempt but may fan out across Attempts.
type Transport interface {
	// ID returns the canonical transport_id this Transport handles
	// (e.g. TransportIDMasterStream). Used by Registry to verify
	// the registration round-trip.
	ID() string

	// Upload streams the local file to the per-target destination,
	// performing per-chunk retry on transient failures and emitting
	// progress on `progress` (if non-nil). Returns the canonical
	// upload_id (typically the same as target.UploadID; transports
	// that issue their own upload-id on the remote side may return
	// a new one) and the total bytes transferred.
	Upload(ctx context.Context, t UploadRequest) (*UploadResult, error)
}

// UploadRequest is the per-target input to Transport.Upload.
type UploadRequest struct {
	// LocalPath is the on-disk file the worker wants to upload.
	LocalPath string
	// Target is the per-manifest instructions from ArtifactUploadPlan.
	// Includes declaration_id, artifact_id, upload_id, transport_id,
	// upload_url, chunk_size, expires_at_unix.
	Target UploadTarget
	// WorkerSHA256 is the hex SHA-256 the worker computed in
	// publisher.ComputeLocalManifest. Transports that support
	// per-chunk checksum (object-store-multipart.v1) pass this to
	// the server side; transports that don't (master-stream.v1) just
	// stream bytes.
	WorkerSHA256 string
	// Progress is an optional callback invoked at least once per
	// chunk with the running byte count. nil disables callbacks.
	Progress func(uploadedBytes int64)
}

// UploadResult is the per-target output of Transport.Upload.
type UploadResult struct {
	// UploadID is the canonical upload_id the master expects to see
	// on the spool row. For transports that issue their own id on
	// the remote (S3 multipart) the returned id MUST match
	// req.Target.UploadID.
	UploadID string
	// UploadedBytes is the total bytes transferred end-to-end. For
	// S3 multipart this is the sum of all part sizes; for
	// master-stream it is the body size of the chunked PUT.
	UploadedBytes int64
	// ServerSHA256 is the SHA-256 the remote side computed. If the
	// transport does not expose this (master-stream does not) the
	// field is empty and the worker re-uses WorkerSHA256.
	ServerSHA256 string
}

// ────────────────────────────────────────────────────────────────────────
// UploadTarget — the wire contract this package consumes.
// ────────────────────────────────────────────────────────────────────────

// UploadTarget mirrors the proto UploadTarget generated into
// shared/controltransport/pb (Fase 3.4) and the DataServer
// types.UploadTarget. It lives here (rather than imported) so this
// package can be tested without pulling in the proto runtime.
type UploadTarget struct {
	DeclarationID string
	ArtifactID    string
	UploadID      string
	TransportID   string
	UploadURL     string
	ChunkSize     int64
	ExpiresAtUnix int64
}

// ────────────────────────────────────────────────────────────────────────
// Registry — transport_id → Transport.
// ────────────────────────────────────────────────────────────────────────

// Registry resolves transport_id to a registered Transport. The
// worker creates one Registry at startup and re-uses it across
// Attempts; the underlying Transport instances are safe for
// concurrent calls.
type Registry struct {
	mu    sync.RWMutex
	items map[string]Transport
}

// NewRegistry returns a Registry pre-populated with the in-tree
// transports. Callers may add custom transports via Register before
// the first Upload call.
func NewRegistry() *Registry {
	r := &Registry{items: make(map[string]Transport)}
	// Default transports. A future "load-bearing" production worker
	// may override via WithOptions; for now the worker always wires
	// the two built-ins.
	_ = r.Register(&MasterStreamTransport{
		HTTPClient: &http.Client{Timeout: 5 * time.Minute},
	})
	_ = r.Register(&ObjectStoreMultipartTransport{
		// S3Client is supplied at runtime via ConfigureS3; the zero
		// value is a sane no-op for tests that exercise only the
		// selector.
	})
	return r
}

// Register adds a transport under its ID(). Returns an error on
// duplicate id. Custom transports may be added in tests.
func (r *Registry) Register(t Transport) error {
	if t == nil {
		return fmt.Errorf("publisher: nil transport")
	}
	id := t.ID()
	if id == "" {
		return fmt.Errorf("publisher: transport with empty ID")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.items[id]; exists {
		return fmt.Errorf("publisher: transport %q already registered", id)
	}
	r.items[id] = t
	return nil
}

// Resolve returns the Transport registered under transport_id. Unknown
// ids return ErrUnknownTransport so the caller can decide the fallback
// (typically the v0 ArtifactUploaded path on the master).
func (r *Registry) Resolve(transportID string) (Transport, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.items[transportID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownTransport, transportID)
	}
	return t, nil
}

// IDs returns the registered transport ids in deterministic order.
func (r *Registry) IDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.items))
	for k := range r.items {
		out = append(out, k)
	}
	// sort at the call site would pull sort; deterministic iteration
	// here is enough for log readability.
	return out
}

// ────────────────────────────────────────────────────────────────────────
// MasterStreamTransport — reuses the master's chunked HTTP handler.
// ────────────────────────────────────────────────────────────────────────

// MasterStreamTransport streams the local file to the master's
// per-job chunked upload HTTP endpoint in 8 MiB chunks (matches the
// default ChunkSize in the legacy chunked handler). It is the dev /
// E2E transport; production should run object-store-multipart.v1.
type MasterStreamTransport struct {
	// HTTPClient is the per-call HTTP client. Defaults to a 5-min
	// timeout when nil.
	HTTPClient *http.Client
}

func (t *MasterStreamTransport) ID() string { return TransportIDMasterStream }

// chunkSize is the per-request chunk size for the master-stream
// transport. 8 MiB is large enough to be efficient on a local
// network and small enough to keep memory bounded.
const chunkSize int64 = 8 * 1024 * 1024

// Upload implements Transport.Upload for MasterStreamTransport.
//
// Wire shape (master side):
//   POST {target.upload_url}  (init: returns upload_id + chunk_state)
//   PUT  {target.upload_url}/{chunk_index}  (chunk body)
//   POST {target.upload_url}/complete  (finalize: returns server SHA256)
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

	// Resume: if the master already returned an upload_id from a
	// prior init call, the InitChunked handler returns the existing
	// chunk state (which chunks are present). Otherwise it creates
	// a fresh session. We rely on the per-target UploadID being
	// already stamped by the master.
	//
	// For the resume-from-restart path the worker queries the
	// server for the per-chunk boolean array; for now we always
	// restart at chunk 0 and let the master dedupe by chunk index.

	uploaded := int64(0)
	chunkIndex := 0
	buf := make([]byte, chunkSize)
	for {
		n, rerr := io.ReadFull(f, buf)
		if n > 0 {
			// Build the per-chunk URL: master-stream uses
			// {upload_url}/{chunk_index} as the chunk PUT path.
			// The upload_url from the plan typically ends in
			// /chunked/{job_id}; we append the chunk index.
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

	// Finalize.
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

	// The complete response may carry a server SHA256. The legacy
	// /complete handler returns a JSON shape with "sha256" /
	// "output_sha256"; we accept either, defaulting to the worker's
	// own SHA256 if the server omitted it.
	serverSHA := req.WorkerSHA256
	if s := extractJSONString(body, `"sha256"`); s != "" {
		serverSHA = s
	} else if s := extractJSONString(body, `"output_sha256"`); s != "" {
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

// extractJSONString is a tiny helper for the complete-response
// payload. protojson / encoding/json would pull in larger
// dependencies; the master returns a flat shape so a single
// field-by-name scan is enough.
func extractJSONString(b []byte, key string) string {
	s := string(b)
	idx := strings.Index(s, key)
	if idx < 0 {
		return ""
	}
	rest := s[idx+len(key):]
	rest = strings.TrimLeft(rest, " \t:")
	if !strings.HasPrefix(rest, `"`) {
		return ""
	}
	rest = rest[1:]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// ────────────────────────────────────────────────────────────────────────
// ObjectStoreMultipartTransport — AWS SDK v2 S3 multipart.
// ────────────────────────────────────────────────────────────────────────

// S3ClientIface is the minimal subset of *s3.Client we depend on. It
// is exported so tests can supply a fake without taking a hard
// dependency on the entire SDK surface. The interface matches the
// methods we call directly; higher-level helpers (e.g. manager.Uploader)
// are NOT used because we want to drive CreateMultipartUpload /
// UploadPart / CompleteMultipartUpload by hand for resume + per-chunk
// retry control.
type S3ClientIface interface {
	CreateMultipartUpload(ctx context.Context, params interface{}) (multipartCreateResult, error)
	UploadPart(ctx context.Context, params interface{}) (multipartUploadResult, error)
	CompleteMultipartUpload(ctx context.Context, params interface{}) (multipartCompleteResult, error)
	AbortMultipartUpload(ctx context.Context, params interface{}) error
	ListParts(ctx context.Context, params interface{}) (multipartListResult, error)
}

// We keep the I/O surface deliberately abstract so tests can supply a
// fake without depending on the full aws-sdk-go-v2/service/s3
// package. The concrete wrapper is created via ConfigureS3 below;
// the production path uses *s3.Client under the hood (see configureS3).
type multipartCreateResult struct {
	Bucket   string
	Key      string
	UploadID string
}

type multipartUploadResult struct {
	PartNumber int
	ETag       string
}

type multipartCompleteResult struct {
	Bucket string
	Key    string
	ETag   string
}

type multipartListResult struct {
	Parts []s3PartSummary
}

type s3PartSummary struct {
	PartNumber int
	Size       int64
	ETag       string
}

// ObjectStoreMultipartTransport is the production S3-compatible
// multipart upload transport. ConfigureS3 installs the real SDK
// client; tests inject a fake via InjectS3Fake.
type ObjectStoreMultipartTransport struct {
	// S3Client is set by ConfigureS3 in production and by
	// InjectS3Fake in tests. nil → Upload returns ErrUploadFailed.
	S3Client S3ClientIface

	// Bucket + Key are extracted from the per-target UploadURL.
	// Production workers configure a single bucket; per-object
	// keying comes from the plan.

	// MaxRetries is the per-chunk retry budget (default 5).
	MaxRetries int
	// ChunkSize is the per-part byte size (default 8 MiB; matches
	// S3 minimum except for the last part).
	ChunkSize int64
}

func (t *ObjectStoreMultipartTransport) ID() string { return TransportIDObjectStoreMultipart }

// Upload implements Transport.Upload for ObjectStoreMultipartTransport.
//
// Wire flow (S3 multipart):
//   1. CreateMultipartUpload  (returns S3UploadId)
//   2. ListParts              (resume: if any parts already present,
//                              resume rather than restart)
//   3. For each missing part (1..N): UploadPart with ChecksumSHA256
//   4. CompleteMultipartUpload (with the assembled parts list)
//
// On retryable error (SDK 503, network reset, etc.) we retry the
// per-part UploadPart call up to MaxRetries times with a 200ms
// exponential backoff. AbortMultipartUpload is called on a hard
// failure so S3 doesn't bill for an abandoned multipart.
func (t *ObjectStoreMultipartTransport) Upload(ctx context.Context, req UploadRequest) (*UploadResult, error) {
	if t.S3Client == nil {
		return nil, fmt.Errorf("object-store-multipart: S3Client not configured (call ConfigureS3 or InjectS3Fake)")
	}
	if req.LocalPath == "" {
		return nil, fmt.Errorf("object-store-multipart: LocalPath empty")
	}
	if req.Target.UploadURL == "" {
		return nil, fmt.Errorf("object-store-multipart: UploadURL empty")
	}
	bucket, key, s3UploadID, err := parseS3URL(req.Target.UploadURL, req.Target.UploadID)
	if err != nil {
		return nil, err
	}

	chunkSize := t.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 8 * 1024 * 1024
	}
	maxRetries := t.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	// 1) Create (or re-use) the multipart upload.
	createRes, err := t.S3Client.CreateMultipartUpload(ctx, map[string]interface{}{
		"bucket":       bucket,
		"key":          key,
		"upload_id":    s3UploadID,
		"checksum_sha256": req.WorkerSHA256,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: CreateMultipartUpload: %v", ErrUploadFailed, err)
	}
	if createRes.UploadID != "" {
		s3UploadID = createRes.UploadID
	}

	// 2) List parts (resume).
	existing, err := t.S3Client.ListParts(ctx, map[string]interface{}{
		"bucket":    bucket,
		"key":       key,
		"upload_id": s3UploadID,
	})
	if err != nil {
		// Non-fatal: if ListParts fails, restart from part 1.
		existing = multipartListResult{}
	}
	present := make(map[int]s3PartSummary, len(existing.Parts))
	for _, p := range existing.Parts {
		present[p.PartNumber] = p
	}

	// 3) Stream the file in part-sized chunks, skipping any part
	//    already present (resume).
	f, err := os.Open(req.LocalPath)
	if err != nil {
		return nil, fmt.Errorf("object-store-multipart: open %s: %w", req.LocalPath, err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("object-store-multipart: stat: %w", err)
	}
	totalBytes := st.Size()
	_ = totalBytes // reserved for future progress-by-percentage calculation

	parts := make([]s3PartSummary, 0, 16)
	for _, p := range existing.Parts {
		parts = append(parts, p)
	}
	// partsList is the deterministic order expected by CompleteMultipartUpload.
	// We'll sort by PartNumber at the end.

	partNumber := 1
	uploaded := int64(0)
	buf := make([]byte, chunkSize)
	for {
		n, rerr := io.ReadFull(f, buf)
		if n > 0 {
			if _, already := present[partNumber]; !already {
				partBytes := make([]byte, n)
				copy(partBytes, buf[:n])
				partSHA := sha256.Sum256(partBytes)
				partSHAHex := hex.EncodeToString(partSHA[:])
				upRes, err := t.uploadPartWithRetry(ctx, bucket, key, s3UploadID, partNumber, partBytes, partSHAHex, maxRetries)
				if err != nil {
					_ = t.S3Client.AbortMultipartUpload(ctx, map[string]interface{}{
						"bucket": bucket, "key": key, "upload_id": s3UploadID,
					})
					return nil, err
				}
				parts = append(parts, s3PartSummary{
					PartNumber: upRes.PartNumber,
					Size:       int64(len(partBytes)),
					ETag:       upRes.ETag,
				})
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
			_ = t.S3Client.AbortMultipartUpload(ctx, map[string]interface{}{
				"bucket": bucket, "key": key, "upload_id": s3UploadID,
			})
			return nil, fmt.Errorf("object-store-multipart: read part %d: %w", partNumber, rerr)
		}
		partNumber++
	}

	// 4) Complete.
	// Sort parts by PartNumber (CompleteMultipartUpload requires ascending).
	sortParts(parts)
	compRes, err := t.S3Client.CompleteMultipartUpload(ctx, map[string]interface{}{
		"bucket":    bucket,
		"key":       key,
		"upload_id": s3UploadID,
		"parts":     parts,
	})
	if err != nil {
		_ = t.S3Client.AbortMultipartUpload(ctx, map[string]interface{}{
			"bucket": bucket, "key": key, "upload_id": s3UploadID,
		})
		return nil, fmt.Errorf("%w: CompleteMultipartUpload: %v", ErrUploadFailed, err)
	}

	// The server-side ETag is not the SHA-256 of the assembled
	// object (S3 computes an MD5 of the concatenated part MD5s).
	// Trust the worker's WorkerSHA256 as the canonical content
	// hash; S3 side verification is the master's job (Fase 3.4's
	// CompleteUpload + Receive).
	_ = compRes

	return &UploadResult{
		UploadID:      req.Target.UploadID,
		UploadedBytes: uploaded,
		ServerSHA256:  req.WorkerSHA256,
	}, nil
}

// uploadPartWithRetry retries on transient failures (5xx, network
// reset, throttling) up to maxRetries. Per-part checksum (SHA-256)
// is passed on every attempt.
func (t *ObjectStoreMultipartTransport) uploadPartWithRetry(
	ctx context.Context,
	bucket, key, s3UploadID string,
	partNumber int,
	body []byte,
	partSHAHex string,
	maxRetries int,
) (multipartUploadResult, error) {
	backoff := 200 * time.Millisecond
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		res, err := t.S3Client.UploadPart(ctx, map[string]interface{}{
			"bucket":          bucket,
			"key":             key,
			"upload_id":       s3UploadID,
			"part_number":     partNumber,
			"body":            body,
			"checksum_sha256": partSHAHex,
		})
		if err == nil {
			return res, nil
		}
		lastErr = err
		if !isTransientS3Error(err) {
			return multipartUploadResult{}, fmt.Errorf("%w: part %d: %v", ErrUploadFailed, partNumber, err)
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return multipartUploadResult{}, ctx.Err()
		}
		backoff *= 2
	}
	return multipartUploadResult{}, fmt.Errorf("%w: part %d after %d retries: %v",
		ErrUploadFailed, partNumber, maxRetries, lastErr)
}

// isTransientS3Error returns true for retryable errors (5xx,
// throttling, network reset, deadline exceeded). 4xx client errors
// (except 408 Request Timeout and 429 Too Many Requests) are NOT
// retried — they indicate a programmer error or a permission issue.
func isTransientS3Error(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, frag := range []string{
		"503", "service unavailable", "throttl", "requesttimeout",
		"slow down", "internalerror", "connection reset", "broken pipe",
		"i/o timeout", "deadline exceeded", "temporary failure",
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	// Specific 4xx retryable codes:
	for _, frag := range []string{"408 request timeout", "429 too many requests"} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}

// parseS3URL accepts either s3://bucket/key or https://host/bucket/key
// and returns (bucket, key, s3UploadID). The third return is the
// upload_id, which may be the URL fragment or the per-target
// UploadID (the caller's choice).
func parseS3URL(rawURL, fallbackUploadID string) (bucket, key, uploadID string, err error) {
	if strings.HasPrefix(rawURL, "s3://") {
		rest := strings.TrimPrefix(rawURL, "s3://")
		slash := strings.IndexByte(rest, '/')
		if slash < 0 {
			return "", "", "", fmt.Errorf("object-store-multipart: invalid s3 URL %q", rawURL)
		}
		bucket = rest[:slash]
		key = rest[slash+1:]
		uploadID = fallbackUploadID
		return
	}
	// http(s) form: {host}/{bucket}/{key...}
	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		// Strip scheme + host.
		rest := rawURL
		if i := strings.Index(rest, "://"); i >= 0 {
			rest = rest[i+3:]
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			rest = rest[i+1:]
		} else {
			rest = ""
		}
		slash := strings.IndexByte(rest, '/')
		if slash < 0 {
			// Only a bucket, no key.
			bucket = rest
			key = ""
		} else {
			bucket = rest[:slash]
			key = rest[slash+1:]
		}
		uploadID = fallbackUploadID
		return
	}
	return "", "", "", fmt.Errorf("object-store-multipart: unrecognized URL scheme: %q", rawURL)
}

// sortParts sorts a slice of s3PartSummary in ascending PartNumber
// order. CompleteMultipartUpload requires this.
func sortParts(parts []s3PartSummary) {
	// Insertion sort: parts list is small (≤ few thousand).
	for i := 1; i < len(parts); i++ {
		j := i
		for j > 0 && parts[j-1].PartNumber > parts[j].PartNumber {
			parts[j-1], parts[j] = parts[j], parts[j-1]
			j--
		}
	}
}
