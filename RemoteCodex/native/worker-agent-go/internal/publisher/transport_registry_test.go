// Tests for the transport registry (Fase 3.7-3.12 of the Artifact
// Commit Protocol). The S3 path uses an in-process fake
// (fakeS3Client) so we don't depend on network. The master-stream
// path uses httptest so we don't depend on a master process.
package publisher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// ────────────────────────────────────────────────────────────────────────
// fakeS3Client — implements S3ClientIface for tests.
// ────────────────────────────────────────────────────────────────────────

type fakeS3Client struct {
	mu sync.Mutex

	// Pre-seeded "already uploaded" parts (for resume tests).
	existingParts []s3PartSummary

	// Per-method override hooks. nil → fall through to default.
	createFn     func(ctx context.Context, params interface{}) (multipartCreateResult, error)
	uploadPartFn func(ctx context.Context, params interface{}) (multipartUploadResult, error)
	completeFn   func(ctx context.Context, params interface{}) (multipartCompleteResult, error)
	abortFn      func(ctx context.Context, params interface{}) error
	listPartsFn  func(ctx context.Context, params interface{}) (multipartListResult, error)

	// Counters.
	createCalls    atomic.Int32
	uploadCalls    atomic.Int32
	completeCalls  atomic.Int32
	abortCalls     atomic.Int32
	listPartsCalls atomic.Int32
}

func (f *fakeS3Client) CreateMultipartUpload(ctx context.Context, params interface{}) (multipartCreateResult, error) {
	f.createCalls.Add(1)
	if f.createFn != nil {
		return f.createFn(ctx, params)
	}
	p, _ := params.(map[string]interface{})
	bucket, _ := p["bucket"].(string)
	key, _ := p["key"].(string)
	uploadID, _ := p["upload_id"].(string)
	return multipartCreateResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}, nil
}

func (f *fakeS3Client) UploadPart(ctx context.Context, params interface{}) (multipartUploadResult, error) {
	f.uploadCalls.Add(1)
	if f.uploadPartFn != nil {
		return f.uploadPartFn(ctx, params)
	}
	p, _ := params.(map[string]interface{})
	pn, _ := p["part_number"].(int)
	body, _ := p["body"].([]byte)
	sum := sha256.Sum256(body)
	return multipartUploadResult{
		PartNumber: pn,
		ETag:       `"` + hex.EncodeToString(sum[:]) + `"`,
	}, nil
}

func (f *fakeS3Client) CompleteMultipartUpload(ctx context.Context, params interface{}) (multipartCompleteResult, error) {
	f.completeCalls.Add(1)
	if f.completeFn != nil {
		return f.completeFn(ctx, params)
	}
	p, _ := params.(map[string]interface{})
	bucket, _ := p["bucket"].(string)
	key, _ := p["key"].(string)
	parts, _ := p["parts"].([]s3PartSummary)
	var etag string
	if len(parts) > 0 {
		etag = parts[0].ETag
	}
	return multipartCompleteResult{Bucket: bucket, Key: key, ETag: etag}, nil
}

func (f *fakeS3Client) AbortMultipartUpload(ctx context.Context, params interface{}) error {
	f.abortCalls.Add(1)
	if f.abortFn != nil {
		return f.abortFn(ctx, params)
	}
	return nil
}

func (f *fakeS3Client) ListParts(ctx context.Context, params interface{}) (multipartListResult, error) {
	f.listPartsCalls.Add(1)
	if f.listPartsFn != nil {
		return f.listPartsFn(ctx, params)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return multipartListResult{Parts: f.existingParts}, nil
}

// ────────────────────────────────────────────────────────────────────────
// 1. Transport selection: Registry.Resolve by transport_id.
// ────────────────────────────────────────────────────────────────────────

func TestRegistry_ResolvesBuiltins(t *testing.T) {
	r := NewRegistry()

	tt, err := r.Resolve(TransportIDMasterStream)
	if err != nil {
		t.Fatalf("Resolve(master-stream.v1) → %v", err)
	}
	if tt.ID() != TransportIDMasterStream {
		t.Errorf("transport.ID = %q; want %q", tt.ID(), TransportIDMasterStream)
	}

	ot, err := r.Resolve(TransportIDObjectStoreMultipart)
	if err != nil {
		t.Fatalf("Resolve(object-store-multipart.v1) → %v", err)
	}
	if ot.ID() != TransportIDObjectStoreMultipart {
		t.Errorf("transport.ID = %q; want %q", ot.ID(), TransportIDObjectStoreMultipart)
	}
}

func TestRegistry_UnknownTransportID_Errors(t *testing.T) {
	r := NewRegistry()
	_, err := r.Resolve("nonexistent.v9")
	if !errors.Is(err, ErrUnknownTransport) {
		t.Fatalf("Resolve(unknown) = %v; want ErrUnknownTransport", err)
	}
}

func TestRegistry_Register_RejectsDuplicate(t *testing.T) {
	r := NewRegistry()
	err := r.Register(&MasterStreamTransport{})
	if err == nil {
		t.Fatalf("Register(master-stream) duplicate: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "already registered") {
		t.Errorf("Register dup error = %v; want 'already registered' substring", err)
	}
}

func TestRegistry_Register_RejectsNilAndEmpty(t *testing.T) {
	r := &Registry{items: make(map[string]Transport)}
	if err := r.Register(nil); err == nil {
		t.Errorf("Register(nil): expected error")
	}
	if err := r.Register(&emptyIDTransport{}); err == nil {
		t.Errorf("Register(emptyID): expected error")
	}
}

type emptyIDTransport struct{}

func (emptyIDTransport) ID() string { return "" }
func (emptyIDTransport) Upload(_ context.Context, _ UploadRequest) (*UploadResult, error) {
	return nil, nil
}

// ────────────────────────────────────────────────────────────────────────
// 2. S3 multipart: success path (all parts uploaded).
// ────────────────────────────────────────────────────────────────────────

func TestObjectStoreMultipart_Upload_Success(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	payload := bytes.Repeat([]byte("a"), 20*1024*1024+123) // 20 MiB + tail
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	wantSHA := sha256.Sum256(payload)
	wantSHAHex := hex.EncodeToString(wantSHA[:])

	tr := &ObjectStoreMultipartTransport{
		S3Client:   &fakeS3Client{},
		ChunkSize:  5 * 1024 * 1024, // 5 MiB → 5 parts
		MaxRetries: 3,
	}

	res, err := tr.Upload(context.Background(), UploadRequest{
		LocalPath:    path,
		WorkerSHA256: wantSHAHex,
		Target: UploadTarget{
			UploadID:    "u-test-success",
			TransportID: TransportIDObjectStoreMultipart,
			UploadURL:   "s3://bucket/key",
		},
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if res.UploadedBytes != int64(len(payload)) {
		t.Errorf("UploadedBytes = %d; want %d", res.UploadedBytes, len(payload))
	}
	// Verdetto P0 #5: object-store-multipart does NOT have a server-
	// side canonical SHA-256 in this code path (S3 ETag is MD5 of
	// concatenated part MD5s). The worker MUST return ServerSHA256=""
	// so the master can fall back to its own server-side SHA derivation
	// (HEAD request, sidecar manifest, or ChecksumSHA256 metadata).
	// Returning the worker's own SHA would defeat the purpose of the
	// fail-closed default.
	if res.ServerSHA256 != "" {
		t.Errorf("ServerSHA256 = %q; want \"\" (fail-closed Verdetto P0 #5)", res.ServerSHA256)
	}

	fake := tr.S3Client.(*fakeS3Client)
	// 20 MiB / 5 MiB = 4 full parts + 1 tail = 5 parts.
	if got := fake.uploadCalls.Load(); got != 5 {
		t.Errorf("upload calls = %d; want 5", got)
	}
	if got := fake.createCalls.Load(); got != 1 {
		t.Errorf("create calls = %d; want 1", got)
	}
	if got := fake.completeCalls.Load(); got != 1 {
		t.Errorf("complete calls = %d; want 1", got)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 3. S3 multipart: RESUME — parts 1+2 already present, only re-upload 3+.
// ────────────────────────────────────────────────────────────────────────

func TestObjectStoreMultipart_Upload_ResumesAfterCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	payload := bytes.Repeat([]byte("b"), 15*1024*1024) // 15 MiB
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	wantSHA := sha256.Sum256(payload)
	wantSHAHex := hex.EncodeToString(wantSHA[:])

	// Pre-seed: parts 1 and 2 already on the server (5 MiB chunk
	// size → 3 parts total: 1, 2, 3).
	fake := &fakeS3Client{
		existingParts: []s3PartSummary{
			{PartNumber: 1, Size: 5 * 1024 * 1024, ETag: `"etag-1"`},
			{PartNumber: 2, Size: 5 * 1024 * 1024, ETag: `"etag-2"`},
		},
	}

	tr := &ObjectStoreMultipartTransport{
		S3Client:   fake,
		ChunkSize:  5 * 1024 * 1024,
		MaxRetries: 3,
	}

	res, err := tr.Upload(context.Background(), UploadRequest{
		LocalPath:    path,
		WorkerSHA256: wantSHAHex,
		Target: UploadTarget{
			UploadID:    "u-test-resume",
			TransportID: TransportIDObjectStoreMultipart,
			UploadURL:   "s3://bucket/key",
		},
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if res.UploadedBytes != int64(len(payload)) {
		t.Errorf("UploadedBytes = %d; want %d", res.UploadedBytes, len(payload))
	}

	// Only part 3 was uploaded (parts 1+2 were already present).
	if got := fake.uploadCalls.Load(); got != 1 {
		t.Errorf("upload calls = %d; want 1 (parts 1+2 already present, only part 3)", got)
	}
	if got := fake.listPartsCalls.Load(); got != 1 {
		t.Errorf("ListParts calls = %d; want 1 (resume probe)", got)
	}
	if got := fake.completeCalls.Load(); got != 1 {
		t.Errorf("Complete calls = %d; want 1", got)
	}
	if got := fake.abortCalls.Load(); got != 0 {
		t.Errorf("Abort calls = %d; want 0 (success path)", got)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 4. S3 multipart: RETRY on transient errors.
// ────────────────────────────────────────────────────────────────────────

func TestObjectStoreMultipart_Upload_RetriesOnTransient(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	// Small file: 1 part. Tests retry on the only part.
	payload := bytes.Repeat([]byte("c"), 1024)
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	wantSHA := sha256.Sum256(payload)
	wantSHAHex := hex.EncodeToString(wantSHA[:])

	attempts := atomic.Int32{}
	tr := &ObjectStoreMultipartTransport{
		S3Client: &fakeS3Client{
			uploadPartFn: func(ctx context.Context, params interface{}) (multipartUploadResult, error) {
				n := attempts.Add(1)
				if n < 3 {
					// First two attempts: simulate transient S3 503.
					return multipartUploadResult{}, fmt.Errorf("503 Service Unavailable")
				}
				p, _ := params.(map[string]interface{})
				pn, _ := p["part_number"].(int)
				body, _ := p["body"].([]byte)
				sum := sha256.Sum256(body)
				return multipartUploadResult{PartNumber: pn, ETag: `"` + hex.EncodeToString(sum[:]) + `"`}, nil
			},
		},
		ChunkSize:  1024,
		MaxRetries: 5,
	}

	res, err := tr.Upload(context.Background(), UploadRequest{
		LocalPath:    path,
		WorkerSHA256: wantSHAHex,
		Target: UploadTarget{
			UploadID:    "u-test-retry",
			TransportID: TransportIDObjectStoreMultipart,
			UploadURL:   "s3://bucket/key",
		},
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if res.UploadedBytes != int64(len(payload)) {
		t.Errorf("UploadedBytes = %d; want %d", res.UploadedBytes, len(payload))
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d; want 3 (2 transient + 1 success)", got)
	}
}

func TestObjectStoreMultipart_Upload_GivesUpAfterMaxRetries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	payload := bytes.Repeat([]byte("d"), 1024)
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	wantSHA := sha256.Sum256(payload)
	wantSHAHex := hex.EncodeToString(wantSHA[:])

	tr := &ObjectStoreMultipartTransport{
		S3Client: &fakeS3Client{
			uploadPartFn: func(ctx context.Context, params interface{}) (multipartUploadResult, error) {
				return multipartUploadResult{}, fmt.Errorf("503 Service Unavailable")
			},
		},
		ChunkSize:  1024,
		MaxRetries: 2,
	}

	_, err := tr.Upload(context.Background(), UploadRequest{
		LocalPath:    path,
		WorkerSHA256: wantSHAHex,
		Target: UploadTarget{
			UploadID:    "u-test-giveup",
			TransportID: TransportIDObjectStoreMultipart,
			UploadURL:   "s3://bucket/key",
		},
	})
	if err == nil {
		t.Fatalf("Upload: expected error after max retries")
	}
	if !errors.Is(err, ErrUploadFailed) {
		t.Errorf("error = %v; want ErrUploadFailed (wrapped)", err)
	}
}

func TestObjectStoreMultipart_Upload_NonTransientNotRetried(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	payload := bytes.Repeat([]byte("e"), 1024)
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	wantSHA := sha256.Sum256(payload)
	wantSHAHex := hex.EncodeToString(wantSHA[:])

	attempts := atomic.Int32{}
	tr := &ObjectStoreMultipartTransport{
		S3Client: &fakeS3Client{
			uploadPartFn: func(ctx context.Context, params interface{}) (multipartUploadResult, error) {
				attempts.Add(1)
				return multipartUploadResult{}, fmt.Errorf("403 Forbidden: invalid signature")
			},
		},
		ChunkSize:  1024,
		MaxRetries: 5,
	}

	_, err := tr.Upload(context.Background(), UploadRequest{
		LocalPath:    path,
		WorkerSHA256: wantSHAHex,
		Target: UploadTarget{
			UploadID:    "u-test-403",
			TransportID: TransportIDObjectStoreMultipart,
			UploadURL:   "s3://bucket/key",
		},
	})
	if err == nil {
		t.Fatalf("Upload: expected error on 403")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d; want 1 (no retry on 4xx)", got)
	}
	if !errors.Is(err, ErrUploadFailed) {
		t.Errorf("error = %v; want ErrUploadFailed (wrapped)", err)
	}
}

// ────────────────────────────────────────────────────────────────────────
// 5. isTransientS3Error truth table.
// ────────────────────────────────────────────────────────────────────────

func TestIsTransientS3Error(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"503", errors.New("503 Service Unavailable"), true},
		{"throttling", errors.New("SlowDown: throttled"), true},
		{"connection reset", errors.New("read tcp: connection reset by peer"), true},
		{"deadline exceeded", errors.New("context deadline exceeded"), true},
		{"403 forbidden", errors.New("403 Forbidden: invalid signature"), false},
		{"404 not found", errors.New("404 Not Found: NoSuchKey"), false},
		{"400 bad request", errors.New("400 Bad Request: MalformedXML"), false},
		{"empty", errors.New(""), false},
	}
	for _, c := range cases {
		if got := isTransientS3Error(c.err); got != c.want {
			t.Errorf("%s: isTransientS3Error(%q) = %v; want %v",
				c.name, c.err, got, c.want)
		}
	}
}

// ────────────────────────────────────────────────────────────────────────
// 6. parseS3URL — both schemes.
// ────────────────────────────────────────────────────────────────────────

func TestParseS3URL(t *testing.T) {
	cases := []struct {
		name       string
		url        string
		fallbackID string
		wantBucket string
		wantKey    string
		wantUpload string
		wantErr    bool
	}{
		{"s3 scheme", "s3://mybucket/path/to/file.mp4", "u-1", "mybucket", "path/to/file.mp4", "u-1", false},
		{"https scheme", "https://s3.us-east-1.amazonaws.com/mybucket/file.mp4", "u-2", "mybucket", "file.mp4", "u-2", false},
		{"http scheme", "http://localhost:9000/mybucket/file.mp4", "u-3", "mybucket", "file.mp4", "u-3", false},
		{"bucket only", "https://host.example.com/mybucket/", "u-4", "mybucket", "", "u-4", false},
		{"unrecognized scheme", "ftp://bucket/key", "u-5", "", "", "", true},
		{"empty s3", "s3://", "u-6", "", "", "", true},
	}
	for _, c := range cases {
		bucket, key, uploadID, err := parseS3URL(c.url, c.fallbackID)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected error, got nil", c.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.name, err)
			continue
		}
		if bucket != c.wantBucket {
			t.Errorf("%s: bucket = %q; want %q", c.name, bucket, c.wantBucket)
		}
		if key != c.wantKey {
			t.Errorf("%s: key = %q; want %q", c.name, key, c.wantKey)
		}
		if uploadID != c.wantUpload {
			t.Errorf("%s: uploadID = %q; want %q", c.name, uploadID, c.wantUpload)
		}
	}
}

// ────────────────────────────────────────────────────────────────────────
// 7. MasterStreamTransport: roundtrip against httptest server.
// ────────────────────────────────────────────────────────────────────────

func TestMasterStreamTransport_Upload_Roundtrip(t *testing.T) {
	// Build a fake master chunked server.
	var (
		mu          sync.Mutex
		chunks      = map[int][]byte{}
		completeSum string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		// /chunked/{job_id}/{idx} or /chunked/{job_id}/complete
		if len(parts) < 3 {
			http.NotFound(w, r)
			return
		}
		switch parts[2] {
		case "complete":
			if r.Method != http.MethodPost {
				http.Error(w, "method", http.StatusBadRequest)
				return
			}
			mu.Lock()
			// Compute SHA-256 of the assembled bytes.
			h := sha256.New()
			for i := 0; ; i++ {
				b, ok := chunks[i]
				if !ok {
					break
				}
				h.Write(b)
			}
			sum := hex.EncodeToString(h.Sum(nil))
			completeSum = sum
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"ok":            true,
				"sha256":        sum,
				"output_sha256": sum,
				"size":          h.Size(),
			})
		default:
			idx, err := func() (int, error) {
				if len(parts) < 3 {
					return 0, fmt.Errorf("bad path")
				}
				return mustAtoi(parts[2])
			}()
			if err != nil {
				http.Error(w, "bad idx", http.StatusBadRequest)
				return
			}
			if r.Method != http.MethodPost {
				http.Error(w, "method", http.StatusBadRequest)
				return
			}
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			chunks[idx] = body
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}))
	defer srv.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	payload := bytes.Repeat([]byte("f"), 5*1024*1024) // 5 MiB (1 chunk)
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	wantSHA := sha256.Sum256(payload)
	wantSHAHex := hex.EncodeToString(wantSHA[:])

	tr := &MasterStreamTransport{HTTPClient: srv.Client()}
	res, err := tr.Upload(context.Background(), UploadRequest{
		LocalPath:    path,
		WorkerSHA256: wantSHAHex,
		Target: UploadTarget{
			UploadID:    "u-master-test",
			TransportID: TransportIDMasterStream,
			UploadURL:   srv.URL + "/chunked/job-test",
		},
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if res.UploadedBytes != int64(len(payload)) {
		t.Errorf("UploadedBytes = %d; want %d", res.UploadedBytes, len(payload))
	}
	if res.ServerSHA256 != wantSHAHex {
		t.Errorf("ServerSHA256 = %q; want %q", res.ServerSHA256, wantSHAHex)
	}
	mu.Lock()
	if completeSum != wantSHAHex {
		t.Errorf("server-computed SHA = %q; want %q", completeSum, wantSHAHex)
	}
	mu.Unlock()
}

func TestMasterStreamTransport_Upload_ServerChecksumMismatch_Errors(t *testing.T) {
	// Worker reports "hello" SHA; the master returns a valid but
	// DIFFERENT 64-char lowercase hex SHA on /complete. The transport
	// MUST surface ErrChecksumMismatch (Verdetto P0 #5: server SHA
	// is authoritative when it returns one).
	wrongSHA := strings.Repeat("d", 64) // lowercase hex, 64 chars, != sha256("hello")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/complete") {
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"ok":     true,
				"sha256": wrongSHA,
			})
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	tr := &MasterStreamTransport{HTTPClient: srv.Client()}
	_, err := tr.Upload(context.Background(), UploadRequest{
		LocalPath:    path,
		WorkerSHA256: sha256Hex([]byte("hello")),
		Target: UploadTarget{
			UploadID:    "u-mismatch",
			TransportID: TransportIDMasterStream,
			UploadURL:   srv.URL + "/chunked/job-mismatch",
		},
	})
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Upload: expected ErrChecksumMismatch, got %v", err)
	}
}

// ────────────────────────────────────────────────────────────────────────
// Helpers.
// ────────────────────────────────────────────────────────────────────────

func mustAtoi(s string) (int, error) {
	var i int
	_, err := fmt.Sscanf(s, "%d", &i)
	return i, err
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// ensure fmt is referenced even if a test is removed during iteration
var _ = fmt.Sprintf
