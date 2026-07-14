package publisher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"
)

// S3ClientIface is the minimal S3-compatible multipart API used by the
// transport. Tests can provide a fake without importing the full AWS SDK.
type S3ClientIface interface {
	CreateMultipartUpload(ctx context.Context, params interface{}) (multipartCreateResult, error)
	UploadPart(ctx context.Context, params interface{}) (multipartUploadResult, error)
	CompleteMultipartUpload(ctx context.Context, params interface{}) (multipartCompleteResult, error)
	AbortMultipartUpload(ctx context.Context, params interface{}) error
	ListParts(ctx context.Context, params interface{}) (multipartListResult, error)
}

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

// ObjectStoreMultipartTransport is the production S3-compatible multipart
// upload transport.
type ObjectStoreMultipartTransport struct {
	// S3Client is supplied by production wiring or a test fake.
	S3Client S3ClientIface

	// MaxRetries is the per-chunk retry budget. The default is five.
	MaxRetries int
	// ChunkSize is the per-part byte size. The default is 8 MiB.
	ChunkSize int64
}

func (t *ObjectStoreMultipartTransport) ID() string { return TransportIDObjectStoreMultipart }

// Upload implements Transport.Upload for ObjectStoreMultipartTransport.
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

	createRes, err := t.S3Client.CreateMultipartUpload(ctx, map[string]interface{}{
		"bucket":          bucket,
		"key":             key,
		"upload_id":       s3UploadID,
		"checksum_sha256": req.WorkerSHA256,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: CreateMultipartUpload: %v", ErrUploadFailed, err)
	}
	if createRes.UploadID != "" {
		s3UploadID = createRes.UploadID
	}

	existing, err := t.S3Client.ListParts(ctx, map[string]interface{}{
		"bucket":    bucket,
		"key":       key,
		"upload_id": s3UploadID,
	})
	if err != nil {
		existing = multipartListResult{}
	}
	present := make(map[int]s3PartSummary, len(existing.Parts))
	for _, p := range existing.Parts {
		present[p.PartNumber] = p
	}

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

	// An S3 multipart ETag is not the SHA-256 of the assembled object. Keep
	// ServerSHA256 empty until the master derives authoritative evidence.
	_ = compRes

	return &UploadResult{
		UploadID:      req.Target.UploadID,
		UploadedBytes: uploaded,
		ServerSHA256:  "",
	}, nil
}

// uploadPartWithRetry retries transient per-part failures with exponential
// backoff.
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
