// Package artifacts / chunked.go — chunked upload service composition root.
//
// The durable chunk upload implementation is split by responsibility across
// chunked_upload.go and chunked_assembly.go. All files remain in package
// artifacts, so the public API and behavior are unchanged.
package artifacts

import (
	"context"
	"fmt"
	"io"
	"sync"

	"velox-server/internal/store"
)

// ChunkedUploadCommand carries the per-chunk metadata for UploadChunk.
type ChunkedUploadCommand struct {
	UploadID   string
	ChunkIndex int
	Reader     io.Reader
}

// ChunkedCompleteCommand carries the auth fields for CompleteChunked.
type ChunkedCompleteCommand struct {
	UploadID         string
	JobID            string
	WorkerID         string
	LeaseID          string
	AttemptNumber    int
	ExpectedRevision int
}

// ChunkState summarises which chunks have been received for resume.
type ChunkState struct {
	TotalChunks int
	Uploaded    []bool
}

// ChunkedUploadService provides persistent chunked upload sessions.
type ChunkedUploadService struct {
	artifactSvc  *Service
	repo         store.UploadRepository
	blobStore    store.BlobStore
	receiveLocks keyedUploadLocks
}

type keyedUploadLocks struct {
	mu    sync.Mutex
	items map[string]*keyedUploadLock
}

type keyedUploadLock struct {
	mu   sync.Mutex
	refs int
}

func (l *keyedUploadLocks) acquire(uploadID string) func() {
	l.mu.Lock()
	if l.items == nil {
		l.items = make(map[string]*keyedUploadLock)
	}
	item := l.items[uploadID]
	if item == nil {
		item = &keyedUploadLock{}
		l.items[uploadID] = item
	}
	item.refs++
	l.mu.Unlock()

	item.mu.Lock()
	return func() {
		item.mu.Unlock()
		l.mu.Lock()
		item.refs--
		if item.refs == 0 {
			delete(l.items, uploadID)
		}
		l.mu.Unlock()
	}
}

// GetUploadByJob returns the active CREATED/UPLOADING upload for a job.
func (s *ChunkedUploadService) GetUploadByJob(ctx context.Context, jobID string) (*store.UploadSession, error) {
	return s.repo.GetActiveUploadByJob(ctx, jobID)
}

// NewChunkedUploadService creates a durable chunked upload service.
func NewChunkedUploadService(artifactSvc *Service, repo store.UploadRepository, blobStore store.BlobStore) *ChunkedUploadService {
	if artifactSvc == nil {
		panic("artifacts: NewChunkedUploadService requires a non-nil artifactSvc")
	}
	if repo == nil {
		panic("artifacts: NewChunkedUploadService requires a non-nil UploadRepository")
	}
	return &ChunkedUploadService{
		artifactSvc: artifactSvc,
		repo:        repo,
		blobStore:   blobStore,
	}
}

// InitChunkedSession creates a chunked upload session via BeginUpload.
func (s *ChunkedUploadService) InitChunkedSession(ctx context.Context, cmd BeginUploadCommand) (*store.UploadSession, error) {
	return s.artifactSvc.BeginUpload(ctx, cmd)
}

// GetUpload returns a durable upload session for the typed master-stream
// protocol. The worker never supplies the session identity as authority.
func (s *ChunkedUploadService) GetUpload(ctx context.Context, uploadID string) (*store.UploadSession, error) {
	if uploadID == "" {
		return nil, fmt.Errorf("artifacts: GetUpload: uploadID required")
	}
	session, err := s.repo.GetUploadSession(ctx, uploadID)
	if err != nil {
		return nil, translateStoreErr(err)
	}
	if session == nil {
		return nil, fmt.Errorf("%w: upload_id=%s", ErrUploadNotFound, uploadID)
	}
	return session, nil
}

// Compile-time check: *ChunkedUploadService is intentionally package-owned.
var _ = (*ChunkedUploadService)(nil)
