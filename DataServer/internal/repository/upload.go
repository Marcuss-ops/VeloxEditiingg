package repository

import (
	"context"
	"time"
)

type UploadStatus string

const (
	UploadCreated    UploadStatus = "CREATED"
	UploadUploading  UploadStatus = "UPLOADING"
	UploadReceived   UploadStatus = "RECEIVED"
	UploadVerifying  UploadStatus = "VERIFYING"
	UploadFinalizing UploadStatus = "FINALIZING"
	UploadCompleted  UploadStatus = "COMPLETED"
	UploadFailed     UploadStatus = "FAILED"
	UploadExpired    UploadStatus = "EXPIRED"
)

type UploadSession struct {
	UploadID            string
	ArtifactID          string
	JobID               string
	WorkerID            string
	LeaseID             string
	AttemptNumber       int
	ExpectedRevision    int
	Kind                string
	ExpectedMIME        string
	TemporaryStorageKey string
	ExpectedSizeBytes   int64
	ExpectedSHA256      string
	ReceivedSizeBytes   int64
	ReceivedSHA256      string
	Status              string
	CreatedAt           time.Time
	ExpiresAt           time.Time
	CompletedAt         time.Time
	FirstByteReceivedAt time.Time
	LastByteReceivedAt  time.Time
	VerifyStartedAt     time.Time
	VerifyCompletedAt   time.Time
	PromoteStartedAt    time.Time
	PromoteCompletedAt  time.Time
	CommitStartedAt     time.Time
	CommitCompletedAt   time.Time
}

type UploadFields struct {
	Status              *string
	ReceivedSizeBytes   *int64
	ReceivedSHA256      *string
	CompletedAt         *time.Time
	FirstByteReceivedAt *time.Time
	LastByteReceivedAt  *time.Time
	VerifyStartedAt     *time.Time
	VerifyCompletedAt   *time.Time
	PromoteStartedAt    *time.Time
	PromoteCompletedAt  *time.Time
	CommitStartedAt     *time.Time
	CommitCompletedAt   *time.Time
}

type ChunkRecord struct {
	UploadID   string
	ChunkIndex int
	SizeBytes  int64
	SHA256     string
	StorageKey string
	ReceivedAt time.Time
}

type UploadRepository interface {
	GetUploadSession(ctx context.Context, uploadID string) (*UploadSession, error)
	UpdateUploadStatus(ctx context.Context, uploadID string, fields UploadFields) error
	DeleteUploadSession(ctx context.Context, uploadID string) error
	FindStuckStaging(ctx context.Context, olderThan time.Time, limit int) ([]UploadSession, error)
	TransitionUploadStatus(ctx context.Context, uploadID, from, to string) error
	GetActiveUploadByJob(ctx context.Context, jobID string) (*UploadSession, error)
	InsertChunk(ctx context.Context, c ChunkRecord) error
	GetChunk(ctx context.Context, uploadID string, chunkIndex int) (*ChunkRecord, error)
	ListChunks(ctx context.Context, uploadID string) ([]ChunkRecord, error)
	DeleteChunks(ctx context.Context, uploadID string) error
}
