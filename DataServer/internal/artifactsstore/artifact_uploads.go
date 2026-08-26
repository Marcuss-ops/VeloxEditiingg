// Package artifactsstore owns the SQLite persistence adapters for artifact
// uploads and their resumable chunks.
package artifactsstore

import (
	"database/sql"
	"errors"

	"velox-server/internal/repository"
	"velox-server/internal/storecore"
)

type UploadSession = repository.UploadSession
type UploadFields = repository.UploadFields
type ChunkRecord = repository.ChunkRecord
type UploadStatus = repository.UploadStatus

const (
	UploadCreated    = repository.UploadCreated
	UploadUploading  = repository.UploadUploading
	UploadReceived   = repository.UploadReceived
	UploadVerifying  = repository.UploadVerifying
	UploadFinalizing = repository.UploadFinalizing
	UploadCompleted  = repository.UploadCompleted
	UploadFailed     = repository.UploadFailed
	UploadExpired    = repository.UploadExpired
)

var (
	ErrUploadNotFound     = storecore.ErrUploadNotFound
	ErrUploadStateInvalid = storecore.ErrUploadStateInvalid
	ErrUploadExpired      = errors.New("artifactsstore: upload session expired")
	ErrTransitionConflict = storecore.ErrTransitionConflict
)

type UploadRepository = repository.UploadRepository

type SQLiteUploadRepository struct {
	db *sql.DB
}

func NewSQLiteUploadRepository(db *sql.DB) *SQLiteUploadRepository {
	if db == nil {
		panic("artifactsstore: NewSQLiteUploadRepository requires a non-nil database")
	}
	return &SQLiteUploadRepository{db: db}
}

var _ UploadRepository = (*SQLiteUploadRepository)(nil)
