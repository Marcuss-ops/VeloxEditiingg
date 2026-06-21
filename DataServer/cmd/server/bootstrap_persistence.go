package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"velox-server/internal/config"
	"velox-server/internal/outbox"
	"velox-server/internal/platform/database"
	"velox-server/internal/store"
)

// persistenceDeps holds the database, blob-store and outbox store —
// the three infra-level dependencies that everything else builds on.
type persistenceDeps struct {
	Handle    *database.Handle
	SQLite    *store.SQLiteStore
	BlobStore store.BlobStore
	Outbox    *outbox.Store
}

// buildPersistence opens the database, builds the SQLiteStore,
// creates the outbox store, and initialises the filesystem blob store.
//
// BlobStore init is fail-fast: if the filesystem directories cannot
// be created, the entire bootstrap aborts.  There is no fallback to
// a no-op store in production.
//
// NopBlobStore is allowed ONLY when VELOX_ALLOW_NOP_BLOBSTORE_DEV=true
// AND GIN_MODE != "release" AND the environment is not production.
// This is an explicit developer opt-in; the master will log a prominent
// warning.  Any other path that would produce a nil BlobStore is a
// hard error.
func buildPersistence(cfg *config.Config) (*persistenceDeps, error) {
	if cfg == nil {
		cfg = config.FromEnv()
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	openCtx, openCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer openCancel()
	handle, err := database.Open(openCtx, databaseConfigFromConfig(cfg.Database))
	if err != nil {
		return nil, fmt.Errorf("bootstrap: open database: %w", err)
	}

	var sqliteStore *store.SQLiteStore
	switch handle.Driver {
	case database.DriverSQLite:
		sqliteStore, err = store.NewSQLiteStoreFromHandle(handle, cfg.Database.DBPath, cfg.Database.MigrateOnStart)
		if err != nil {
			_ = handle.DB.Close()
			return nil, fmt.Errorf("bootstrap: build SQLite store: %w", err)
		}
		log.Printf("[BOOTSTRAP] sqlite path=%s schema_mode=%s (driver=%s, migrate_on_start=%t)",
			cfg.Database.DBPath, schemaModeLabel(cfg.Database.MigrateOnStart),
			database.Driver(strings.ToLower(strings.TrimSpace(cfg.Database.Driver))),
			cfg.Database.MigrateOnStart)
	case database.DriverPostgres:
		_ = handle.DB.Close()
		return nil, ErrPostgresNotYetWired
	default:
		_ = handle.DB.Close()
		return nil, fmt.Errorf("bootstrap: unsupported driver %q returned by platform/database.Open", handle.Driver)
	}

	outboxStore := outbox.NewStore(sqliteStore.DB())
	// Wire the outbox to the SQLiteStore so transactional callers
	// (jobs writer, artifact writer) can atomically enqueue events.
	// A nil outbox at runtime is a bootstrap bug — emitOutbox fails
	// hard and the caller MUST rollback.
	sqliteStore.SetOutbox(outboxStore)

	var blobStore store.BlobStore
	blobStore, bsErr := store.NewFilesystemBlobStore(cfg.Runtime.StagingDir, cfg.Runtime.StorageDir)
	if bsErr != nil {
		// Check if operator explicitly opted into the dev no-op store.
		// Production ban is enforced by cfg.Validate() — if we reach
		// here with AllowNopBlobStoreDev=true, the env is already
		// confirmed non-production.
		if cfg.Runtime.AllowNopBlobStoreDev {
			log.Printf("[BOOTSTRAP] WARNING: VELOX_ALLOW_NOP_BLOBSTORE_DEV=true — using NopBlobStore (DEVELOPMENT ONLY, not safe for production)")
			blobStore = store.NewNopBlobStore(cfg.Runtime.DataDir)
		} else {
			_ = handle.DB.Close()
			return nil, fmt.Errorf("bootstrap: BlobStore init failed: %w (staging=%s storage=%s) — BlobStore is mandatory in production",
				bsErr, cfg.Runtime.StagingDir, cfg.Runtime.StorageDir)
		}
	}
	log.Printf("[BOOTSTRAP] BlobStore ready: staging=%s storage=%s", blobStore.StagingDir(), blobStore.FinalDir())

	return &persistenceDeps{
		Handle:    handle,
		SQLite:    sqliteStore,
		BlobStore: blobStore,
		Outbox:    outboxStore,
	}, nil
}


