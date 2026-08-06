package backup

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"velox-server/internal/config"
	"velox-server/internal/credentials"
)

type encryptedBackup struct {
	Format     string `json:"format"`
	KeyVersion int    `json:"key_version"`
	SHA256     string `json:"sha256"`
	Ciphertext []byte `json:"ciphertext"`
}

// BackupEncryptedSQLite creates a consistent SQLite snapshot and stores only
// an encrypted, integrity-bound envelope at destination.
func BackupEncryptedSQLite(ctx context.Context, db *sql.DB, destination string, credentialConfig config.CredentialsConfig) (Verification, error) {
	keyring, err := credentials.LoadKeyring(credentialConfig)
	if err != nil {
		return Verification{}, err
	}
	tmp, err := os.CreateTemp("", "velox-backup-*.sqlite")
	if err != nil {
		return Verification{}, err
	}
	tmpPath := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(tmpPath)
	verification, err := BackupSQLite(ctx, db, tmpPath)
	if err != nil {
		return Verification{}, err
	}
	plain, err := os.ReadFile(tmpPath)
	if err != nil {
		return Verification{}, err
	}
	ciphertext, version, err := keyring.Seal(plain)
	if err != nil {
		return Verification{}, err
	}
	digest := sha256.Sum256(plain)
	envelope, err := json.Marshal(encryptedBackup{Format: "velox-sqlite-backup-v1", KeyVersion: version, SHA256: hex.EncodeToString(digest[:]), Ciphertext: ciphertext})
	if err != nil {
		return Verification{}, err
	}
	if err := os.WriteFile(destination, envelope, 0600); err != nil {
		return Verification{}, fmt.Errorf("backup: write encrypted backup: %w", err)
	}
	return verification, nil
}

// RestoreEncryptedSQLite decrypts an envelope into a temporary isolated copy
// and delegates to the same verified atomic restore path as plain snapshots.
func RestoreEncryptedSQLite(ctx context.Context, source, destination string, credentialConfig config.CredentialsConfig) (Verification, error) {
	keyring, err := credentials.LoadKeyring(credentialConfig)
	if err != nil {
		return Verification{}, err
	}
	envelopeBytes, err := os.ReadFile(source)
	if err != nil {
		return Verification{}, err
	}
	var envelope encryptedBackup
	if err := json.Unmarshal(envelopeBytes, &envelope); err != nil || envelope.Format != "velox-sqlite-backup-v1" {
		return Verification{}, fmt.Errorf("backup: invalid encrypted envelope")
	}
	plain, err := keyring.Open(envelope.KeyVersion, envelope.Ciphertext)
	if err != nil {
		return Verification{}, err
	}
	digest := sha256.Sum256(plain)
	if hex.EncodeToString(digest[:]) != envelope.SHA256 {
		return Verification{}, ErrIntegrity
	}
	tmp, err := os.CreateTemp("", "velox-restore-*.sqlite")
	if err != nil {
		return Verification{}, err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(plain); err != nil {
		_ = tmp.Close()
		return Verification{}, err
	}
	if err := tmp.Close(); err != nil {
		return Verification{}, err
	}
	return RestoreSQLite(ctx, tmpPath, destination)
}

// RestoreTest performs the periodic isolated restore check required by the
// operational runbook and rejects snapshots without the migration ledger or
// core tables.
func RestoreTest(ctx context.Context, source, isolatedDestination string) (Verification, error) {
	verification, err := RestoreSQLite(ctx, source, isolatedDestination)
	if err != nil {
		return Verification{}, err
	}
	if verification.SchemaMigrationCount == 0 {
		return Verification{}, ErrSchemaMissing
	}
	for _, table := range coreTables {
		if _, ok := verification.TableCounts[table]; !ok {
			return Verification{}, fmt.Errorf("backup: restore test missing table %s", table)
		}
	}
	return verification, nil
}
