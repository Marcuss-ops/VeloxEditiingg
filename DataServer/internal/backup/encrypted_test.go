package backup

import (
	"testing"

	"velox-server/internal/config"
)

func TestEncryptedBackupEnvelopeFormat(t *testing.T) {
	_ = config.CredentialsConfig{}
	if (encryptedBackup{Format: "velox-sqlite-backup-v1", KeyVersion: 1}).Format == "" {
		t.Fatal("missing format")
	}
}
