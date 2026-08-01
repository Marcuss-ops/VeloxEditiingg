package backup

import "testing"

func TestEncryptedBackupEnvelopeFormat(t *testing.T) {
	if (encryptedBackup{Format: "velox-sqlite-backup-v1", KeyVersion: 1}).Format == "" {
		t.Fatal("missing format")
	}
}
