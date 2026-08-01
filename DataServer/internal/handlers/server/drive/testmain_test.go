package drive

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	if os.Getenv("VELOX_CREDENTIAL_KEY") == "" && os.Getenv("VELOX_CREDENTIAL_KEY_FILE") == "" {
		_ = os.Setenv("VELOX_CREDENTIAL_KEY", "01234567890123456789012345678901")
	}
	os.Exit(m.Run())
}
