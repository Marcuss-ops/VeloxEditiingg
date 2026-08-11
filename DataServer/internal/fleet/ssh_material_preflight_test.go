package fleet

import (
	"os"
	"path/filepath"
	"testing"
)

func TestValidateSSHMaterialAcceptsRuntimeOwned0600Key(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "id_ed25519_velox")
	known := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(key, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(known, []byte("host-key"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSSHMaterial(key, known); err != nil {
		t.Fatalf("ValidateSSHMaterial: %v", err)
	}
}

func TestValidateSSHMaterialRejectsGroupReadablePrivateKey(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "id_ed25519_velox")
	known := filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(key, []byte("private"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(known, []byte("host-key"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSSHMaterial(key, known); err == nil {
		t.Fatal("ValidateSSHMaterial accepted group-readable private key")
	}
}
