package credentials

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type memoryRepository struct {
	records map[string]StoredCredential
	uses    []UsageEvent
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{records: map[string]StoredCredential{}}
}
func (m *memoryRepository) PutCredential(_ context.Context, record StoredCredential) error {
	m.records[record.Ref] = record
	return nil
}
func (m *memoryRepository) GetCredential(_ context.Context, ref string) (*StoredCredential, error) {
	record, ok := m.records[ref]
	if !ok {
		return nil, ErrNotFound
	}
	copy := record
	return &copy, nil
}
func (m *memoryRepository) UpdateCredential(_ context.Context, record StoredCredential) error {
	m.records[record.Ref] = record
	return nil
}
func (m *memoryRepository) RevokeCredential(_ context.Context, ref string, at time.Time) error {
	record, ok := m.records[ref]
	if !ok {
		return ErrNotFound
	}
	record.RevokedAt = &at
	m.records[ref] = record
	return nil
}
func (m *memoryRepository) RecordCredentialUse(_ context.Context, _ string, event UsageEvent) error {
	m.uses = append(m.uses, event)
	return nil
}

func testVault(t *testing.T) (*Vault, *memoryRepository) {
	t.Helper()
	repo := newMemoryRepository()
	keys, err := NewKeyring(1, map[int][]byte{1: []byte("01234567890123456789012345678901")})
	if err != nil {
		t.Fatal(err)
	}
	vault, err := NewVault(repo, keys)
	if err != nil {
		t.Fatal(err)
	}
	return vault, repo
}

func TestVaultOpaqueEncryptedScopedLease(t *testing.T) {
	vault, repo := testVault(t)
	ref, err := vault.Put(context.Background(), "youtube", "channel-1", []string{"upload", "metadata"}, time.Now().Add(time.Hour), time.Now().Add(24*time.Hour), Material{AccessToken: "access-secret", RefreshToken: "refresh-secret", ClientSecret: "client-secret"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ref, "cred_") || len(ref) != 41 {
		t.Fatalf("credential ref = %q", ref)
	}
	stored := repo.records[ref]
	if strings.Contains(string(stored.Ciphertext), "access-secret") || strings.Contains(string(stored.Ciphertext), "refresh-secret") {
		t.Fatal("ciphertext contains plaintext secret")
	}
	lease, err := vault.IssueAccessLease(context.Background(), ref, "worker-1", "publication-1", []string{"upload"})
	if err != nil {
		t.Fatal(err)
	}
	if lease.AccessToken != "access-secret" || lease.WorkerID != "worker-1" || len(repo.uses) != 1 {
		t.Fatalf("lease/use = %#v %#v", lease, repo.uses)
	}
	if _, err := vault.IssueAccessLease(context.Background(), ref, "worker-1", "publication-1", []string{"admin"}); !errors.Is(err, ErrScope) {
		t.Fatalf("scope error = %v", err)
	}
}

func TestVaultRevocationRotationAndRedaction(t *testing.T) {
	vault, _ := testVault(t)
	ref, err := vault.Put(context.Background(), "drive", "owner", []string{"files"}, time.Now().Add(time.Hour), time.Time{}, Material{AccessToken: "old"})
	if err != nil {
		t.Fatal(err)
	}
	if err := vault.Rotate(context.Background(), ref, Material{AccessToken: "new"}, time.Now().Add(time.Hour), time.Time{}); err != nil {
		t.Fatal(err)
	}
	lease, err := vault.IssueAccessLease(context.Background(), ref, "w", "p", []string{"files"})
	if err != nil || lease.AccessToken != "new" {
		t.Fatalf("rotated lease = %#v err=%v", lease, err)
	}
	if err := vault.Revoke(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
	if _, err := vault.IssueAccessLease(context.Background(), ref, "w", "p", []string{"files"}); !errors.Is(err, ErrRevoked) {
		t.Fatalf("revocation error = %v", err)
	}
	raw := `{"access_token":"a","nested":{"authorization":"Bearer b"},"url":"https://x/?signature=secret"}`
	redacted := JSON(raw)
	if strings.Contains(redacted, `"a"`) || strings.Contains(redacted, "Bearer b") || strings.Contains(redacted, "secret") {
		t.Fatalf("redaction failed: %s", redacted)
	}
}
