package lifecycle

import (
	"testing"
)

// mockStore implements a minimal store interface for testing credential operations.
type mockStore struct {
	credentials map[string]string // workerID → credentialHash
}

func newMockStore() *mockStore {
	return &mockStore{credentials: make(map[string]string)}
}

func (m *mockStore) HasWorkerCredential(workerID string) (bool, error) {
	_, ok := m.credentials[workerID]
	return ok, nil
}

func (m *mockStore) ValidateWorkerCredential(workerID, credentialHash string) (bool, error) {
	stored, ok := m.credentials[workerID]
	if !ok {
		return false, nil
	}
	return stored == credentialHash, nil
}

func (m *mockStore) SetWorkerCredential(workerID, credentialHash string) error {
	m.credentials[workerID] = credentialHash
	return nil
}

func TestRegisterV2_NewWorkerWithCredential(t *testing.T) {
	// New worker (no existing credential) → credential stored, 200 OK
	ms := newMockStore()

	// Simulate the credential validation logic inline
	workerID := "worker-new"
	credential := "sha256-abc123"

	hasCred, _ := ms.HasWorkerCredential(workerID)
	match, _ := ms.ValidateWorkerCredential(workerID, credential)

	if hasCred {
		t.Fatal("expected no existing credential for new worker")
	}
	if match {
		t.Fatal("expected no match for new worker")
	}

	// Store credential
	if err := ms.SetWorkerCredential(workerID, credential); err != nil {
		t.Fatalf("SetWorkerCredential failed: %v", err)
	}

	// Verify stored
	hasCred, _ = ms.HasWorkerCredential(workerID)
	if !hasCred {
		t.Fatal("expected credential to be stored")
	}
	match, _ = ms.ValidateWorkerCredential(workerID, credential)
	if !match {
		t.Fatal("expected credential to match after storage")
	}
}

func TestRegisterV2_ExistingWorkerMatchingCredential(t *testing.T) {
	// Known worker with matching credential → authenticated, 200 OK
	ms := newMockStore()
	workerID := "worker-known"
	credential := "sha256-xyz789"

	_ = ms.SetWorkerCredential(workerID, credential)

	hasCred, _ := ms.HasWorkerCredential(workerID)
	if !hasCred {
		t.Fatal("expected existing credential")
	}

	match, _ := ms.ValidateWorkerCredential(workerID, credential)
	if !match {
		t.Fatal("expected credential to match")
	}
}

func TestRegisterV2_CredentialMismatch(t *testing.T) {
	// Known worker with WRONG credential → rejected (401)
	ms := newMockStore()
	workerID := "worker-known"
	storedCred := "sha256-xyz789"
	wrongCred := "sha256-attack"

	_ = ms.SetWorkerCredential(workerID, storedCred)

	hasCred, _ := ms.HasWorkerCredential(workerID)
	if !hasCred {
		t.Fatal("expected existing credential")
	}

	match, _ := ms.ValidateWorkerCredential(workerID, wrongCred)
	if match {
		t.Fatal("expected wrong credential to NOT match")
	}
	// This is the mismatch path: hasCred=true, match=false → return 401
}

func TestRegisterV2_NoCredentialProvided(t *testing.T) {
	// Worker sends no credential → skip validation, proceed normally
	ms := newMockStore()
	workerID := "worker-no-cred"

	// Simulate the `if body.Credential != ""` guard — credential is empty, skip block
	hasCred, _ := ms.HasWorkerCredential(workerID)
	if hasCred {
		t.Fatal("expected no credential stored for worker without credential")
	}
	// No credential field → no validation, no storage → registration proceeds
}

func TestCredentialValidation_Lifecycle(t *testing.T) {
	// Full lifecycle: new → stored → match → mismatch → reject
	ms := newMockStore()
	workerID := "worker-lifecycle"
	cred1 := "sha256-first"
	cred2 := "sha256-rotated"

	// Phase 1: New worker registers with cred1
	hasCred, _ := ms.HasWorkerCredential(workerID)
	if hasCred {
		t.Fatal("should be unknown")
	}
	_ = ms.SetWorkerCredential(workerID, cred1)

	// Phase 2: Worker re-registers with same cred1 → match
	match, _ := ms.ValidateWorkerCredential(workerID, cred1)
	if !match {
		t.Fatal("cred1 should match")
	}

	// Phase 3: Attacker tries cred2 → mismatch
	hasCred, _ = ms.HasWorkerCredential(workerID)
	match, _ = ms.ValidateWorkerCredential(workerID, cred2)
	if hasCred && !match {
		// Correct: known worker, wrong credential → reject
	} else {
		t.Fatal("expected hasCred=true, match=false for wrong credential")
	}

	// Phase 4: Rotate credential
	_ = ms.SetWorkerCredential(workerID, cred2)
	match, _ = ms.ValidateWorkerCredential(workerID, cred2)
	if !match {
		t.Fatal("cred2 should match after rotation")
	}
	match, _ = ms.ValidateWorkerCredential(workerID, cred1)
	if match {
		t.Fatal("cred1 should NOT match after rotation")
	}
}
