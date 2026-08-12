package ansible

import (
	"errors"
	"testing"

	"velox-server/internal/store"
)

type nilComputerStore struct{}

func (nilComputerStore) UpsertAnsibleHost(store.AnsibleHostFields) error { return nil }
func (nilComputerStore) DeleteAnsibleHost(string) error                  { return nil }
func (nilComputerStore) GetAnsibleHost(string) (*store.AnsibleHostFields, error) {
	return nil, nil
}
func (nilComputerStore) ListAnsibleHosts() ([]store.AnsibleHostFields, error) { return nil, nil }
func (nilComputerStore) CountAnsibleHosts() (int, error)                      { return 0, nil }
func (nilComputerStore) CountAnsibleHostsEnabled() (int, error)               { return 0, nil }

var _ AnsibleComputerStore = nilComputerStore{}

type failingComputerStore struct{ err error }

func (s failingComputerStore) UpsertAnsibleHost(store.AnsibleHostFields) error { return s.err }
func (s failingComputerStore) DeleteAnsibleHost(string) error                  { return s.err }
func (s failingComputerStore) GetAnsibleHost(string) (*store.AnsibleHostFields, error) {
	return nil, s.err
}
func (s failingComputerStore) ListAnsibleHosts() ([]store.AnsibleHostFields, error) {
	return nil, s.err
}
func (s failingComputerStore) CountAnsibleHosts() (int, error)        { return 0, s.err }
func (s failingComputerStore) CountAnsibleHostsEnabled() (int, error) { return 0, s.err }

func TestAnsibleComputerManager_MutationsFailClosedWithoutStore(t *testing.T) {
	manager, err := NewAnsibleComputerManager(t.TempDir(), nil)
	if manager != nil || !errors.Is(err, ErrComputerStoreNotConfigured) {
		t.Fatalf("NewAnsibleComputerManager() = (%v, %v), want nil and ErrComputerStoreNotConfigured", manager, err)
	}
	if err := (*AnsibleComputerManager)(nil).SaveComputer(AnsibleComputer{Host: "worker-a"}); !errors.Is(err, ErrComputerStoreNotConfigured) {
		t.Fatalf("nil manager SaveComputer error = %v, want ErrComputerStoreNotConfigured", err)
	}
}

func TestAnsibleComputerManager_ReadsFailClosedOnStoreErrors(t *testing.T) {
	storeErr := errors.New("inventory unavailable")
	manager, err := NewAnsibleComputerManager(t.TempDir(), failingComputerStore{err: storeErr})
	if err != nil {
		t.Fatalf("NewAnsibleComputerManager() error = %v", err)
	}
	if computers, err := manager.ListComputers(); err == nil || !errors.Is(err, storeErr) || computers != nil {
		t.Fatalf("ListComputers() = (%v, %v), want nil and wrapped store error", computers, err)
	}
	if _, _, err := manager.GetComputer("worker-a"); err == nil || !errors.Is(err, storeErr) {
		t.Fatalf("GetComputer() error = %v, want wrapped store error", err)
	}
	if _, err := manager.Count(); err == nil || !errors.Is(err, storeErr) {
		t.Fatalf("Count() error = %v, want wrapped store error", err)
	}
	if _, err := manager.CountEnabled(); err == nil || !errors.Is(err, storeErr) {
		t.Fatalf("CountEnabled() error = %v, want wrapped store error", err)
	}
}
