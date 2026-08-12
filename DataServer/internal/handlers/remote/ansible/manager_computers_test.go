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

func TestAnsibleComputerManager_MutationsFailClosedWithoutStore(t *testing.T) {
	manager := NewAnsibleComputerManager(t.TempDir(), nil)
	if err := manager.SaveComputer(AnsibleComputer{Host: "worker-a"}); !errors.Is(err, ErrComputerStoreNotConfigured) {
		t.Fatalf("SaveComputer error = %v, want ErrComputerStoreNotConfigured", err)
	}
	if err := manager.DeleteComputer("worker-a"); !errors.Is(err, ErrComputerStoreNotConfigured) {
		t.Fatalf("DeleteComputer error = %v, want ErrComputerStoreNotConfigured", err)
	}
	if err := (*AnsibleComputerManager)(nil).SaveComputer(AnsibleComputer{Host: "worker-a"}); !errors.Is(err, ErrComputerStoreNotConfigured) {
		t.Fatalf("nil manager SaveComputer error = %v, want ErrComputerStoreNotConfigured", err)
	}
}
