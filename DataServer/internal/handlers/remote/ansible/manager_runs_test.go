package ansible

import (
	"errors"
	"testing"

	"velox-server/internal/store"
)

type stubRunStore struct {
	currentHosts []string
	listErr      error
	deleteErr    error
	addErr       error
	upsertCalls  int
}

func (s *stubRunStore) UpsertAnsibleRun(string, string, string, string, int64, int64, int, string, string, string, string, string) error {
	s.upsertCalls++
	return nil
}

func (s *stubRunStore) GetAnsibleRun(string) (*store.AnsibleRun, error) { return nil, nil }

func (s *stubRunStore) ListAnsibleRuns(int) ([]store.AnsibleRun, error) { return nil, nil }

func (s *stubRunStore) DeleteAnsibleRun(string) error { return nil }

func (s *stubRunStore) AddAnsibleRunHost(string, string) error { return s.addErr }

func (s *stubRunStore) DeleteAnsibleRunHost(string, string) error { return s.deleteErr }

func (s *stubRunStore) ListAnsibleRunHosts(string) ([]string, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return append([]string(nil), s.currentHosts...), nil
}

var _ AnsibleRunStore = (*stubRunStore)(nil)

func TestAnsibleRunManager_CreateRunFailsClosedWithoutStore(t *testing.T) {
	manager := NewAnsibleRunManager("", "", nil)
	if err := manager.CreateRun(AnsibleRunRecord{ID: "short"}); !errors.Is(err, ErrRunStoreNotConfigured) {
		t.Fatalf("CreateRun error = %v, want ErrRunStoreNotConfigured", err)
	}
	if err := (*AnsibleRunManager)(nil).CreateRun(AnsibleRunRecord{ID: "short"}); !errors.Is(err, ErrRunStoreNotConfigured) {
		t.Fatalf("nil manager CreateRun error = %v, want ErrRunStoreNotConfigured", err)
	}
}

func TestAnsibleRunManager_CreateRunPropagatesHostDiffErrors(t *testing.T) {
	listErr := errors.New("list unavailable")
	deleteErr := errors.New("delete unavailable")
	addErr := errors.New("add unavailable")
	tests := []struct {
		name       string
		store      *stubRunStore
		run        AnsibleRunRecord
		wantErr    error
		wantUpsert int
	}{
		{
			name:       "list",
			store:      &stubRunStore{listErr: listErr},
			wantErr:    listErr,
			wantUpsert: 1,
		},
		{
			name:       "delete",
			store:      &stubRunStore{currentHosts: []string{"old-host"}, deleteErr: deleteErr},
			wantErr:    deleteErr,
			wantUpsert: 1,
		},
		{
			name:       "add",
			store:      &stubRunStore{addErr: addErr},
			run:        AnsibleRunRecord{ID: "short", Hosts: []string{"new-host"}},
			wantErr:    addErr,
			wantUpsert: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := NewAnsibleRunManager("", "", tt.store)
			err := manager.CreateRun(tt.run)
			if err == nil || !errors.Is(err, tt.wantErr) {
				t.Fatalf("CreateRun error = %v, want wrapped %v", err, tt.wantErr)
			}
			if tt.store.upsertCalls != tt.wantUpsert {
				t.Fatalf("UpsertAnsibleRun calls = %d, want %d", tt.store.upsertCalls, tt.wantUpsert)
			}
		})
	}
}
