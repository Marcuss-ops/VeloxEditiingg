package youtube

import (
	"errors"
	"testing"
)

// storeMock is a minimal YouTubeStore stub exposing only the methods the
// Membership/BulkMembership paths call (GetYouTubeChannel). It does NOT
// satisfy the full YouTubeStore interface — that's intentional; tests for
// other surfaces use the richer test fixtures in sqlite_youtube_entities_test.go.
type membershipStoreMock struct {
	rows map[string]map[string]interface{}
	err  error
}

func (m *membershipStoreMock) GetYouTubeChannel(channelID string) (map[string]interface{}, error) {
	if m.err != nil {
		return nil, m.err
	}
	if row, ok := m.rows[channelID]; ok {
		return row, nil
	}
	return nil, nil
}

// newTestServiceWithStore builds a Service fixture with the supplied store
// for the Membership / BulkMembership paths. Other fields are zero-valued —
// these tests do not need cipher, OAuth, or channels map.
func newTestServiceWithStore(s YouTubeStore) *Service {
	return &Service{store: s}
}

func TestMembership_NoStore(t *testing.T) {
	svc := &Service{store: nil}
	ch, err := svc.Membership("UC_any")
	if err != nil {
		t.Fatalf("Membership with nil store must NOT error; got %v", err)
	}
	if ch != nil {
		t.Fatalf("Membership with nil store must return nil channel; got %+v", ch)
	}
}

func TestMembership_RowMissing(t *testing.T) {
	store := &membershipStoreMock{rows: map[string]map[string]interface{}{}}
	svc := newTestServiceWithStore(store)
	ch, err := svc.Membership("UC_missing")
	if err != nil {
		t.Fatalf("Membership for missing row must NOT error; got %v", err)
	}
	if ch != nil {
		t.Fatalf("Membership for missing row must return nil channel; got %+v", ch)
	}
}

func TestMembership_RowPresent(t *testing.T) {
	store := &membershipStoreMock{
		rows: map[string]map[string]interface{}{
			"UC_present": {
				"channel_id":       "UC_present",
				"title":            "Present Channel",
				"display_name":     "Present Display",
				"channel_url":      "https://youtube.com/@present",
				"thumbnail_url":    "https://example/thumb.jpg",
				"language":         "en",
				"view_count":       int64(1234),
				"subscriber_count": int64(567),
			},
		},
	}
	svc := newTestServiceWithStore(store)
	ch, err := svc.Membership("UC_present")
	if err != nil {
		t.Fatalf("Membership for present row must NOT error; got %v", err)
	}
	if ch == nil {
		t.Fatalf("Membership for present row must return non-nil channel")
	}
	if ch.ID != "UC_present" || ch.Title != "Present Channel" || ch.Language != "en" || ch.SubCount != 567 || ch.ViewCount != 1234 {
		t.Fatalf("Membership did not decode canonical row faithfully: %+v", ch)
	}
}

func TestMembership_StoreErrorSurfaced(t *testing.T) {
	store := &membershipStoreMock{err: errors.New("sqlite: disk full")}
	svc := newTestServiceWithStore(store)
	_, err := svc.Membership("UC_any")
	if err == nil {
		t.Fatalf("Membership MUST surface SQL errors (DB-first invariant); got nil")
	}
	if err.Error() == "" || !contains(err.Error(), "UC_any") {
		t.Fatalf("Membership error must wrap the failing channel id; got %v", err)
	}
}

func TestBulkMembership_EmptyInput(t *testing.T) {
	svc := newTestServiceWithStore(&membershipStoreMock{})
	out, err := svc.BulkMembership(nil)
	if err != nil {
		t.Fatalf("BulkMembership with nil input must NOT error; got %v", err)
	}
	if out != nil {
		t.Fatalf("BulkMembership with nil input must return nil; got %+v", out)
	}
}

func TestBulkMembership_MixedPresence(t *testing.T) {
	store := &membershipStoreMock{
		rows: map[string]map[string]interface{}{
			"UC_a": {"channel_id": "UC_a", "title": "A"},
			"UC_b": {"channel_id": "UC_b", "title": "B"},
		},
	}
	svc := newTestServiceWithStore(store)
	out, err := svc.BulkMembership([]string{"UC_a", "", "UC_b", "UC_missing"})
	if err != nil {
		t.Fatalf("BulkMembership mixed-presence must NOT error; got %v", err)
	}
	if len(out) != 4 {
		t.Fatalf("BulkMembership must preserve input order; got %d entries", len(out))
	}
	if out[0] == nil || out[0].ID != "UC_a" || out[0].Title != "A" {
		t.Fatalf("BulkMembership[0] expected UC_a/A; got %+v", out[0])
	}
	if out[1] != nil {
		t.Fatalf("BulkMembership[1] (empty id) must be nil; got %+v", out[1])
	}
	if out[2] == nil || out[2].ID != "UC_b" || out[2].Title != "B" {
		t.Fatalf("BulkMembership[2] expected UC_b/B; got %+v", out[2])
	}
	if out[3] != nil {
		t.Fatalf("BulkMembership[3] (missing id) must be nil; got %+v", out[3])
	}
}

func TestBulkMembership_StoreErrorPropagates(t *testing.T) {
	store := &membershipStoreMock{err: errors.New("sqlite: I/O error")}
	svc := newTestServiceWithStore(store)
	_, err := svc.BulkMembership([]string{"UC_a", "UC_b"})
	if err == nil {
		t.Fatalf("BulkMembership MUST propagate SQL errors; got nil")
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
