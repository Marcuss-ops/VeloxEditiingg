package youtube

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// membershipStoreMock is a YouTubeStore stub used only by the Membership /
// BulkMembership tests in this file. It deliberately implements only the
// GetYouTubeChannel method usefully; every other YouTubeStore method is
// stubbed with a zero-returning body so the type SATISFIES the wider
// interface at compile time but does NOT introduce real behaviour the
// test path depends on. If a future test in this file accidentally
// dispatches through one of them, the test will get zero values back
// rather than a panic — both are safe for this narrowly-scoped test.
//
// Wider-surface tests (groups, oauth tokens, API cache) live in
// sqlite_youtube_entities_test.go with a real SQLite fixture; do not
// extend this mock for those paths.
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

// --- Zero-return stubs below. Do not call these from the test bodies. ---

func (m *membershipStoreMock) ListYouTubeChannels() ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *membershipStoreMock) UpsertYouTubeChannel(channelID, title, displayName, channelURL, thumbnailURL, language, notes string, viewCount, subCount int64, addedAt, lastSyncAt string) error {
	return nil
}
func (m *membershipStoreMock) UpdateChannelTitle(channelID, title string) error {
	return nil
}
func (m *membershipStoreMock) UpdateChannelLanguage(channelID, language string) error {
	return nil
}
func (m *membershipStoreMock) UpdateChannelNotes(channelID, notes string) error {
	return nil
}
func (m *membershipStoreMock) UpdateChannelStats(channelID string, viewCount, subCount int64, lastSyncAt string) error {
	return nil
}
func (m *membershipStoreMock) UpdateYouTubeChannelMetadata(channelID, title, thumbnailURL string) error {
	return nil
}
func (m *membershipStoreMock) DeleteYouTubeChannel(channelID string) error {
	return nil
}
func (m *membershipStoreMock) DeleteChannelAtomic(channelID string) (int64, error) {
	return 0, nil
}
func (m *membershipStoreMock) UpsertYouTubeOAuthToken(channelID string, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error {
	return nil
}
func (m *membershipStoreMock) ListActiveYouTubeOAuthTokens() ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *membershipStoreMock) AuditYouTubeOAuthTokenOrphans() (interface{}, error) {
	return nil, nil
}
func (m *membershipStoreMock) ConnectChannelAtomic(channel interface{}, accessTokenEnc, refreshTokenEnc []byte, tokenType, expiry, scopes string, keyVersion int) error {
	return nil
}
func (m *membershipStoreMock) GetYouTubeOAuthToken(channelID string) (map[string]interface{}, error) {
	return nil, nil
}
func (m *membershipStoreMock) MarkYouTubeOAuthTokenRevoked(channelID string) error {
	return nil
}
func (m *membershipStoreMock) ListYouTubeGroupsV2() ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *membershipStoreMock) UpsertYouTubeGroupV2(name, groupType, description, privacy string) (int64, error) {
	return 0, nil
}
func (m *membershipStoreMock) GetYouTubeGroupV2ID(name, groupType string) (int64, error) {
	return 0, nil
}
func (m *membershipStoreMock) DeleteYouTubeGroupV2(id int64) error {
	return nil
}
func (m *membershipStoreMock) AddChannelToGroupV2(groupID int64, channelID string) error {
	return nil
}
func (m *membershipStoreMock) RemoveChannelFromGroupV2(groupID int64, channelID string) error {
	return nil
}
func (m *membershipStoreMock) DeleteYouTubeGroupChannelsByGroupID(groupID int64) error {
	return nil
}
func (m *membershipStoreMock) DeleteYouTubeGroupChannelsByChannelID(channelID string) error {
	return nil
}
func (m *membershipStoreMock) ListGroupChannelsV2(groupID int64) ([]string, error) {
	return nil, nil
}
func (m *membershipStoreMock) ListAllGroupMembershipsV2() ([]map[string]interface{}, error) {
	return nil, nil
}
func (m *membershipStoreMock) GetYouTubeCache(key string) (int64, string, error) {
	return 0, "", nil
}
func (m *membershipStoreMock) SetYouTubeCache(key string, timestamp int64, dataJSON string) error {
	return nil
}
func (m *membershipStoreMock) CleanupYouTubeCache(maxAge int64) (int64, error) {
	return 0, nil
}
func (m *membershipStoreMock) ClearYouTubeCache() error {
	return nil
}
func (m *membershipStoreMock) MigrateYouTubeCache(entries map[string]struct {
	Timestamp int64       `json:"timestamp"`
	Data      interface{} `json:"data"`
}) (int, error) {
	return 0, nil
}

// unused-import guards: the file references `errors`, `strings`, `testing`,
// and `time` only via indirect paths; force-link `io` via a no-op _ = io.EOF
// to keep the import list minimal if a future contributor deletes the
// other imports. (Removed in cleanup below if no longer needed.)

var _ = io.EOF
var _ = time.RFC3339

func newTestServiceWithStore(s YouTubeStore) *Service {
	return &Service{store: s}
}

// --- Tests ---

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
	svc := newTestServiceWithStore(&membershipStoreMock{rows: map[string]map[string]interface{}{}})
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
	svc := newTestServiceWithStore(&membershipStoreMock{err: errors.New("sqlite: disk full")})
	_, err := svc.Membership("UC_any")
	if err == nil {
		t.Fatalf("Membership MUST surface SQL errors (DB-first invariant); got nil")
	}
	if !strings.Contains(err.Error(), "UC_any") {
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
	svc := newTestServiceWithStore(&membershipStoreMock{err: errors.New("sqlite: I/O error")})
	_, err := svc.BulkMembership([]string{"UC_a", "UC_b"})
	if err == nil {
		t.Fatalf("BulkMembership MUST propagate SQL errors; got nil")
	}
}
