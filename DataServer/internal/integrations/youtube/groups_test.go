package youtube

import (
	"testing"

	"velox-server/internal/store/youtubetypes"
)

// fakeStoreForGroups is a minimal fake YouTubeStore used by group tests.
// It stores channels, groups, and memberships in memory so that
// Service.loadAuthChannel / loadChannelGroup can hydrate from it.
type fakeStoreForGroups struct {
	YouTubeStore

	channels    map[string]*youtubetypes.YouTubeChannel
	oauthTokens map[string]*youtubetypes.YouTubeOAuthToken
	groups      []youtubetypes.YouTubeGroup
	memberships map[int64][]string
}

func (f *fakeStoreForGroups) GetYouTubeChannel(channelID string) (*youtubetypes.YouTubeChannel, error) {
	return f.channels[channelID], nil
}

func (f *fakeStoreForGroups) GetYouTubeOAuthToken(channelID string) (*youtubetypes.YouTubeOAuthToken, error) {
	return f.oauthTokens[channelID], nil
}

func (f *fakeStoreForGroups) ListYouTubeGroups() ([]youtubetypes.YouTubeGroup, error) {
	return f.groups, nil
}

func (f *fakeStoreForGroups) ListGroupChannels(groupID int64) ([]string, error) {
	return f.memberships[groupID], nil
}

// helper: create a minimal Service backed by the fake store.
func mockService(t *testing.T) *Service {
	t.Helper()
	return &Service{
		repo: &fakeStoreForGroups{
			channels:    make(map[string]*youtubetypes.YouTubeChannel),
			oauthTokens: make(map[string]*youtubetypes.YouTubeOAuthToken),
			memberships: make(map[int64][]string),
		},
	}
}

// helper: add a channel to the fake store.
func addMockChannel(t *testing.T, s *Service, id, name, language string) {
	t.Helper()
	fs := s.repo.(*fakeStoreForGroups)
	fs.channels[id] = &youtubetypes.YouTubeChannel{
		ChannelID:   id,
		DisplayName: name,
		Title:       name + " Title",
		Language:    language,
	}
	fs.oauthTokens[id] = &youtubetypes.YouTubeOAuthToken{
		ChannelID: id,
	}
}

// helper: add a group with channel IDs to the fake store.
func addMockGroup(t *testing.T, s *Service, name string, channelIDs []string) {
	t.Helper()
	fs := s.repo.(*fakeStoreForGroups)
	gid := int64(len(fs.groups) + 1)
	fs.groups = append(fs.groups, youtubetypes.YouTubeGroup{
		ID:        gid,
		Name:      name,
		GroupType: "upload",
	})
	fs.memberships[gid] = append([]string{}, channelIDs...)
}

// =============================================================================
// ResolveChannelByLanguage tests
// =============================================================================

func TestResolveChannelByLanguage_ExactMatch(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "it")
	addMockChannel(t, s, "ch_en_1", "English Channel", "en")
	addMockChannel(t, s, "ch_fr_1", "Chaîne Française", "fr")
	addMockGroup(t, s, "Amish", []string{"ch_it_1", "ch_en_1", "ch_fr_1"})

	ch, err := s.ResolveChannelByLanguage("Amish", "it")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage: %v", err)
	}
	if ch.ID != "ch_it_1" {
		t.Fatalf("want channel ch_it_1, got %s", ch.ID)
	}
	if ch.Language != "it" {
		t.Fatalf("want language 'it', got %q", ch.Language)
	}
	if ch.Name != "Canale Italiano" {
		t.Fatalf("want name 'Canale Italiano', got %q", ch.Name)
	}
}

func TestResolveChannelByLanguage_EnglishMatch(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "it")
	addMockChannel(t, s, "ch_en_1", "English Channel", "en")
	addMockGroup(t, s, "Amish", []string{"ch_it_1", "ch_en_1"})

	ch, err := s.ResolveChannelByLanguage("Amish", "en")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage: %v", err)
	}
	if ch.ID != "ch_en_1" {
		t.Fatalf("want channel ch_en_1, got %s", ch.ID)
	}
}

func TestResolveChannelByLanguage_CaseInsensitive(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "IT")
	addMockChannel(t, s, "ch_en_1", "English Channel", "EN")
	addMockGroup(t, s, "Amish", []string{"ch_it_1", "ch_en_1"})

	ch, err := s.ResolveChannelByLanguage("Amish", "it")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage (lower request): %v", err)
	}
	if ch.ID != "ch_it_1" {
		t.Fatalf("want channel ch_it_1, got %s", ch.ID)
	}

	ch, err = s.ResolveChannelByLanguage("Amish", "IT")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage (upper request): %v", err)
	}
	if ch.ID != "ch_it_1" {
		t.Fatalf("want channel ch_it_1, got %s", ch.ID)
	}
}

func TestResolveChannelByLanguage_NoMatchThenFallbackToUnconfigured(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "it")
	addMockChannel(t, s, "ch_unconf", "Unconfigured Channel", "") // no language
	addMockGroup(t, s, "Amish", []string{"ch_it_1", "ch_unconf"})

	ch, err := s.ResolveChannelByLanguage("Amish", "fr")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage: %v", err)
	}
	if ch.ID != "ch_unconf" {
		t.Fatalf("want fallback to unconfigured channel ch_unconf, got %s", ch.ID)
	}
	if ch.Language != "" {
		t.Fatalf("want empty language on fallback channel, got %q", ch.Language)
	}
}

func TestResolveChannelByLanguage_NoMatchThenFallbackToAny(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "it")
	addMockChannel(t, s, "ch_en_1", "English Channel", "en")
	addMockGroup(t, s, "Amish", []string{"ch_it_1", "ch_en_1"})

	ch, err := s.ResolveChannelByLanguage("Amish", "de")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage: %v", err)
	}
	if ch.ID != "ch_it_1" {
		t.Fatalf("want fallback to first channel ch_it_1, got %s", ch.ID)
	}
}

func TestResolveChannelByLanguage_GroupNotFound(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "it")

	_, err := s.ResolveChannelByLanguage("NonExistentGroup", "it")
	if err == nil {
		t.Fatal("want error for non-existent group, got nil")
	}
}

func TestResolveChannelByLanguage_EmptyGroupName(t *testing.T) {
	s := mockService(t)
	_, err := s.ResolveChannelByLanguage("", "it")
	if err == nil {
		t.Fatal("want error for empty group name, got nil")
	}
}

func TestResolveChannelByLanguage_EmptyLanguage(t *testing.T) {
	s := mockService(t)
	_, err := s.ResolveChannelByLanguage("Amish", "")
	if err == nil {
		t.Fatal("want error for empty language, got nil")
	}
}

func TestResolveChannelByLanguage_GroupHasNoChannels(t *testing.T) {
	s := mockService(t)
	addMockGroup(t, s, "EmptyGroup", []string{})

	_, err := s.ResolveChannelByLanguage("EmptyGroup", "it")
	if err == nil {
		t.Fatal("want error for group with no channels, got nil")
	}
}

func TestResolveChannelByLanguage_ChannelIDsNotInChannelsMap(t *testing.T) {
	s := mockService(t)
	addMockGroup(t, s, "OrphanGroup", []string{"ghost_ch_1", "ghost_ch_2"})

	_, err := s.ResolveChannelByLanguage("OrphanGroup", "it")
	if err == nil {
		t.Fatal("want error for group with no resolvable channels, got nil")
	}
}

func TestResolveChannelByLanguage_ReturnsDeepCopy(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "it")
	addMockGroup(t, s, "Amish", []string{"ch_it_1"})

	ch, err := s.ResolveChannelByLanguage("Amish", "it")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage: %v", err)
	}

	// Modify the returned copy — should NOT affect the store data
	ch.Name = "Modified"

	reloaded := s.GetAuthChannel("ch_it_1")
	if reloaded == nil {
		t.Fatal("expected channel to be reloadable")
	}
	if reloaded.Name != "Canale Italiano" {
		t.Fatalf("expected original channel name unchanged, got %q", reloaded.Name)
	}
}

func TestResolveChannelByLanguage_PrefersExactMatchOverFallback(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_fallback", "Fallback Channel", "") // no language
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "it")
	addMockGroup(t, s, "Amish", []string{"ch_fallback", "ch_it_1"})

	ch, err := s.ResolveChannelByLanguage("Amish", "it")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage: %v", err)
	}
	if ch.ID != "ch_it_1" {
		t.Fatalf("want exact match ch_it_1, got %s", ch.ID)
	}
}

func TestResolveChannelByLanguage_WithWhitespaceInGroupName(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "it")
	addMockGroup(t, s, "  Amish Group  ", []string{"ch_it_1"})

	ch, err := s.ResolveChannelByLanguage("  Amish Group  ", "it")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage with whitespace: %v", err)
	}
	if ch.ID != "ch_it_1" {
		t.Fatalf("want ch_it_1, got %s", ch.ID)
	}
}

func TestResolveChannelByLanguage_MultipleLanguageChannels(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_en_1", "English 1", "en")
	addMockChannel(t, s, "ch_en_2", "English 2", "en")
	addMockChannel(t, s, "ch_it_1", "Italiano", "it")
	addMockGroup(t, s, "Amish", []string{"ch_en_1", "ch_en_2", "ch_it_1"})

	ch, err := s.ResolveChannelByLanguage("Amish", "en")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage: %v", err)
	}
	if ch.ID != "ch_en_1" {
		t.Fatalf("want first English channel ch_en_1, got %s", ch.ID)
	}
}
