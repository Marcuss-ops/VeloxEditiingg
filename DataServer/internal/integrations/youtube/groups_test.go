package youtube

import (
	"testing"
)

// helper: create a minimal Service with in-memory channels and groups.
func mockService(t *testing.T) *Service {
	t.Helper()
	return &Service{
		channels: make(map[string]*AuthChannel),
		groups:   make(map[string]*ChannelGroup),
	}
}

// helper: add a channel to the service.
func addMockChannel(t *testing.T, s *Service, id, name, language string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[id] = &AuthChannel{
		ID:       id,
		Name:     name,
		Title:    name + " Title",
		Language: language,
	}
}

// helper: add a group with channel IDs to the service.
func addMockGroup(t *testing.T, s *Service, name string, channelIDs []string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups[name] = &ChannelGroup{
		Name:     name,
		Channels: channelIDs,
	}
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

	// Request with lowercase
	ch, err := s.ResolveChannelByLanguage("Amish", "it")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage (lower request): %v", err)
	}
	if ch.ID != "ch_it_1" {
		t.Fatalf("want channel ch_it_1, got %s", ch.ID)
	}

	// Request with uppercase
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

	// Request a language that doesn't exist in the group (e.g. "fr")
	ch, err := s.ResolveChannelByLanguage("Amish", "fr")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage: %v", err)
	}
	// Should fallback to the unconfigured channel
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

	// Request a language that doesn't exist — fallback to first channel
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
	// Group references channel IDs that don't exist in the channels map
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

	// Modify the returned copy — should NOT affect the original
	ch.Name = "Modified"

	// Check the original is unchanged
	s.mu.RLock()
	original := s.channels["ch_it_1"]
	s.mu.RUnlock()
	if original.Name != "Canale Italiano" {
		t.Fatalf("expected original channel name unchanged, got %q", original.Name)
	}
}

func TestResolveChannelByLanguage_PrefersExactMatchOverFallback(t *testing.T) {
	s := mockService(t)
	addMockChannel(t, s, "ch_fallback", "Fallback Channel", "") // no language
	addMockChannel(t, s, "ch_it_1", "Canale Italiano", "it")
	addMockGroup(t, s, "Amish", []string{"ch_fallback", "ch_it_1"})

	// Should prefer the Italian channel over the fallback
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

	// The group name lookup is exact, so whitespace in the group name is preserved.
	// This test verifies the function doesn't crash with whitespace.
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

	// Request English — should return first matching English channel
	ch, err := s.ResolveChannelByLanguage("Amish", "en")
	if err != nil {
		t.Fatalf("ResolveChannelByLanguage: %v", err)
	}
	if ch.ID != "ch_en_1" {
		t.Fatalf("want first English channel ch_en_1, got %s", ch.ID)
	}
}
