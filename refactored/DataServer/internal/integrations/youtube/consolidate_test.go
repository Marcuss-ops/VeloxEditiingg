package youtube

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeLegacyFile writes a token-file at <dataDir>/youtube/Token or
// <dataDir>/youtube/group/<group>/Token depending on `p`. The JSON
// body may either contain an explicit channel_id or rely on the
// filename for channel_id derivation (for the bare Token case).
// Returns the absolute path written.
func writeLegacyFile(t *testing.T, dataDir string, p, channelID string, body string) string {
	t.Helper()
	abs := filepath.Join(dataDir, p)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(abs), err)
	}
	if err := os.WriteFile(abs, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile(%s): %v", abs, err)
	}
	// Sanity: archived channelID for caller-side asserts.
	_ = channelID
	return abs
}

// writeCanonicalCopy primes the canonical account_<channel>.json
// content so a merge test can drive a byte-identical match.
func writeCanonicalCopy(t *testing.T, dataDir, channelID, body string) string {
	t.Helper()
	canonicalDir := filepath.Join(dataDir, CanonicalOAuthTokenSubPath)
	if err := os.MkdirAll(canonicalDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(canonical): %v", err)
	}
	abs := filepath.Join(canonicalDir, "account_"+channelID+".json")
	if err := os.WriteFile(abs, []byte(body), 0o600); err != nil {
		t.Fatalf("WriteFile(canonical): %v", err)
	}
	return abs
}

func TestConsolidateOAuthTokens_EmptyDataDir(t *testing.T) {
	res, err := ConsolidateOAuthTokens("", false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Found != 0 || res.Moved != 0 || res.Merged != 0 ||
		res.DeletedLegacyFiles != 0 || res.RemovedEmptyDirs != 0 ||
		len(res.Errors) != 0 {
		t.Errorf("expected zero counters, got %+v", res)
	}
}

func TestConsolidateOAuthTokens_NoYoutubeDir(t *testing.T) {
	dir := t.TempDir()
	// No youtube/ subdir at all.
	res, err := ConsolidateOAuthTokens(dir, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Found != 0 {
		t.Errorf("Found=%d, want 0", res.Found)
	}
}

func TestConsolidateOAuthTokens_SingleLegacyFile_ByJSONChannelID(t *testing.T) {
	dir := t.TempDir()
	body := `{"token":"a","refresh_token":"r","channel_id":"UC_json_chid"}`
	writeLegacyFile(t, dir, "youtube/Token", "UC_json_chid", body)

	res, err := ConsolidateOAuthTokens(dir, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Found != 1 || res.Moved != 1 || res.Merged != 0 ||
		res.DeletedLegacyFiles != 0 || len(res.Errors) != 0 {
		t.Errorf("counters wrong: %+v", res)
	}
	dest := filepath.Join(dir, CanonicalOAuthTokenSubPath, "account_UC_json_chid.json")
	if _, err := os.Stat(dest); err != nil {
		t.Errorf("canonical file not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "youtube", "Token")); !os.IsNotExist(err) {
		t.Errorf("legacy file should have been moved; stat err=%v", err)
	}
}

func TestConsolidateOAuthTokens_SingleLegacyFile_ByFilename(t *testing.T) {
	dir := t.TempDir()
	body := `{"token":"a","refresh_token":"r"}` // no channel_id in JSON
	// Filename is `account_<channel>.json` so channel_id resolves via
	// filename.
	writeLegacyFile(t, dir, "youtube/account_UC_name_chid.json", "UC_name_chid", body)

	res, err := ConsolidateOAuthTokens(dir, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Found != 1 || res.Moved != 1 || len(res.Errors) != 0 {
		t.Errorf("counters wrong: %+v", res)
	}
	dest := filepath.Join(dir, CanonicalOAuthTokenSubPath, "account_UC_name_chid.json")
	if _, err := os.Stat(dest); err != nil {
		t.Errorf("canonical file not created: %v", err)
	}
}

func TestConsolidateOAuthTokens_MergeIdenticalCanonical(t *testing.T) {
	dir := t.TempDir()
	body := `{"token":"a","refresh_token":"r","channel_id":"UC_merge"}`
	// Legacy AND canonical exist with the same channel_id AND the
	// same body bytes. The function must treat this as a merge:
	// Merged=1, Moved=0, DeletedLegacyFiles=1, the legacy file is
	// removed, the canonical keeps its content.
	writeLegacyFile(t, dir, "youtube/Token", "UC_merge", body)
	writeCanonicalCopy(t, dir, "UC_merge", body)

	res, err := ConsolidateOAuthTokens(dir, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Found != 1 {
		t.Errorf("Found=%d, want 1", res.Found)
	}
	if res.Merged != 1 || res.Moved != 0 || res.DeletedLegacyFiles != 1 {
		t.Errorf("counters wrong (want Merged=1, DeletedLegacyFiles=1, Moved=0): %+v", res)
	}
	if len(res.Errors) != 0 {
		t.Errorf("Errors=%v, want none for a same-content merge", res.Errors)
	}
	if _, err := os.Stat(filepath.Join(dir, "youtube", "Token")); !os.IsNotExist(err) {
		t.Errorf("legacy should be removed on merge; stat err=%v", err)
	}
}

func TestConsolidateOAuthTokens_MergeContentDiffers_Skipped(t *testing.T) {
	dir := t.TempDir()
	legacyBody := `{"token":"fresh_access","refresh_token":"r","channel_id":"UC_conflict"}`
	canonicalBody := `{"token":"older_access","refresh_token":"r","channel_id":"UC_conflict"}`
	writeLegacyFile(t, dir, "youtube/Token", "UC_conflict", legacyBody)
	writeCanonicalCopy(t, dir, "UC_conflict", canonicalBody)

	res, err := ConsolidateOAuthTokens(dir, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Merged != 0 || res.Moved != 0 {
		t.Errorf("counters wrong on conflict (want no Moved/Merged): %+v", res)
	}
	if len(res.Errors) == 0 {
		t.Errorf("expected a per-file Error on conflict; got none")
	}
	// Both files must still exist on disk.
	if _, err := os.Stat(filepath.Join(dir, "youtube", "Token")); err != nil {
		t.Errorf("legacy must be preserved on conflict: %v", err)
	}
	destPath := filepath.Join(dir, CanonicalOAuthTokenSubPath, "account_UC_conflict.json")
	got, rerr := os.ReadFile(destPath)
	if rerr != nil {
		t.Fatalf("read canonical: %v", rerr)
	}
	if !strings.Contains(string(got), "older_access") {
		t.Errorf("canonical was overwritten; got=%s", string(got))
	}
}

func TestConsolidateOAuthTokens_DryRun(t *testing.T) {
	dir := t.TempDir()
	body := `{"token":"a","refresh_token":"r","channel_id":"UC_dry"}`
	writeLegacyFile(t, dir, "youtube/Token", "UC_dry", body)

	res, err := ConsolidateOAuthTokens(dir, true)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Found != 1 || res.Moved != 1 || len(res.Errors) != 0 {
		t.Errorf("counters wrong: %+v", res)
	}
	// dryRun must NOT touch the filesystem: legacy still present,
	// canonical not yet created.
	if _, err := os.Stat(filepath.Join(dir, "youtube", "Token")); err != nil {
		t.Errorf("dryRun removed the legacy: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, CanonicalOAuthTokenSubPath)); err == nil {
		t.Errorf("dryRun created the canonical dir unexpectedly")
	}
}

func TestConsolidateOAuthTokens_EmptyLegacyDirPruned(t *testing.T) {
	dir := t.TempDir()
	body := `{"token":"a","refresh_token":"r","channel_id":"UC_prune"}`
	writeLegacyFile(t, dir, "youtube/Token", "UC_prune", body)

	res, err := ConsolidateOAuthTokens(dir, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Found != 1 || res.Moved != 1 {
		t.Errorf("counters wrong: %+v", res)
	}
	// The empty <dir>/youtube/ directory should now have been pruned.
	if _, err := os.Stat(filepath.Join(dir, "youtube")); !os.IsNotExist(err) {
		t.Errorf("empty legacy youtube/ should have been removed; stat err=%v", err)
	}
	if res.RemovedEmptyDirs == 0 {
		t.Errorf("RemovedEmptyDirs=%d, want >= 1", res.RemovedEmptyDirs)
	}
}

func TestConsolidateOAuthTokens_PerGroupToken(t *testing.T) {
	dir := t.TempDir()
	body := `{"token":"a","refresh_token":"r"}` // no channel_id in JSON
	// Per-group `Token_<channel>` filename gives us the channel_id.
	writeLegacyFile(t, dir, "youtube/group/wnba/Token_UC_pergroup", "UC_pergroup", body)

	res, err := ConsolidateOAuthTokens(dir, false)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if res.Found != 1 || res.Moved != 1 || len(res.Errors) != 0 {
		t.Errorf("counters wrong: %+v", res)
	}
	dest := filepath.Join(dir, CanonicalOAuthTokenSubPath, "account_UC_pergroup.json")
	if _, err := os.Stat(dest); err != nil {
		t.Errorf("canonical file not created from per-group Token: %v", err)
	}
}
