package assetref

import (
	"errors"
	"testing"
)

func TestParseContentHashCanonicalizesAndValidates(t *testing.T) {
	t.Parallel()
	valid := " ABCDEFabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123 "
	hash, err := ParseContentHash(valid)
	if err != nil {
		t.Fatalf("ParseContentHash(valid): %v", err)
	}
	if got, want := hash.String(), "abcdefabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123"; got != want {
		t.Fatalf("hash=%q, want %q", got, want)
	}
	for _, raw := range []string{"", "short", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde"} {
		if _, err := ParseContentHash(raw); err == nil {
			t.Errorf("ParseContentHash(%q) accepted invalid digest", raw)
		}
	}
}

func TestAssetKeyStringAndEmpty(t *testing.T) {
	t.Parallel()
	if got := (AssetKey("clip-1")).String(); got != "clip-1" {
		t.Fatalf("AssetKey.String()=%q, want clip-1", got)
	}
	if !(AssetKey("  ")).Empty() || (AssetKey("clip-1")).Empty() {
		t.Fatal("AssetKey.Empty() did not distinguish blank and populated keys")
	}
}

func TestDriveFileID_FileLinkForm(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"file link with /view", "https://drive.google.com/file/d/ABC123/view", "ABC123"},
		{"file link with usp sharing", "https://drive.google.com/file/d/ABC123/view?usp=sharing", "ABC123"},
		{"file link with usp drive_link", "https://drive.google.com/file/d/ABC123/view?usp=drive_link", "ABC123"},
		{"file link without suffix", "https://drive.google.com/file/d/ABC123", "ABC123"},
		{"file link with /preview", "https://drive.google.com/file/d/ABC123/preview", "ABC123"},
		{"file link with /edit", "https://drive.google.com/file/d/ABC123/edit", "ABC123"},
		{"file link with trailing slash", "https://drive.google.com/file/d/ABC123/", "ABC123"},
		{
			"file link preferred over query id",
			"https://drive.google.com/file/d/ABC123/view?usp=sharing&id=ZZZ",
			"ABC123",
		},
		{
			"realistic long id with drive_link",
			"https://drive.google.com/file/d/19m3s1-_guIYqEZE2Ywy77s_mJZMR7686/view?usp=drive_link",
			"19m3s1-_guIYqEZE2Ywy77s_mJZMR7686",
		},
		{"uppercase host", "https://Drive.Google.COM/file/d/ABC123/view", "ABC123"},
		{"www prefix host", "https://www.drive.google.com/file/d/ABC123/view", "ABC123"},
		{"uppercase FILE in path", "https://drive.google.com/FILE/d/ABC123/view", "ABC123"},
		{"http scheme", "http://drive.google.com/file/d/ABC123/view", "ABC123"},
		{"leading whitespace", "  https://drive.google.com/file/d/ABC123/view", "ABC123"},
		{"leading tab and trailing newline", "	https://drive.google.com/file/d/ABC123/view\n", "ABC123"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseDriveFileID(tc.in)
			if err != nil {
				t.Fatalf("ParseDriveFileID(%q) unexpected error: %v", tc.in, err)
			}
			if got.String() != tc.want {
				t.Fatalf("ParseDriveFileID(%q) = %q, want %q", tc.in, got.String(), tc.want)
			}
		})
	}
}

func TestDriveFileID_QueryForm(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"uc bare id", "https://drive.google.com/uc?id=ABC123", "ABC123"},
		{
			"uc with export and confirm",
			"https://drive.google.com/uc?id=ABC123&export=download&confirm=t",
			"ABC123",
		},
		{"u prefix uc", "https://drive.google.com/u/0/uc?id=ABC123", "ABC123"},
		{
			"u prefix uc with export",
			"https://drive.google.com/u/2/uc?id=ABC123&export=download",
			"ABC123",
		},
		{"open link", "https://drive.google.com/open?id=ABC123", "ABC123"},
		{
			"open link with usp",
			"https://drive.google.com/open?id=ABC123&usp=sharing",
			"ABC123",
		},
		{
			"thumbnail link with size",
			"https://drive.google.com/thumbnail?id=ABC123&sz=w320",
			"ABC123",
		},
		{
			"realistic long uc id",
			"https://drive.google.com/uc?id=1b_bKMz0SCgIbOo_-Z5PN44DOBrFquPFM&export=download",
			"1b_bKMz0SCgIbOo_-Z5PN44DOBrFquPFM",
		},
		{
			"drive root with id query",
			"https://drive.google.com/?id=ABC123",
			"ABC123",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseDriveFileID(tc.in)
			if err != nil {
				t.Fatalf("ParseDriveFileID(%q) unexpected error: %v", tc.in, err)
			}
			if got.String() != tc.want {
				t.Fatalf("ParseDriveFileID(%q) = %q, want %q", tc.in, got.String(), tc.want)
			}
		})
	}
}

func TestDriveFileID_FoldersRejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
	}{
		{"folder root", "https://drive.google.com/drive/folders/abcd1234"},
		{"folder with usp", "https://drive.google.com/drive/folders/abcd1234?usp=sharing"},
		{"folder for user 0", "https://drive.google.com/drive/u/0/folders/abc"},
		{"folder for user 2", "https://drive.google.com/drive/u/2/folders/folder-123"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseDriveFileID(tc.in)
			if err == nil {
				t.Fatalf("ParseDriveFileID(%q) expected error, got nil", tc.in)
			}
			var fe *FolderError
			if !errors.As(err, &fe) {
				t.Fatalf("ParseDriveFileID(%q) expected *FolderError, got %T: %v", tc.in, err, err)
			}
		})
	}
}

func TestDriveFileID_Invalid(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
	}{
		{"empty string", ""},
		{"whitespace only", "   	  \n"},
		{"non-Drive host", "https://example.com/file/d/ABC/view"},
		{"accounts host", "https://accounts.google.com/file/d/ABC/view"},
		{"drive root", "https://drive.google.com/"},
		{"file path empty id", "https://drive.google.com/file/d/"},
		{"plain text", "not-a-url"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseDriveFileID(tc.in)
			if err == nil {
				t.Fatalf("ParseDriveFileID(%q) expected error, got nil", tc.in)
			}
		})
	}
}

func TestDriveFileID_PercentEncodedPins(t *testing.T) {
	t.Parallel()

	// Real Drive IDs use [a-zA-Z0-9_-] but URL-encoded forms surface from
	// third-party producers. We pin current behaviour (return url.Parse's
	// decoded path segment) so any future tightening of ID validation is
	// a deliberate change to a single test.
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			"encoded space in id",
			"https://drive.google.com/file/d/ABC%20123/view",
			"ABC 123",
		},
		{
			"hyphen and underscore kept",
			"https://drive.google.com/file/d/19m3s1-_guIYqEZE2Ywy77s_mJZMR7686/view",
			"19m3s1-_guIYqEZE2Ywy77s_mJZMR7686",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseDriveFileID(tc.in)
			if err != nil {
				t.Fatalf("ParseDriveFileID(%q) unexpected error: %v", tc.in, err)
			}
			if got.String() != tc.want {
				t.Fatalf("ParseDriveFileID(%q) = %q, want %q", tc.in, got.String(), tc.want)
			}
		})
	}
}

func TestDriveFileID_EmptyUsesSentinel(t *testing.T) {
	t.Parallel()
	_, err := ParseDriveFileID("   ")
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("expected ErrEmpty for whitespace input, got %v", err)
	}
}
