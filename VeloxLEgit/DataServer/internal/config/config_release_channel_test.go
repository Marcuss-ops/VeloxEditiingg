package config

import (
	"os"
	"testing"
)

// TestReleaseChannel_DefaultsToDev verifies backward compatibility: when
// VELOX_RELEASE_CHANNEL is unset, loadRuntimeConfig defaults to "dev"
// so legacy installs that pre-date PR-5 keep behaving the same way.
// Bootstrap.go's PR-5 fail-fast is keyed off "!= dev", so defaulting to
// "dev" is the safe-for-existing-clients choice.
func TestReleaseChannel_DefaultsToDev(t *testing.T) {
	os.Unsetenv("VELOX_RELEASE_CHANNEL")
	c := loadRuntimeConfig("")
	if c.ReleaseChannel != "dev" {
		t.Fatalf("expected ReleaseChannel=dev fallback, got %q", c.ReleaseChannel)
	}
}

// TestReleaseChannel_ExplicitValues verifies that operators can set the
// channel to one of the three accepted values via the env var.
// IMPORTANT: loadRuntimeConfig applies TrimSpace only (no case normalisation
// or value validation). The CaseSensitive map below pins the canonical
// lower-case strings a production deploy SHOULD set; the deploy validator
// at deploy/validate-master-env.sh is responsible for rejecting non-canonical
// values upstream, so any operator-populated VELOX_RELEASE_CHANNEL that
// reaches this Go code has already been validated.
func TestReleaseChannel_ExplicitValues(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"production", "production"},
		{"staging", "staging"},
		{"dev", "dev"},
		{"PRODUCTION", "PRODUCTION"}, // case NOT normalised; treated as-is
		{"Staging", "Staging"},       // case NOT normalised; treated as-is
	}
	for _, tc := range cases {
		os.Setenv("VELOX_RELEASE_CHANNEL", tc.in)
		got := loadRuntimeConfig("").ReleaseChannel
		if got != tc.want {
			t.Errorf("VELOX_RELEASE_CHANNEL=%q → ReleaseChannel=%q (expected %q)", tc.in, got, tc.want)
		}
	}
	os.Unsetenv("VELOX_RELEASE_CHANNEL")
}

// TestReleaseChannel_TrimsWhitespace ensures that padded values
// (e.g. from a misconfigured .env file with trailing spaces) are
// normalised. The validator at deploy/validate-master-env.sh also
// trims before comparing, so this test pins the behaviour on both
// sides of the deploy boundary.
func TestReleaseChannel_TrimsWhitespace(t *testing.T) {
	os.Setenv("VELOX_RELEASE_CHANNEL", "   production   ")
	got := loadRuntimeConfig("").ReleaseChannel
	if got != "production" {
		t.Fatalf("expected trimmed 'production', got %q", got)
	}
	os.Unsetenv("VELOX_RELEASE_CHANNEL")
}

// TestReleaseChannel_SurvivesOtherVarsUnset ensures the new field's
// default does not interact with side effects of other env vars in
// loadRuntimeConfig (this is a smoke-test against future regressions
// where adding new fields might accidentally couple initialisation
// order to each other).
func TestReleaseChannel_SurvivesOtherVarsUnset(t *testing.T) {
	os.Unsetenv("VELOX_RELEASE_CHANNEL")
	os.Unsetenv("VELOX_GRPC_ALLOW_INSECURE_DEV")
	os.Unsetenv("VELOX_ALLOW_NOP_BLOBSTORE_DEV")
	c := loadRuntimeConfig("")
	if c.ReleaseChannel != "dev" || c.GRPCAllowInsecureDev || c.AllowNopBlobStoreDev {
		t.Fatalf("expected defaults preserved: ReleaseChannel=dev, GRPCAllowInsecureDev=false, AllowNopBlobStoreDev=false; got %+v", c)
	}
}
