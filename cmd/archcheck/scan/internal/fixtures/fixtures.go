// Package fixtures contains canned Go identifiers used as the input dataset
// for the percheck_voiceover_alias_ban scan.
//
// The mix of ALLOWED and BANNED identifiers below is intentionally crafted
// to exercise both positive and negative cases of the regex
//
//	/[Vv]oice ?[Oo]ver[Aa]lias|Asset[Aa]lias\.Voiceover/
//
// ALLOWED identifiers (must NOT trigger a violation):
//   - Voiceover                       base voiceover word
//   - voiceoverProvider               snake_case provider var
//   - Asset.voiceover                 field of a struct (not the alias)
//   - voiceoverAssets                 snake_case plural
//
// BANNED identifiers (MUST trigger a violation):
//   - VoiceoverAlias                  exact alias-of-voiceover
//   - voiceOverAlias                  camelCase alias
//   - VoiceOveralias                  mixed-case alias
//   - VoiceoverAliasAlias             double alias marker
//   - voiceover_ALIAS                 upper-case marker
//   - AssetAlias                      struct name that combines an alias
package fixtures

// ----------------------------------------------------------------------------
// ALLOWED identifiers
// ----------------------------------------------------------------------------

// Voiceover is the base, allowed voiceover identifier.
const Voiceover = "default-voiceover-kind"

// voiceoverProvider is a snake_case voiceover variable name. ALLOWED.
var voiceoverProvider = "stub-provider"

// voiceoverAssets is a snake_case plural form. ALLOWED.
var voiceoverAssets = 7

// Asset is a struct with an allowed voiceover field.
type Asset struct {
	ID        string
	Kind      string
	voiceover string // field-level voiceover, ALLOWED (it is per-asset, not alias).
}

// AssetVoiceover is a struct that exposes a voiceover-style method. ALLOWED.
// Method names do not match the alias-ban regex because they describe a
// behaviour (Get voiceover) rather than an alias relationship.
type AssetVoiceover struct{}

func (AssetVoiceover) Get() string { return Voiceover }

// ----------------------------------------------------------------------------
// BANNED identifiers — these MUST be reported by the scan.
// ----------------------------------------------------------------------------

// VoiceoverAlias is the canonical banned name.
type VoiceoverAlias struct{}

// voiceOverAlias is the camelCase variant.
type voiceOverAlias struct{}

// VoiceOveralias is a mixed-case variant.
type VoiceOveralias struct{}

// VoiceoverAliasAlias is a double alias marker.
type VoiceoverAliasAlias struct{}

// voiceover_ALIAS is an upper-case-snake variant.
var voiceover_ALIAS bool = true

// AssetAlias is the banned AssetAlias struct.
type AssetAlias struct {
	Voiceover string // field is allowed (name does not match AssetAlias.Voiceover path).
}
