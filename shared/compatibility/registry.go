// Package compatibility owns temporary wire-format aliases shared by all
// Velox payload readers. Producers must emit the canonical key; readers may
// use ReadStringList while the migration window is open.
package compatibility

import (
	"strings"
	"sync/atomic"
)

// VoiceoverPathsKey is the canonical top-level audio reference field.
const VoiceoverPathsKey = "voiceover_paths"

// CompatibilityAlias describes one canonical field and the legacy names that
// may still be encountered by readers. Sunset is an operator-facing ISO date.
type CompatibilityAlias struct {
	CanonicalKey string
	Aliases      []string
	Sunset       string
}

// AliasReadObserver receives one event for every legacy alias actually read.
// Observers must be non-blocking; readers invoke the callback synchronously.
type AliasReadObserver func(alias, canonical string)

var aliasReadObserver atomic.Value

func init() {
	aliasReadObserver.Store(AliasReadObserver(func(string, string) {}))
}

// SetAliasReadObserver installs the process-wide compatibility telemetry hook.
// Passing nil restores the no-op observer. The shared package deliberately
// knows nothing about Prometheus or DataServer metrics.
func SetAliasReadObserver(observer AliasReadObserver) {
	if observer == nil {
		observer = func(string, string) {}
	}
	aliasReadObserver.Store(observer)
}

func recordAliasRead(alias, canonical string) {
	aliasReadObserver.Load().(AliasReadObserver)(alias, canonical)
}

var voiceoverAliasRegistry = CompatibilityAlias{
	CanonicalKey: VoiceoverPathsKey,
	Aliases: []string{
		"voiceover_path",
		"voiceover",
		"unified_voiceover_link",
		"voiceovers",
		"voiceovers_urls",
		"audio_url",
		"audio_path",
		"source_url",
		"source",
		"url",
		"source_media",
		"source_media_url",
		"audio_source",
	},
	Sunset: "2026-12-31",
}

// Registry returns a defensive copy of every registered compatibility group.
func Registry() []CompatibilityAlias {
	return []CompatibilityAlias{
		{
			CanonicalKey: voiceoverAliasRegistry.CanonicalKey,
			Aliases:      append([]string(nil), voiceoverAliasRegistry.Aliases...),
			Sunset:       voiceoverAliasRegistry.Sunset,
		},
	}
}

// Lookup returns the compatibility group for a canonical key.
func Lookup(canonical string) (CompatibilityAlias, bool) {
	if canonical != voiceoverAliasRegistry.CanonicalKey {
		return CompatibilityAlias{}, false
	}
	entry := voiceoverAliasRegistry
	entry.Aliases = append([]string(nil), entry.Aliases...)
	return entry, true
}

// ReadStringList reads the canonical key followed by its registered aliases,
// accepting JSON-shaped strings, string arrays, and newline-delimited strings.
// Canonical values are authoritative and are returned first. Each present
// legacy alias emits exactly one telemetry event, even if its value is empty.
func ReadStringList(source map[string]interface{}, canonical string) []string {
	entry, ok := Lookup(canonical)
	if !ok || source == nil {
		return nil
	}

	keys := make([]string, 0, 1+len(entry.Aliases))
	keys = append(keys, entry.CanonicalKey)
	keys = append(keys, entry.Aliases...)

	var values []string
	for _, key := range keys {
		value, present := source[key]
		if !present {
			continue
		}
		if key != entry.CanonicalKey {
			recordAliasRead(key, entry.CanonicalKey)
		}
		values = append(values, stringsFromValue(value)...)
	}
	return dedupeStrings(values)
}

func stringsFromValue(value interface{}) []string {
	switch typed := value.(type) {
	case string:
		var out []string
		for _, line := range strings.Split(typed, "\n") {
			if value := strings.TrimSpace(line); value != "" {
				out = append(out, value)
			}
		}
		return out
	case []string:
		out := make([]string, 0, len(typed))
		for _, value := range typed {
			if value = strings.TrimSpace(value); value != "" {
				out = append(out, value)
			}
		}
		return out
	case []interface{}:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if value, ok := item.(string); ok {
				if value = strings.TrimSpace(value); value != "" {
					out = append(out, value)
				}
			}
		}
		return out
	default:
		return nil
	}
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
