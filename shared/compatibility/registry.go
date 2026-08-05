// Package compatibility owns temporary wire-format aliases shared by all
// Velox payload readers. Producers must emit canonical keys; readers may use
// the registered aliases only while compatibility mode is enabled.
package compatibility

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// CompatibilityMode controls whether registered legacy aliases are accepted.
type CompatibilityMode string

const (
	// ModeCompat preserves the temporary migration behavior and is the default.
	ModeCompat CompatibilityMode = "compat"
	// ModeStrict rejects payloads containing registered legacy aliases.
	ModeStrict CompatibilityMode = "strict"
)

var compatibilityMode atomic.Value

// AliasReadObserver receives one event for every legacy alias actually read.
// Observers must be non-blocking; readers invoke the callback synchronously.
type AliasReadObserver func(alias, canonical string)

// AliasRejectedObserver receives one event for every legacy alias rejected by
// strict mode. Observers must be non-blocking.
type AliasRejectedObserver func(alias, canonical string)

var (
	aliasReadObserver     atomic.Value
	aliasRejectedObserver atomic.Value
)

func init() {
	compatibilityMode.Store(ModeCompat)
	aliasReadObserver.Store(AliasReadObserver(func(string, string) {}))
	aliasRejectedObserver.Store(AliasRejectedObserver(func(string, string) {}))
}

// SetMode changes the process-wide compatibility policy. Unknown values are
// intentionally fail-open to ModeCompat so a typo cannot unexpectedly reject
// existing producers; config validation should surface operator mistakes.
func SetMode(mode CompatibilityMode) {
	if mode != ModeStrict {
		mode = ModeCompat
	}
	compatibilityMode.Store(mode)
}

// Mode returns the current process-wide compatibility policy.
func Mode() CompatibilityMode { return compatibilityMode.Load().(CompatibilityMode) }

// SetAliasReadObserver installs the process-wide compatibility read telemetry
// hook. Passing nil restores the no-op observer.
func SetAliasReadObserver(observer AliasReadObserver) {
	if observer == nil {
		observer = func(string, string) {}
	}
	aliasReadObserver.Store(observer)
}

// SetAliasRejectedObserver installs the process-wide strict-mode rejection
// telemetry hook. Passing nil restores the no-op observer.
func SetAliasRejectedObserver(observer AliasRejectedObserver) {
	if observer == nil {
		observer = func(string, string) {}
	}
	aliasRejectedObserver.Store(observer)
}

func recordAliasRead(alias, canonical string) {
	aliasReadObserver.Load().(AliasReadObserver)(alias, canonical)
}

func recordAliasRejected(alias, canonical string) {
	aliasRejectedObserver.Load().(AliasRejectedObserver)(alias, canonical)
}

// VoiceoverPathsKey is the canonical top-level audio reference field.
const VoiceoverPathsKey = "voiceover_paths"

// CompatibilityAlias describes one canonical field and its temporary legacy
// aliases. Lifecycle metadata is intentionally kept in this one registry so
// owners do not create parallel compatibility lists in individual packages.
type CompatibilityAlias struct {
	CanonicalKey      string
	Aliases           []string
	Owner             string
	Consumers         []string
	ReadCounter       uint64
	RejectionCounter  uint64
	ReadCounters      map[string]uint64
	RejectionCounters map[string]uint64
	RemovalDate       string
	// RemovalDate is the canonical lifecycle field.
	MinimumVersion string
}

type aliasCounters struct {
	reads      atomic.Uint64
	rejections atomic.Uint64
}

var aliasCountersByName = map[string]*aliasCounters{
	"voiceover_path":         {},
	"voiceover":              {},
	"unified_voiceover_link": {},
	"voiceovers":             {},
	"voiceovers_urls":        {},
	"audio_url":              {},
	"audio_path":             {},
	"source_url":             {},
	"source_media":           {},
	"source_media_url":       {},
	"audio_source":           {},
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
		"source_media",
		"source_media_url",
		"audio_source",
	},
	Owner:          "platform-contracts",
	Consumers:      []string{"shared/contract/payload_v2.go", "DataServer/internal/jobs/enqueue/normalize_media.go", "DataServer/internal/remoteengine/dto_assets.go", "DataServer/internal/handlers/server/pipeline/worker_payload_projection.go", "DataServer/cmd/server/bootstrap_composition.go", "RemoteCodex/native/worker-agent-go/pkg/api/renderplan/validation.go"},
	RemovalDate:    "2026-12-31",
	MinimumVersion: "payload.v2",
}

func snapshotEntry(entry CompatibilityAlias) CompatibilityAlias {
	entry.Aliases = append([]string(nil), entry.Aliases...)
	entry.Consumers = append([]string(nil), entry.Consumers...)
	entry.ReadCounters = make(map[string]uint64, len(entry.Aliases))
	entry.RejectionCounters = make(map[string]uint64, len(entry.Aliases))
	for _, alias := range entry.Aliases {
		if counters, ok := aliasCountersByName[alias]; ok {
			reads := counters.reads.Load()
			rejections := counters.rejections.Load()
			entry.ReadCounters[alias] = reads
			entry.RejectionCounters[alias] = rejections
			entry.ReadCounter += reads
			entry.RejectionCounter += rejections
		}
	}
	return entry
}

// Registry returns a defensive snapshot of every registered compatibility
// group, including current read and rejection counters.
func Registry() []CompatibilityAlias {
	return []CompatibilityAlias{snapshotEntry(voiceoverAliasRegistry)}
}

// Lookup returns the compatibility group for a canonical key.
func Lookup(canonical string) (CompatibilityAlias, bool) {
	if canonical != voiceoverAliasRegistry.CanonicalKey {
		return CompatibilityAlias{}, false
	}
	return snapshotEntry(voiceoverAliasRegistry), true
}

// AliasRejectionError identifies a registered alias refused by strict mode.
type AliasRejectionError struct {
	Alias     string
	Canonical string
}

func (e *AliasRejectionError) Error() string {
	return fmt.Sprintf("legacy compatibility alias %q is rejected in strict mode; use %q", e.Alias, e.Canonical)
}

func rejectAlias(alias, canonical string) error {
	if counters, ok := aliasCountersByName[alias]; ok {
		counters.rejections.Add(1)
	}
	recordAliasRejected(alias, canonical)
	return &AliasRejectionError{Alias: alias, Canonical: canonical}
}

// ValidateNoLegacyAliases rejects the first registered legacy alias present in
// source. It is the strict boundary API for callers that can return errors.
func ValidateNoLegacyAliases(source map[string]interface{}) error {
	if Mode() != ModeStrict || source == nil {
		return nil
	}
	entry := voiceoverAliasRegistry
	for _, alias := range entry.Aliases {
		if _, present := source[alias]; present {
			return rejectAlias(alias, entry.CanonicalKey)
		}
	}
	return nil
}

// ReadStringList reads the canonical key followed by its registered aliases,
// accepting JSON-shaped strings, string arrays, and newline-delimited strings.
// Canonical values are authoritative. In strict mode an alias is rejected and
// no legacy value is returned; callers with an error surface should use
// ValidateNoLegacyAliases before this compatibility-preserving helper.
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
			if Mode() == ModeStrict {
				_ = rejectAlias(key, entry.CanonicalKey)
				return nil
			}
			if counters, exists := aliasCountersByName[key]; exists {
				counters.reads.Add(1)
			}
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
