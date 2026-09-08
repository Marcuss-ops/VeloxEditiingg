package assetref

import (
	"bytes"
	"encoding/json"
	"strings"
)

// Package assetref — extraction from job payloads.
//
// ExtractAssetKeys walks a Velox render-job payload and returns
// the canonical keys used by the worker cache. Canonical
// `velox-asset://<id>` references yield `<id>`; legacy Drive URLs
// yield their Drive file ID for backwards compatibility.
//
// Schema coverage (single, canonical walker — no separate schema
// decode target):
//
//	{
//	  "scenes": [
//	    {
//	      "clip_link":  "...single URL...",
//	      "clip_links": ["...URL...", "...URL..."],
//	      "video_url":  "...URL...",
//	      "source_url": "...URL...",
//	      "clip":       { "url": "velox-asset://..." },
//	      "stock":      [...],
//	      "voiceover":  { "url": "velox-asset://..." }
//	    },
//	    ...
//	  ]
//	}
//
// Designed to be PASSIVE on parse failures:
//   - empty input → empty set (no panic).
//   - non-JSON input → empty set (no panic).
//   - non-Drive URL in any field → silently skipped.
//   - URL whose Drive ID cannot be extracted → silently skipped.
//
// The set is returned as map[AssetKey]struct{} for cheap O(1) dedup
// and zero-payload iteration. Caller does NOT need to free or
// iterate in any particular order. Keys are typed AssetKey (the
// canonical cache identity) rather than bare strings so a Drive file
// ID, a velox-asset ID, and a content hash cannot be confused at the
// call site.
func ExtractAssetKeys(payload json.RawMessage) map[AssetKey]struct{} {
	out := make(map[AssetKey]struct{})
	if len(payload) == 0 || bytes.Equal(payload, []byte("null")) {
		return out
	}
	var value interface{}
	if err := json.Unmarshal(payload, &value); err != nil {
		return out
	}
	return ExtractAssetKeysValueInto(value, out)
}

// ExtractAssetKeysValue walks an already-decoded payload. It is the same
// canonical walker as ExtractAssetKeys, without forcing callers that already
// have a map-shaped task payload through another JSON marshal/unmarshal.
func ExtractAssetKeysValue(value interface{}) map[AssetKey]struct{} {
	return ExtractAssetKeysValueInto(value, make(map[AssetKey]struct{}))
}

// ExtractAssetKeysValueInto adds keys from an already-decoded payload to out.
// The destination map is useful when a transport envelope contains an
// embedded canonical document whose identities must be unioned with the
// outer payload.
func ExtractAssetKeysValueInto(value interface{}, out map[AssetKey]struct{}) map[AssetKey]struct{} {
	if out == nil {
		out = make(map[AssetKey]struct{})
	}

	// clipStringKeys are the scene-level slots that carry a Drive URL
	// (or velox-asset:// reference) directly as a string value.
	clipStringKeys := map[string]struct{}{
		"clip_link":  {},
		"video_url":  {},
		"source_url": {},
		"clip_links": {},
	}

	var walk func(interface{})
	walk = func(v interface{}) {
		switch x := v.(type) {
		case map[string]interface{}:
			for key, child := range x {
				switch s := child.(type) {
				case string:
					trimmed := strings.TrimSpace(s)
					// Self-sufficient wire schemes (local velox-asset:// and
					// deferred velox-drive://) both yield their asset ID for the
					// worker cache key set.
					if id, ok := WireAssetID(trimmed); ok {
						out[AssetKey(id)] = struct{}{}
					} else if _, isClip := clipStringKeys[key]; isClip {
						if id, err := ParseDriveFileID(trimmed); err == nil && !id.Empty() {
							out[AssetKey(id.String())] = struct{}{}
						}
					}
				case []interface{}:
					// Legacy array-shaped clip_links: the recursive
					// walker normally does not attach parent field
					// names to arrays, so clip-bearing string arrays
					// are unwrapped here.
					if _, isClip := clipStringKeys[key]; isClip {
						for _, item := range s {
							if link, ok := item.(string); ok {
								if id, err := ParseDriveFileID(strings.TrimSpace(link)); err == nil && !id.Empty() {
									out[AssetKey(id.String())] = struct{}{}
								}
							}
						}
					}
					walk(child)
				case []string:
					if _, isClip := clipStringKeys[key]; isClip {
						for _, link := range s {
							if id, err := ParseDriveFileID(strings.TrimSpace(link)); err == nil && !id.Empty() {
								out[AssetKey(id.String())] = struct{}{}
							}
						}
					}
					walk(child)
				default:
					walk(child)
				}
			}
		case []interface{}:
			for _, child := range x {
				walk(child)
			}
		case []string:
			for _, child := range x {
				walk(child)
			}
		case []map[string]interface{}:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(value)
	return out
}
