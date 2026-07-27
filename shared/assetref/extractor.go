// Package assetref — extraction from job payloads.
//
// ExtractDriveFileIDs walks a Velox render-job payload and returns
// the set of Drive file IDs referenced as clip links. The extractor
// shares its single-source-of-truth principle with the parser:
// one canonical function reads clip URLs from any job payload
// shape the master schedules, and the same function is reachable
// from both DataServer (master snapshot) and the worker agent
// (resolveClip) without duplication.
//
// Schema (single, canonical):
//
//	{
//	  "scenes": [
//	    {
//	      "clip_link":  "...single URL...",
//	      "clip_links": ["...URL...", "...URL..."],
//	      "video_url":  "...URL...",
//	      "source_url": "...URL..."
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
// The set is returned as map[string]struct{} for cheap O(1) dedup
// and zero-payload iteration. Caller does NOT need to free or
// iterate in any particular order.
package assetref

import (
	"bytes"
	"encoding/json"
)

// extractSchema is the local-private decode target. The fields
// below match every clip-bearing slot we know about across the
// studio-creator and pipeline render paths. Anything outside this
// surface is decoded-then-discarded: a future schema addition that
// re-uses one of the existing keys (e.g. a new "stock_url" at the
// top level) is a deliberate future PR — adding it here bumps the
// snapshot coverage for the asset-cache feature.
type extractSchema struct {
	Scenes []struct {
		ClipLink  string   `json:"clip_link"`
		ClipLinks []string `json:"clip_links"`
		VideoURL  string   `json:"video_url"`
		SourceURL string   `json:"source_url"`
	} `json:"scenes"`
}

// ExtractDriveFileIDs returns the set of Drive file IDs referenced
// as clip links inside payload. The returned set is NEVER nil
// (an empty but non-nil map is the no-match signal) so callers can
// safely `for id := range ExtractDriveFileIDs(...) { ... }`.
//
// Behaviour:
//   - json.RawMessage of length 0 or `null` → empty set.
//   - JSON that decodes successfully but has no `scenes` key → empty.
//   - JSON that fails to decode at all → empty (best-effort; we
//     never want one malformed payload to break the snapshot loop).
//   - Per-URL errors from DriveFileID are swallowed: an invalid or
//     non-Drive URL in any slot is dropped silently. Loud failures
//     belong to the resolver layer, not to the lightweight
//     snapshot pass.
func ExtractDriveFileIDs(payload json.RawMessage) map[string]struct{} {
	out := make(map[string]struct{})

	if len(payload) == 0 || bytes.Equal(payload, []byte("null")) {
		return out
	}

	var schema extractSchema
	if err := json.Unmarshal(payload, &schema); err != nil {
		// Best-effort fallback: parse failure is logged by the caller
		// (snapshot service) via metric, never propagated.
		return out
	}

	add := func(rawURL string) {
		id, err := DriveFileID(rawURL)
		if err != nil || id == "" {
			return
		}
		out[id] = struct{}{}
	}

	for _, sc := range schema.Scenes {
		add(sc.ClipLink)
		for _, link := range sc.ClipLinks {
			add(link)
		}
		add(sc.VideoURL)
		add(sc.SourceURL)
	}
	return out
}
