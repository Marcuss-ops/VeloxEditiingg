package main

import "encoding/json"

// jsonUnmarshal is a thin indirection so orchestrator_legacy_adapter.go
// can decode jobs.Payload (a TEXT column) into map[string]any without
// pulling encoding/json into the adapter file directly. Kept in a
// separate file so the adapter file remains a clean COMPATIBILITY-scoped
// surface that Fase 8 can delete in a single commit.
func jsonUnmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
