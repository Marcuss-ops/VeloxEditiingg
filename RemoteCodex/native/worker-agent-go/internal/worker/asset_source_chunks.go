package worker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"velox-shared/contract"
)

type videoSourceChunksCacheV1 struct {
	Version   int                          `json:"version"`
	SHA256    string                       `json:"sha256"`
	SizeBytes int64                        `json:"size_bytes"`
	Chunks    contract.VideoSourceChunksV1 `json:"chunks"`
}

func videoSourceChunksPath(sourcePath string) string { return sourcePath + ".chunks.json" }

func writeVideoSourceChunks(sourcePath, sourceSHA string, size int64, chunks *contract.VideoSourceChunksV1) error {
	if chunks == nil || chunks.Validate(size) != nil || !isLowerHexSHA256(sourceSHA) {
		return fmt.Errorf("source chunk cache: invalid evidence")
	}
	entry := videoSourceChunksCacheV1{Version: 1, SHA256: sourceSHA, SizeBytes: size, Chunks: *chunks}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	path := videoSourceChunksPath(sourcePath)
	tmp, err := os.CreateTemp(filepath.Dir(path), ".source-chunks-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(encoded); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

func readVideoSourceChunks(sourcePath, sourceSHA string, size int64) (*contract.VideoSourceChunksV1, bool) {
	encoded, err := os.ReadFile(videoSourceChunksPath(sourcePath))
	if err != nil {
		return nil, false
	}
	var entry videoSourceChunksCacheV1
	if json.Unmarshal(encoded, &entry) != nil || entry.Version != 1 || entry.SHA256 != sourceSHA || entry.SizeBytes != size || entry.Chunks.Validate(size) != nil {
		return nil, false
	}
	return &entry.Chunks, true
}
