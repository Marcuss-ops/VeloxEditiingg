package contract

import "fmt"

const VideoSourceChunkSize int64 = 1 << 20
const VideoSourceMaxChunks = 1024

// VideoSourceChunksV1 binds fixed source blocks to the complete source
// certificate. It is emitted only while verifying the same full-file SHA.
type VideoSourceChunksV1 struct {
	ChunkSize int64    `json:"chunk_size"`
	SHA256    []string `json:"sha256"`
}

func (c VideoSourceChunksV1) Validate(size int64) error {
	if size <= 0 || c.ChunkSize != VideoSourceChunkSize || len(c.SHA256) > VideoSourceMaxChunks || int64(len(c.SHA256)) != (size-1)/VideoSourceChunkSize+1 {
		return fmt.Errorf("video source chunks: invalid block layout")
	}
	for _, sha := range c.SHA256 {
		if !isLowerSHA256(sha) {
			return fmt.Errorf("video source chunks: invalid block SHA-256")
		}
	}
	return nil
}
