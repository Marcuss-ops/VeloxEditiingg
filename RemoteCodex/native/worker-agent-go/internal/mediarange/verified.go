package mediarange

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"velox-shared/contract"
)

// Every parsed metadata byte and every returned sample byte is read through
// this verifier. Disk caching bounds RAM and avoids re-fetching metadata blocks.
func (r *rangeReader) readAt(offset int64, buf []byte) (int, error) {
	if r.integrity == nil {
		return r.readRemoteAt(offset, buf)
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if offset < 0 || offset >= r.size {
		return 0, io.EOF
	}
	want := int64(len(buf))
	if want > r.size-offset {
		want = r.size - offset
	}
	first, last := offset/contract.VideoSourceChunkSize, (offset+want-1)/contract.VideoSourceChunkSize
	for chunk := first; chunk <= last; chunk++ {
		if r.verified[chunk] {
			continue
		}
		start := chunk * contract.VideoSourceChunkSize
		size := contract.VideoSourceChunkSize
		if size > r.size-start {
			size = r.size - start
		}
		data := make([]byte, size)
		n, err := r.readRemoteAt(start, data)
		if err != nil {
			return 0, err
		}
		if int64(n) != size {
			return 0, io.ErrUnexpectedEOF
		}
		digest := sha256.Sum256(data)
		if hex.EncodeToString(digest[:]) != r.integrity.SHA256[chunk] {
			return 0, fmt.Errorf("mediarange: block %d SHA-256 does not match source certificate", chunk)
		}
		if _, err := r.chunkFile.WriteAt(data, start); err != nil {
			return 0, err
		}
		r.verified[chunk] = true
	}
	n, err := r.chunkFile.ReadAt(buf[:want], offset)
	if int64(n) == want && err == io.EOF {
		err = nil
	}
	return n, err
}

func (r *rangeReader) enableIntegrity(proof *contract.VideoSourceChunksV1, directory string) (func(), error) {
	if proof == nil {
		return func() {}, nil
	}
	if err := proof.Validate(r.size); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(directory, 0750); err != nil {
		return nil, err
	}
	cache, err := os.CreateTemp(directory, ".verified-source-blocks-*")
	if err != nil {
		return nil, err
	}
	r.integrity = proof
	r.chunkFile = cache
	r.verified = make(map[int64]bool)
	return func() { cache.Close(); os.Remove(cache.Name()) }, nil
}
