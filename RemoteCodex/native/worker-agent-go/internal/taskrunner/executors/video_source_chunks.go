package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"velox-shared/contract"
)

func (e *videoPrepareWindowExecutor) certifySourceChunks(ctx context.Context, path, sourceSHA string, size int64, probe windowSourceProbe, downloaded *contract.VideoSourceChunksV1) (*contract.VideoSourceChunksV1, error) {
	if size <= 0 || size > contract.VideoSourceChunkSize*contract.VideoSourceMaxChunks {
		return nil, nil
	}
	unlock := e.lockWindow("source-chunks:" + sourceSHA)
	defer unlock()
	if downloaded != nil && downloaded.Validate(size) == nil {
		probe.Chunks = downloaded
		e.sourceProbeMu.Lock()
		if e.sourceProbes == nil {
			e.sourceProbes = make(map[string]windowSourceProbe)
		}
		e.sourceProbes[sourceSHA] = probe
		e.sourceProbeMu.Unlock()
		e.writePersistedSourceProbe(sourceSHA, probe)
		return downloaded, nil
	}
	e.sourceProbeMu.Lock()
	cached, ok := e.sourceProbes[sourceSHA]
	e.sourceProbeMu.Unlock()
	if ok && cached.Chunks != nil && cached.Chunks.Validate(size) == nil {
		return cached.Chunks, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	chunks := &contract.VideoSourceChunksV1{ChunkSize: contract.VideoSourceChunkSize}
	complete := sha256.New()
	buffer := make([]byte, contract.VideoSourceChunkSize)
	var read int64
	for read < size {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		want := size - read
		if want > int64(len(buffer)) {
			want = int64(len(buffer))
		}
		n, err := io.ReadFull(file, buffer[:want])
		if err != nil {
			return nil, err
		}
		complete.Write(buffer[:n])
		digest := sha256.Sum256(buffer[:n])
		chunks.SHA256 = append(chunks.SHA256, hex.EncodeToString(digest[:]))
		read += int64(n)
	}
	var extra [1]byte
	if n, err := file.Read(extra[:]); n != 0 || err != io.EOF {
		return nil, fmt.Errorf("source changed size during block certification")
	}
	if hex.EncodeToString(complete.Sum(nil)) != sourceSHA {
		return nil, fmt.Errorf("source changed SHA-256 during block certification")
	}
	probe.Chunks = chunks
	e.sourceProbeMu.Lock()
	if e.sourceProbes == nil {
		e.sourceProbes = make(map[string]windowSourceProbe)
	}
	e.sourceProbes[sourceSHA] = probe
	e.sourceProbeMu.Unlock()
	e.writePersistedSourceProbe(sourceSHA, probe)
	return chunks, nil
}
