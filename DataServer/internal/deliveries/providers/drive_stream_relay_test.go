package providers

import (
	"testing"

	"velox-server/internal/repository"
)

func TestRelayChunkAtOffsetSupportsDrivePartialAcknowledgement(t *testing.T) {
	chunks := []repository.ChunkRecord{
		{ChunkIndex: 0, SizeBytes: 8},
		{ChunkIndex: 1, SizeBytes: 4},
	}

	chunk, next, start, ok := relayChunkAtOffset(chunks, 5)
	if !ok || chunk.ChunkIndex != 0 || next == nil || next.ChunkIndex != 1 || start != 0 {
		t.Fatalf("relayChunkAtOffset(partial first chunk) = (%+v,%+v,%d,%v)", chunk, next, start, ok)
	}
	chunk, next, start, ok = relayChunkAtOffset(chunks, 10)
	if !ok || chunk.ChunkIndex != 1 || next != nil || start != 8 {
		t.Fatalf("relayChunkAtOffset(partial final chunk) = (%+v,%+v,%d,%v)", chunk, next, start, ok)
	}
	if _, _, _, ok := relayChunkAtOffset(chunks, 12); ok {
		t.Fatal("relayChunkAtOffset accepted the end-of-file offset")
	}
}
