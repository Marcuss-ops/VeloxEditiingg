package artifacts

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChunkedUpload_BreakdownBytesEqualPersistedChunks(t *testing.T) {
	chunked, env, uploadID := setupChunkedEnv(t)
	chunks := [][]byte{bytes.Repeat([]byte("a"), 17), bytes.Repeat([]byte("b"), 29), bytes.Repeat([]byte("c"), 7)}
	var want int64
	for i, data := range chunks {
		want += int64(len(data))
		require.NoError(t, uploadChunk(t, chunked, uploadID, i, data))
	}

	rows, err := env.repo.ListChunks(context.Background(), uploadID)
	require.NoError(t, err)
	var got int64
	for _, row := range rows {
		got += row.SizeBytes
	}
	if got != want {
		t.Fatalf("persisted bytes = %d, want %d", got, want)
	}

	result, err := chunked.ReceiveChunked(context.Background(), uploadID)
	require.NoError(t, err)
	if result.ReceivedSizeBytes != want {
		t.Fatalf("received bytes = %d, want %d", result.ReceivedSizeBytes, want)
	}
}
