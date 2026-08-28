//go:build !linux

package prefetch

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// WarmPreparedJob is the portable fallback for systems without
// posix_fadvise. It performs a bounded prefix read and closes every file
// immediately, preserving the same no-FD-leak contract as the Linux path.
func WarmPreparedJob(ctx context.Context, job PreparedJob, maxBytes int64) (int64, error) {
	if maxBytes <= 0 || len(job.Assets) == 0 {
		return 0, nil
	}
	assets := preparedAssetsForWarm(job)
	buf := make([]byte, 256<<10)
	var warmed int64
	for _, asset := range assets {
		if err := ctx.Err(); err != nil {
			return warmed, err
		}
		if warmed >= maxBytes {
			break
		}
		f, err := os.Open(asset.LocalPath)
		if err != nil {
			return warmed, fmt.Errorf("prefetch warm %s: %w", asset.AssetKey, err)
		}
		st, statErr := f.Stat()
		if statErr != nil {
			f.Close()
			return warmed, statErr
		}
		if asset.SizeBytes > 0 && st.Size() != asset.SizeBytes {
			f.Close()
			return warmed, fmt.Errorf("prefetch warm size mismatch %s: got=%d want=%d", asset.AssetKey, st.Size(), asset.SizeBytes)
		}
		remaining := maxBytes - warmed
		limit := int64(len(buf))
		if limit > remaining {
			limit = remaining
		}
		n, readErr := io.ReadFull(f, buf[:limit])
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			f.Close()
			return warmed, readErr
		}
		warmed += int64(n)
		_ = f.Close()
	}
	return warmed, nil
}

func preparedAssetsForWarm(job PreparedJob) []PreparedAssetMetadata {
	assets := make([]PreparedAssetMetadata, 0, len(job.Assets))
	for _, asset := range job.Assets {
		assets = append(assets, asset)
	}
	sort.SliceStable(assets, func(i, j int) bool {
		pi := preparedWarmPriority(assets[i])
		pj := preparedWarmPriority(assets[j])
		if pi != pj {
			return pi > pj
		}
		if assets[i].SizeBytes != assets[j].SizeBytes {
			return assets[i].SizeBytes > assets[j].SizeBytes
		}
		return assets[i].AssetKey < assets[j].AssetKey
	})
	return assets
}

func preparedWarmPriority(asset PreparedAssetMetadata) int {
	score := 0
	mime := strings.ToLower(asset.MIMEType)
	if asset.HasAudio || asset.AudioCodec != "" || strings.HasPrefix(mime, "audio/") {
		score += 200
	}
	if asset.HasVideo || asset.Codec != "" || strings.HasPrefix(mime, "video/") {
		score += 100
	}
	if asset.SizeBytes >= 128<<20 {
		score += 40
	} else if asset.SizeBytes >= 32<<20 {
		score += 20
	}
	return score
}
