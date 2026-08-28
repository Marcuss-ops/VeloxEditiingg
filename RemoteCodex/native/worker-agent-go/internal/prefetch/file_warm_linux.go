//go:build linux

package prefetch

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

// WarmPreparedJob asks the kernel to pull a bounded prefix of the most useful
// prepared assets into page cache. Files are opened only for the duration of
// the advisory call and closed immediately, so pre-job warming cannot grow the
// worker's steady-state FD footprint.
func WarmPreparedJob(ctx context.Context, job PreparedJob, maxBytes int64) (int64, error) {
	if maxBytes <= 0 || len(job.Assets) == 0 {
		return 0, nil
	}
	assets := preparedAssetsForWarm(job)
	var advised int64
	for _, asset := range assets {
		if err := ctx.Err(); err != nil {
			return advised, err
		}
		if advised >= maxBytes {
			break
		}
		path := strings.TrimSpace(asset.LocalPath)
		if path == "" {
			continue
		}
		f, err := os.Open(path)
		if err != nil {
			return advised, fmt.Errorf("prefetch warm %s: %w", asset.AssetKey, err)
		}
		st, statErr := f.Stat()
		if statErr != nil {
			f.Close()
			return advised, fmt.Errorf("prefetch warm stat %s: %w", asset.AssetKey, statErr)
		}
		if asset.SizeBytes > 0 && st.Size() != asset.SizeBytes {
			f.Close()
			return advised, fmt.Errorf("prefetch warm size mismatch %s: got=%d want=%d", asset.AssetKey, st.Size(), asset.SizeBytes)
		}
		remaining := maxBytes - advised
		length := st.Size()
		if length > remaining {
			length = remaining
		}
		if length > 0 {
			// FADV_WILLNEED is advisory. Some filesystems return EINVAL/ENOSYS;
			// those cases do not invalidate a verified asset, they merely mean
			// the kernel cannot honor this optional acceleration hint.
			advErr := unix.Fadvise(int(f.Fd()), 0, length, unix.FADV_WILLNEED)
			if advErr == nil {
				advised += length
			} else if advErr != unix.EINVAL && advErr != unix.ENOSYS && advErr != unix.EOPNOTSUPP {
				f.Close()
				return advised, fmt.Errorf("prefetch warm fadvise %s: %w", asset.AssetKey, advErr)
			}
		}
		_ = f.Close()
	}
	return advised, nil
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
