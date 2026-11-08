package assets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// RepairMissingPreparedWindow reconstructs a missing local stock-window blob
// only from its persisted immutable Drive source identity. Both the downloaded
// source and reconstructed output must match the hashes already certified in
// the registry; otherwise the repair fails closed.
func (s *AssetService) RepairMissingPreparedWindow(ctx context.Context, assetID string) (*Asset, error) {
	asset, err := s.Get(ctx, strings.TrimSpace(assetID))
	if err != nil || asset == nil {
		return nil, err
	}
	if asset.Status != AssetStatusReady || asset.Kind != "video_segment" || asset.SHA256 == "" || asset.StorageKey == "" {
		return nil, fmt.Errorf("asset is not a repairable READY video segment")
	}
	if reader, ok := s.blobStore.(interface {
		ReadFinal(string) (*os.File, error)
	}); ok {
		if f, openErr := reader.ReadFinal(asset.StorageKey); openErr == nil {
			_ = f.Close()
			return asset, nil
		}
	} else {
		return nil, fmt.Errorf("blob store cannot verify local asset presence")
	}
	sourceReader, ok := s.repo.(assetSourceReader)
	if !ok {
		return nil, fmt.Errorf("asset source registry unavailable")
	}
	sources, err := sourceReader.ListSources(ctx, asset.AssetID)
	if err != nil {
		return nil, err
	}
	for _, source := range sources {
		ref, err := parsePreparedWindowReference(source.SourceReference)
		if err != nil {
			continue
		}
		var evidence struct {
			SourceSHA256        string `json:"source_sha256"`
			FrameCount          int64  `json:"frame_count"`
			ProfileID           string `json:"canonical_profile_id"`
			PreparationRevision string `json:"preparation_revision"`
		}
		if json.Unmarshal([]byte(asset.MetadataJSON), &evidence) != nil ||
			!strings.EqualFold(evidence.SourceSHA256, ref.sourceSHA) ||
			evidence.ProfileID != ref.profile || evidence.FrameCount <= 0 ||
			(ref.revision != "" && ref.revision != s.videoTrimmer.PreparationRevision()) ||
			(evidence.PreparationRevision != "" && evidence.PreparationRevision != s.videoTrimmer.PreparationRevision()) {
			continue
		}
		driveURL, err := DeferredDriveReference(ref.sourceID)
		if err != nil {
			continue
		}
		materialized, cleanup, err := s.materializeVideoSource(ctx, driveURL)
		if err != nil {
			continue
		}
		func() {
			defer cleanup()
			in, openErr := os.Open(materialized)
			if openErr != nil {
				err = openErr
				return
			}
			h := sha256.New()
			_, err = io.Copy(h, in)
			_ = in.Close()
			if err != nil || !strings.EqualFold(hex.EncodeToString(h.Sum(nil)), ref.sourceSHA) {
				err = fmt.Errorf("source SHA-256 does not match prepared-window certificate")
				return
			}
			probe, probeErr := s.videoTrimmer.Probe(ctx, materialized)
			if probeErr != nil {
				err = probeErr
				return
			}
			out, createErr := os.CreateTemp(filepath.Dir(materialized), ".velox-repair-window-*.mp4")
			if createErr != nil {
				err = createErr
				return
			}
			output := out.Name()
			_ = out.Close()
			defer os.Remove(output)
			segment := VideoSegment{StartSeconds: float64(ref.startUS) / 1e6, EndSeconds: float64(ref.startUS+ref.durationUS) / 1e6}
			trim, trimErr := s.videoTrimmer.TrimFramesWithProbe(ctx, probe, materialized, output, segment, evidence.FrameCount)
			if trimErr != nil {
				err = trimErr
				return
			}
			prepared, hashErr := os.Open(output)
			if hashErr != nil {
				err = hashErr
				return
			}
			outputHash := sha256.New()
			_, err = io.Copy(outputHash, prepared)
			_ = prepared.Close()
			if err != nil {
				return
			}
			if !strings.EqualFold(hex.EncodeToString(outputHash.Sum(nil)), asset.SHA256) {
				err = fmt.Errorf("reconstructed window SHA-256 differs from registered asset")
				return
			}
			asset, err = s.registerPreparedVideoFile(ctx, output, source.SourceReference, trim.Plan)
		}()
		if err == nil && asset != nil {
			return asset, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("prepared-window repair failed: %w", err)
	}
	return nil, fmt.Errorf("no matching immutable prepared-window source is available")
}

type preparedWindowReference struct {
	sourceID, sourceSHA       string
	startUS, durationUS       int64
	profile, revision, scheme string
}

func parsePreparedWindowReference(raw string) (preparedWindowReference, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "velox-stock-window" && u.Scheme != "velox-video-window") || u.Host == "" {
		return preparedWindowReference{}, fmt.Errorf("not a prepared-window reference")
	}
	parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	if len(parts) < 4 {
		return preparedWindowReference{}, fmt.Errorf("invalid prepared-window reference")
	}
	decodedSHA, err := hex.DecodeString(parts[0])
	if err != nil || len(decodedSHA) != sha256.Size {
		return preparedWindowReference{}, fmt.Errorf("invalid prepared-window source SHA-256")
	}
	start, e1 := strconv.ParseInt(parts[1], 10, 64)
	duration, e2 := strconv.ParseInt(parts[2], 10, 64)
	if e1 != nil || e2 != nil || start < 0 || duration <= 0 || duration > int64(^uint64(0)>>1)-start {
		return preparedWindowReference{}, fmt.Errorf("invalid prepared-window interval")
	}
	profile, err := url.PathUnescape(parts[3])
	if err != nil || profile == "" {
		return preparedWindowReference{}, fmt.Errorf("invalid prepared-window profile")
	}
	revision := ""
	if len(parts) > 4 && u.Scheme == "velox-video-window" {
		decodedRevision, decodeErr := url.PathUnescape(parts[4])
		if decodeErr != nil || decodedRevision == "" {
			return preparedWindowReference{}, fmt.Errorf("invalid preparation revision")
		}
		revision = decodedRevision
	}
	return preparedWindowReference{sourceID: u.Host, sourceSHA: strings.ToLower(parts[0]), startUS: start, durationUS: duration, profile: profile, revision: revision, scheme: u.Scheme}, nil
}
