package publisher

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type progressiveJournal struct {
	UploadID    string                   `json:"upload_id"`
	ChunkSize   int64                    `json:"chunk_size"`
	Finalized   bool                     `json:"finalized"`
	FinalSize   int64                    `json:"final_size"`
	FinalSHA256 string                   `json:"final_sha256"`
	Parts       []progressiveJournalPart `json:"parts"`
}

type progressiveJournalPart struct {
	Number int   `json:"number"`
	Size   int64 `json:"size"`
}

type ProgressiveResume struct {
	UploadID       string
	ChunkSize      int64
	CompletedParts []int
}

func LoadProgressiveResume(path, uploadID string, chunkSize, fileSize int64) (ProgressiveResume, error) {
	j, err := loadProgressiveJournal(path)
	if err != nil {
		return ProgressiveResume{}, err
	}
	if j.UploadID != "" && uploadID != "" && j.UploadID != uploadID {
		return ProgressiveResume{}, fmt.Errorf("progressive journal: upload_id mismatch")
	}
	if j.ChunkSize != 0 && chunkSize != 0 && j.ChunkSize != chunkSize {
		return ProgressiveResume{}, fmt.Errorf("progressive journal: chunk size changed")
	}
	if j.FinalSize > 0 && fileSize > 0 && j.FinalSize > fileSize {
		return ProgressiveResume{}, fmt.Errorf("progressive journal: final size exceeds local file")
	}
	parts := make([]int, 0, len(j.Parts))
	for _, p := range j.Parts {
		if p.Number <= 0 || p.Size <= 0 {
			return ProgressiveResume{}, fmt.Errorf("progressive journal: invalid part")
		}
		parts = append(parts, p.Number)
	}
	sort.Ints(parts)
	return ProgressiveResume{UploadID: j.UploadID, ChunkSize: j.ChunkSize, CompletedParts: parts}, nil
}

func loadProgressiveJournal(path string) (progressiveJournal, error) {
	if path == "" {
		return progressiveJournal{}, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return progressiveJournal{}, nil
	}
	if err != nil {
		return progressiveJournal{}, err
	}
	var j progressiveJournal
	if err := json.Unmarshal(data, &j); err != nil {
		return progressiveJournal{}, fmt.Errorf("progressive journal: decode: %w", err)
	}
	return j, nil
}

func saveProgressiveJournal(path string, j progressiveJournal) error {
	if path == "" {
		return nil
	}
	sort.Slice(j.Parts, func(i, k int) bool { return j.Parts[i].Number < j.Parts[k].Number })
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
func (j *progressiveJournal) hasPart(number int, size int64) bool {
	for _, p := range j.Parts {
		if p.Number == number {
			return p.Size == size
		}
	}
	return false
}
func (j *progressiveJournal) addPart(number int, size int64) {
	for _, p := range j.Parts {
		if p.Number == number {
			return
		}
	}
	j.Parts = append(j.Parts, progressiveJournalPart{Number: number, Size: size})
}
